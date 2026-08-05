(ns hooray.plan2-test
  (:require
   [clojure.spec.alpha :as s]
   [clojure.test :refer [deftest is testing]]
   [hooray.plan :as plan]
   [hooray.plan2 :as plan2]
   [hooray.query :as query]))

(def empty-db
  {:eav {}
   :aev {}
   :ave {}
   :opts {:storage :hash}})

(defn- conform-query [query]
  (s/conform ::query/query query))

(deftest descriptor-and-logical-planning-match-plan-test
  (let [conformed-query (conform-query
                         '{:find [?e ?name ?label]
                           :where [[?e :name ?name]
                                   [(string? ?name)]
                                   [(str ?name) ?label]]})
        clauses (:where conformed-query)
        original (plan/clauses->descriptors clauses)
        copied (plan2/clauses->descriptors clauses)
        descriptor-shape (fn [descriptor]
                           (select-keys descriptor [:kind :variables :clause]))
        variable-order (vec (query/query->variable-order conformed-query))]
    (is (= (mapv descriptor-shape original)
           (mapv descriptor-shape copied)))
    (is (= (mapv #((:groundable %) #{'?e '?name}) original)
           (mapv #((:groundable %) #{'?e '?name}) copied)))
    (is (= (plan/plan-scope copied variable-order [])
           (plan2/plan-scope copied variable-order [])))))

(deftest incoming-variables-are-trimmed-and-moved-to-the-front-test
  (let [incoming-a '?incoming-a
        incoming-b '?incoming-b
        irrelevant '?irrelevant
        new-variable '?new
        descriptor {:kind :triple
                    :idx 7
                    :variables [incoming-a new-variable]
                    :groundable (fn [bound]
                                  (vec (remove bound [incoming-a new-variable])))}
        stages (plan2/plan-scope [descriptor]
                                 [new-variable incoming-b incoming-a]
                                 [irrelevant incoming-a incoming-b])]
    (is (= [[incoming-b]
            [incoming-b incoming-a]
            [incoming-b incoming-a new-variable]]
           (mapv :target-variables stages)))))

(deftest plan-emits-a-compiled-scope-with-coalesced-generic-stages-test
  (let [conformed-query (conform-query
                         '{:find [?e ?name]
                           :where [[?e :name ?name]]})
        variable-order (vec (query/query->variable-order conformed-query))
        compiled (plan2/plan empty-db conformed-query [] variable-order)]
    (is (= {:input-variables []
            :variable-order '[?e ?name]
            :extenders []
            :stages [{:kind :generic
                      :target-variables '[?e ?name]}]}
           (assoc compiled :extenders [])))
    (is (= 1 (count (:extenders compiled))))))

(deftest ordinary-proposal-and-composite-validation-become-separate-stages-test
  (let [conformed-query (conform-query
                         '{:find [?e ?name]
                           :where [[?e :name ?name]
                                   (or [(= ?name "A")]
                                       [(= ?name "B")])]})
        variable-order (vec (query/query->variable-order conformed-query))
        {:keys [stages]} (plan2/plan empty-db conformed-query [] variable-order)
        [generic-stage or-stage] stages]
    (is (= [:generic :or] (mapv :kind stages)))
    (is (= variable-order (:target-variables generic-stage)))
    (is (= [] (:added or-stage)))
    (is (= variable-order (:target-variables or-stage)))
    (is (= [['?name] ['?name]]
           (mapv :input-variables (:branches or-stage))))))

(deftest proposing-or-uses-the-pre-stage-layout-test
  (let [conformed-query (conform-query
                         '{:find [?e ?name]
                           :where [[?e :type :person]
                                   (or [?e :name ?name]
                                       [?e :alias ?name])]})
        variable-order (vec (query/query->variable-order conformed-query))
        {:keys [stages]} (plan2/plan empty-db conformed-query [] variable-order)
        or-stage (second stages)]
    (is (= [:generic :or] (mapv :kind stages)))
    (is (= '[?name] (:added or-stage)))
    (is (= [['?e] ['?e]]
           (mapv :input-variables (:branches or-stage))))))

(deftest leaf-validation-after-a-proposing-or-is-absorbed-test
  (let [conformed-query (conform-query
                         '{:find [?e ?name ?age]
                           :where [(or (and [?e :name ?name]
                                            [?e :age ?age])
                                       (and [?e :alias ?name]
                                            [?e :years ?age]))
                                   [(< ?age 100)]]})
        variable-order (vec (query/query->variable-order conformed-query))
        {:keys [stages]} (plan2/plan empty-db conformed-query [] variable-order)]
    (is (= [:or] (mapv :kind stages)))
    (is (= variable-order (:added (first stages))))
    (is (= variable-order (:target-variables (first stages))))))

(deftest not-is-a-validation-stage-with-post-generic-input-test
  (let [conformed-query (conform-query
                         '{:find [?e ?name]
                           :where [[?e :name ?name]
                                   (not [?e :blocked-name ?name])]})
        variable-order (vec (query/query->variable-order conformed-query))
        {:keys [stages]} (plan2/plan empty-db conformed-query [] variable-order)
        not-stage (second stages)]
    (is (= [:generic :not] (mapv :kind stages)))
    (is (= variable-order (:target-variables not-stage)))
    (is (= variable-order (get-in not-stage [:body :input-variables])))))

(deftest checked-stage-map-constructors-reject-invalid-layouts-test
  (testing "duplicate variables"
    (is (thrown? IllegalStateException
                 (#'plan2/generic-stage ['?e '?e]))))
  (testing "scope input must be a prefix of its variable order"
    (is (thrown? IllegalStateException
                 (#'plan2/compiled-scope ['?name]
                                         ['?e '?name]
                                         []
                                         []))))
  (testing "NOT must preserve the current layout"
    (is (thrown? IllegalStateException
                 (#'plan2/not-stage ['?e]
                                    ['?e '?name]
                                    ['?e]
                                    {:input-variables ['?e]
                                     :variable-order ['?e]
                                     :extenders []
                                     :stages []})))))

(deftest compiled-scope-uses-a-valid-topological-prefix-order-test
  (let [conformed-query (conform-query
                         '{:find [?result ?e]
                           :where [[(> ?result 0)]
                                   [?e :age ?age]
                                   [(inc ?age) ?result]]})
        requested-order (vec (query/query->variable-order conformed-query))
        compiled (plan2/plan empty-db conformed-query [] requested-order)]
    (is (= '[?result ?e ?age] requested-order))
    (is (= '[?e ?age ?result] (:variable-order compiled)))
    (is (= '[?e ?age ?result]
           (get-in compiled [:stages 0 :target-variables])))))
