(ns hooray.plan-test
  (:require
   [clojure.spec.alpha :as s]
   [clojure.test :refer [deftest is testing]]
   [hooray.core :as h]
   [hooray.fixtures :as fix]
   [hooray.plan :as plan]
   [hooray.query :as query])
  (:import
   (hooray.plan Stage)
   (org.hooray.engine BindingSet TriplePattern)))

(deftest clauses-compile-to-runtime-independent-descriptors-test
  (let [conformed-query (s/conform ::query/query
                                   '{:find [?e ?name ?label]
                                     :where [[?e :name ?name]
                                             [(string? ?name)]
                                             [(str ?name) ?label]]})
        clauses (:where conformed-query)
        [triple predicate function] (plan/clauses->descriptors clauses)]
    (is (= {:kind :triple
            :variables '[?e ?name]
            :clause (second (first clauses))}
           (select-keys triple [:kind :variables :clause])))
    (is (= {:kind :predicate
            :variables '[?name]
            :clause (second (second clauses))}
           (select-keys predicate [:kind :variables :clause])))
    (is (= {:kind :function
            :variables '[?name ?label]
            :clause (second (nth clauses 2))}
           (select-keys function [:kind :variables :clause])))
    (is (every? #(= #{:kind :idx :variables :groundable :clause}
                    (set (keys %)))
                [triple predicate function]))
    (is (= '[?e ?name] ((:groundable triple) #{})))
    (is (= [] ((:groundable predicate) #{'?name})))
    (is (= '[?label] ((:groundable function) #{'?name})))))

(deftest composite-descriptors-preserve-branch-and-child-structure-test
  (let [conformed-query (s/conform ::query/query
                                   '{:find [?e ?friend]
                                     :where [(not (or [?e :friend ?friend]
                                                      (and [?e :colleague ?friend]
                                                           [(= ?friend ?friend)])))]})
        clause (first (:where conformed-query))
        [{:keys [children] :as not-descriptor}]
        (plan/clauses->descriptors [clause])
        [{:keys [branches] :as or-descriptor}] children]
    (is (= :not (:kind not-descriptor)))
    (is (= (second clause) (:clause not-descriptor)))
    (is (= [:or] (mapv :kind children)))
    (is (= [[:triple] [:triple :predicate]]
           (mapv #(mapv :kind %) branches)))
    (is (= #{:kind :idx :variables :groundable :clause :branches}
           (set (keys or-descriptor))))))

(deftest or-descriptor-groundability-closes-each-branch-before-intersection-test
  (let [conformed-query (s/conform ::query/query
                                   '{:find [?e ?amount ?incremented]
                                     :where [(or (and [(inc ?amount) ?incremented]
                                                      [?e :age ?amount])
                                                 (and [(inc ?amount) ?incremented]
                                                      [?e :salary ?amount]))]})
        [descriptor] (plan/clauses->descriptors (:where conformed-query))]
    (is (= '[?amount ?incremented ?e]
           ((:groundable descriptor) #{})))))

(deftest or-descriptor-groundability-is-all-or-nothing-test
  (let [conformed-query (s/conform ::query/query
                                   '{:find [?v ?e]
                                     :where [(or [?v :next ?e]
                                                 (and [?e :age 35]
                                                      [(< ?v 3)]))]})
        [descriptor] (plan/clauses->descriptors (:where conformed-query))]
    (is (= [] ((:groundable descriptor) #{})))
    (is (= [] ((:groundable descriptor) #{'?e})))
    (is (= '[?e] ((:groundable descriptor) #{'?v})))))

(deftest inputs-compile-to-relation-descriptors-test
  (let [conformed-query (s/conform ::query/query
                                   '{:find [?x ?item ?left ?right ?e ?name]
                                     :in [?x [?item ...] [?left ?right] [[?e ?name]]]
                                     :where [[?e :name ?name]]})
        descriptors (plan/inputs->descriptors (:in conformed-query)
                                              [5 [3 1 2] [7 8] [[:alice "Alice"]]])
        binding-sets (mapv :binding-set descriptors)]
    (is (every? #(= #{:kind :idx :variables :groundable :binding-set}
                    (set (keys %)))
                descriptors))
    (is (= [:relation :relation :relation :relation]
           (mapv :kind descriptors)))
    (is (= ['[?x] '[?item] '[?left ?right] '[?e ?name]]
           (mapv #(.getVariables ^BindingSet %) binding-sets)))
    (is (= [[[5]] [[1] [2] [3]] [[7 8]] [[:alice "Alice"]]]
           (mapv #(mapv vec (.getRows ^BindingSet %)) binding-sets)))))

(deftest plan-scope-descriptor-groups
  (let [entity '?entity
        name '?name
        entity-descriptor {:kind :triple
                           :idx 1
                           :variables [entity name]
                           :groundable (fn [bound]
                                         (vec (remove bound [entity name])))}
        predicate-descriptor {:kind :predicate
                              :idx 2
                              :variables [name]
                              :groundable (constantly [])}]
    (is (= [{:added [entity]
             :proposers [1]
             :participants [1]
             :target-variables [entity]}
            {:added [name]
             :proposers [1]
             :participants [1 2]
             :target-variables [entity name]}]
           (plan/plan-scope [entity-descriptor predicate-descriptor]
                            [entity name]
                            [])))))

(deftest plan-scope-keeps-grouped-or-and-validation-logical-test
  (let [variables ['?entity '?name '?amount]
        or-descriptor {:kind :or
                       :idx 3
                       :variables variables
                       :groundable (fn [bound]
                                     (vec (remove bound variables)))}
        predicate-descriptor {:kind :predicate
                              :idx 4
                              :variables ['?amount]
                              :groundable (constantly [])}]
    (is (= [{:added variables
             :proposers [3]
             :participants [3]
             :target-variables variables}
            {:added []
             :proposers []
             :participants [4]
             :target-variables variables}]
           (plan/plan-scope [or-descriptor predicate-descriptor]
                            variables
                            [])))))

(deftest plan-scope-partially-validates-patterns-after-grouped-or-test
  (testing "grouped-or proposal + triple validation"
    (let [[x y z] ['?x '?y '?z]
          or-descriptor {:kind :or
                         :idx 1
                         :variables [x y]
                         :groundable (fn [bound]
                                       (vec (remove bound [x y])))}
          triple-descriptor {:kind :triple
                             :idx 2
                             :variables [y z]
                             :groundable (fn [bound]
                                           (vec (remove bound [y z])))}]
      (is (= [{:added [x y]
               :proposers [1]
               :participants [1 2]
               :target-variables [x y]}
              {:added [z]
               :proposers [2]
               :participants [2]
               :target-variables [x y z]}]
             (plan/plan-scope [or-descriptor triple-descriptor]
                              [x y z]
                              [])))))

  (testing "gruped-or proposal + triple completion"
    (let [[x y] ['?x '?y]
          or-descriptor {:kind :or
                         :idx 1
                         :variables [x y]
                         :groundable (fn [bound]
                                       (vec (remove bound [x y])))}
          triple-descriptor {:kind :triple
                             :idx 2
                             :variables [y]
                             :groundable (fn [bound]
                                           (vec (remove bound [y])))}]
      (is (= [{:added [x y]
               :proposers [1]
               :participants [1 2]
               :target-variables [x y]}]
             (plan/plan-scope [or-descriptor triple-descriptor]
                              [x y]
                              []))))))

(deftest plan-scope-keep-only-relevant-incoming-variables-test
  (let [incoming-a '?incoming-a
        incoming-b '?incoming-b
        irrelevant '?irrelevant
        new-variable '?new
        descriptor {:kind :triple
                    :idx 7
                    :variables [incoming-a new-variable]
                    :groundable (fn [bound]
                                  (vec (remove bound [incoming-a new-variable])))}]
    (is (= [{:added [incoming-b]
             :proposers [-1]
             :participants [-1]
             :target-variables [incoming-b]}
            {:added [incoming-a]
             :proposers [-1 7]
             :participants [-1 7]
             :target-variables [incoming-b incoming-a]}
            {:added [new-variable]
             :proposers [7]
             :participants [7]
             :target-variables [incoming-b incoming-a new-variable]}]
           (plan/plan-scope [descriptor]
                            [new-variable incoming-b incoming-a]
                            [irrelevant incoming-a incoming-b])))))

(deftest plan-scope-groups-or-as-the-only-proposer-test
  (let [variables ['?e '?name '?amount]
        descriptor {:kind :or
                    :idx 1
                    :variables variables
                    :groundable (fn [bound]
                                  (vec (remove bound variables)))}]
    (is (= [{:added variables
             :proposers [1]
             :participants [1]
             :target-variables variables}]
           (plan/plan-scope [descriptor] variables [])))))

(deftest plan-scope-plans-input-relations-as-proposers-test
  (let [variables ['?e '?name]
        relation-descriptor {:kind :relation
                             :idx 1
                             :variables variables
                             :groundable (fn [bound]
                                           (when-let [variable (first (remove bound variables))]
                                             [variable]))}
        triple-descriptor {:kind :triple
                           :idx 2
                           :variables variables
                           :groundable (fn [bound]
                                         (vec (remove bound variables)))}]
    (is (= [{:added ['?e]
             :proposers [1 2]
             :participants [1 2]
             :target-variables ['?e]}
            {:added ['?name]
             :proposers [1 2]
             :participants [1 2]
             :target-variables variables}]
           (plan/plan-scope [relation-descriptor triple-descriptor]
                            variables
                            [])))))

(deftest plan-scope-grounds-function-output-after-arguments-test
  (let [x '?x
        y '?y
        relation-descriptor {:kind :relation
                             :idx 1
                             :variables [x]
                             :groundable (fn [bound]
                                           (when-not (contains? bound x)
                                             [x]))}
        function-descriptor {:kind :function
                             :idx 2
                             :variables [x y]
                             :groundable (fn [bound]
                                           (when (and (contains? bound x)
                                                      (not (contains? bound y)))
                                             [y]))}]
    (is (= [{:added [x]
             :proposers [1]
             :participants [1]
             :target-variables [x]}
            {:added [y]
             :proposers [2]
             :participants [2]
             :target-variables [x y]}]
           (plan/plan-scope [relation-descriptor function-descriptor]
                            [x y]
                            [])))))

(deftest plan-scope-groups-remaining-or-variables-test
  (let [e '?e
        variables [e '?name '?amount]
        prefix-descriptor {:kind :triple
                           :idx 1
                           :variables [e]
                           :groundable (fn [bound]
                                         (vec (remove bound [e])))}
        or-descriptor {:kind :or
                       :idx 2
                       :variables variables
                       :groundable (fn [bound]
                                     (vec (remove bound variables)))}]
    (is (= [{:added [e]
             :proposers [1]
             :participants [1]
             :target-variables [e]}
            {:added ['?name '?amount]
             :proposers [2]
             :participants [2]
             :target-variables variables}]
           (plan/plan-scope [prefix-descriptor or-descriptor]
                            variables
                            [])))))

(deftest plan-scope-requires-every-or-branch-to-produce-a-variable-test
  (let [query '{:find [?e ?amount ?incremented]
                :where [(or (and [?e :age ?amount]
                                 [(inc ?amount) ?incremented])
                            (and [?e :salary ?amount]
                                 [(> ?incremented 0)]))]}
        conformed-query (s/conform ::query/query query)
        variable-order (vec (query/query->variable-order conformed-query))
        descriptors (plan/clauses->descriptors (:where conformed-query))]
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"\?incremented not bound"
         (plan/plan-scope descriptors variable-order ['?e '?amount])))))

(deftest plan-scope-validates-or-only-after-all-variables-are-grounded-test
  (let [e '?e
        name '?name
        amount '?amount
        variables [e name amount]
        name-descriptor {:kind :triple
                         :idx 1
                         :variables [e name]
                         :groundable (fn [bound]
                                       (vec (remove bound [e name])))}
        amount-descriptor {:kind :triple
                           :idx 2
                           :variables [e amount]
                           :groundable (fn [bound]
                                         (vec (remove bound [e amount])))}
        or-descriptor {:kind :or
                       :idx 3
                       :variables variables
                       :groundable (constantly [])}]
    (is (= [{:added [e]
             :proposers [1 2]
             :participants [1 2]
             :target-variables [e]}
            {:added [name]
             :proposers [1]
             :participants [1]
             :target-variables [e name]}
            {:added [amount]
             :proposers [2]
             :participants [2 3]
             :target-variables variables}]
           (plan/plan-scope [name-descriptor amount-descriptor or-descriptor]
                            variables
                            [])))))

(deftest plan-assembles-executable-stages-test
  (let [conformed-query (s/conform ::query/query
                                   '{:find [?e ?name]
                                     :where [[?e :name ?name]]})
        variable-order (vec (query/query->variable-order conformed-query))]
    (with-open [node (h/connect fix/*opts*)]
      (let [stages (plan/plan (h/db node) conformed-query [] variable-order)
            participant (-> stages first :participants first)]
        (is (every? #(instance? Stage %) stages))
        (is (instance? TriplePattern participant))
        (is (= #{'?e '?name} (.getVariables ^TriplePattern participant)))
        (is (= [(plan/->Stage ['?e] [participant] [0] ['?e])
                (plan/->Stage ['?name] [participant] [0] ['?e '?name])]
               stages))))))
