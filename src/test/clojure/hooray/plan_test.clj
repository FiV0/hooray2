(ns hooray.plan-test
  (:require
   [clojure.spec.alpha :as s]
   [clojure.test :as t :refer [deftest is testing]]
   [hooray.core :as h]
   [hooray.fixtures :as fix]
   [hooray.plan :as plan]
   [hooray.query :as query])
  (:import
   (org.hooray.engine BindingSet NotPattern OrPattern RelationPattern Stage)))

(t/use-fixtures :each fix/with-node fix/with-people-schema)

(defn- plan-query [query & args]
  (let [conformed-query (s/conform ::query/query query)
        variable-order (vec (query/query->variable-order conformed-query))]
    (query/validate-query conformed-query)
    (plan/plan (h/db fix/*node*) conformed-query args variable-order)))

(deftest or-pattern-grounds-all-variables-as-the-only-proposer-test
  (h/transact fix/*node* [{:db/id :alice :name "Alice" :age 30}
                          {:db/id :bob :name "Bob" :salary 40}])

  (let [stages (plan-query '{:find [?e ?name ?amount]
                             :where [(or (and [?e :name ?name]
                                              [?e :age ?amount])
                                         (and [?e :salary ?amount]
                                              [?e :name ?name]))]})
        ^Stage stage (first stages)
        participants (.getParticipants stage)
        ^BindingSet result (query/execute stages)]
    (is (= 1 (count stages)))
    (is (= '[?e ?name ?amount] (.getAdded stage)))
    (is (= '[?e ?name ?amount] (.getTargetVariables stage)))
    (is (= 1 (count participants)))
    (is (instance? OrPattern (first participants)))
    (is (= #{[:alice "Alice" 30]
             [:bob "Bob" 40]}
           (set (map vec (.getRows result)))))))

(deftest input-bindings-are-planned-as-relations-test
  (h/transact fix/*node* [{:db/id :alice :name "Alice"}
                          {:db/id :bob :name "Bob"}])

  (let [stages (plan-query '{:find [?e ?name]
                             :in [[[?e ?name]]]
                             :where [[?e :name ?name]]}
                           [[:alice "Alice"]
                            [:bob "Not Bob"]])
        participants (mapcat #(.getParticipants ^Stage %) stages)
        ^BindingSet result (query/execute stages)]
    (is (some #(instance? RelationPattern %) participants))
    (is (= #{[:alice "Alice"]}
           (set (map vec (.getRows result)))))))

(deftest nested-or-inside-not-is-planned-recursively-test
  (h/transact fix/*node* [{:db/id :alice :name "Alice" :age 30}
                          {:db/id :bob :name "Bob" :salary 40}
                          {:db/id :cara :name "Cara" :sex :female}])

  (let [stages (plan-query '{:find [?e ?name]
                             :where [[?e :name ?name]
                                     (not (or [?e :age 30]
                                              [?e :salary 40]))]})
        participants (mapcat #(.getParticipants ^Stage %) stages)
        ^BindingSet result (query/execute stages)]
    (is (some #(instance? NotPattern %) participants))
    (is (= #{[:cara "Cara"]}
           (set (map vec (.getRows result)))))))

(deftest or-pattern-grounds-the-remaining-variables-as-the-only-proposer-test
  (h/transact fix/*node* [{:db/id :alice :name "Alice" :sex :male :age 30}
                          {:db/id :bob :name "Bob" :sex :male :salary 40}
                          {:db/id :cara :name "Cara" :sex :female :age 50}])

  (let [stages (plan-query '{:find [?e ?name ?amount]
                             :where [[?e :sex :male]
                                     (or (and [?e :name ?name]
                                              [?e :age ?amount])
                                         (and [?e :name ?name]
                                              [?e :salary ?amount]))]})
        ^Stage first-stage (first stages)
        ^Stage or-stage (second stages)
        or-participants (.getParticipants or-stage)
        ^BindingSet result (query/execute stages)]
    (testing "another pattern grounds a proper subset of the OR variables"
      (is (= '[?e] (.getAdded first-stage))))

    (testing "the OR is the sole proposer for every remaining variable"
      (is (= '[?name ?amount] (.getAdded or-stage)))
      (is (= '[?e ?name ?amount] (.getTargetVariables or-stage)))
      (is (= 1 (count or-participants)))
      (is (instance? OrPattern (first or-participants))))

    (is (= #{[:alice "Alice" 30]
             [:bob "Bob" 40]}
           (set (map vec (.getRows result)))))))
