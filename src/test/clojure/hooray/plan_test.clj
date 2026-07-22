(ns hooray.plan-test
  (:require
   [clojure.spec.alpha :as s]
   [clojure.test :as t :refer [deftest is testing]]
   [hooray.core :as h]
   [hooray.fixtures :as fix]
   [hooray.plan :as plan]
   [hooray.query :as query])
  (:import
   (org.hooray.engine
    BindingSet
    FunctionPattern
    IStage
    NotPattern
    OrPattern
    RelationPattern)))

(t/use-fixtures :each fix/with-node fix/with-people-schema)

(defn- plan-query [query & args]
  (let [conformed-query (s/conform ::query/query query)
        variable-order (vec (query/query->variable-order conformed-query))]
    (query/validate-query conformed-query)
    (plan/plan (h/db fix/*node*) conformed-query args variable-order)))

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

(deftest or-pattern-grounds-all-variables-as-the-only-proposer-test
  (h/transact fix/*node* [{:db/id :alice :name "Alice" :age 30}
                          {:db/id :bob :name "Bob" :salary 40}])

  (let [stages (plan-query '{:find [?e ?name ?amount]
                             :where [(or (and [?e :name ?name]
                                              [?e :age ?amount])
                                         (and [?e :salary ?amount]
                                              [?e :name ?name]))]})
        ^IStage stage (first stages)
        participants (.getParticipants stage)
        result (query/execute stages)]
    (is (= 1 (count stages)))
    (is (= '[?e ?name ?amount] (.getAdded stage)))
    (is (= '[?e ?name ?amount] (.getTargetVariables stage)))
    (is (= 1 (count participants)))
    (is (instance? OrPattern (first participants)))
    (is (= #{[:alice "Alice" 30]
             [:bob "Bob" 40]}
           (set (map vec result))))))

(deftest input-bindings-are-planned-as-relations-test
  (h/transact fix/*node* [{:db/id :alice :name "Alice"}
                          {:db/id :bob :name "Bob"}])

  (let [stages (plan-query '{:find [?e ?name]
                             :in [[[?e ?name]]]
                             :where [[?e :name ?name]]}
                           [[:alice "Alice"]
                            [:bob "Not Bob"]])
        ^IStage entity-stage (first stages)
        ^IStage name-stage (second stages)
        participants (mapcat #(.getParticipants ^IStage %) stages)
        result (query/execute stages)]
    (is (= '[?e] (.getAdded entity-stage)))
    (is (= '[?name] (.getAdded name-stage)))
    (is (some #(instance? RelationPattern %) participants))
    (is (= #{[:alice "Alice"]}
           (set (map vec result))))))

(deftest function-output-becomes-groundable-after-its-arguments-test
  (let [stages (plan-query '{:find [?x ?y]
                             :in [?x]
                             :where [[(+ ?x 2) ?y]]}
                           5)
        ^IStage argument-stage (first stages)
        ^IStage output-stage (second stages)
        result (query/execute stages)]
    (is (= '[?x] (.getAdded argument-stage)))
    (is (not-any? #(instance? FunctionPattern %) (.getParticipants argument-stage)))
    (is (= '[?y] (.getAdded output-stage)))
    (is (some #(instance? FunctionPattern %) (.getParticipants output-stage)))
    (is (= [[5 7]] (mapv vec result)))))

(deftest nested-or-inside-not-is-planned-recursively-test
  (h/transact fix/*node* [{:db/id :alice :name "Alice" :age 30}
                          {:db/id :bob :name "Bob" :salary 40}
                          {:db/id :cara :name "Cara" :sex :female}])

  (let [stages (plan-query '{:find [?e ?name]
                             :where [[?e :name ?name]
                                     (not (or [?e :age 30]
                                              [?e :salary 40]))]})
        participants (mapcat #(.getParticipants ^IStage %) stages)
        result (query/execute stages)]
    (is (some #(instance? NotPattern %) participants))
    (is (= #{[:cara "Cara"]}
           (set (map vec result))))))

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
        ^IStage first-stage (first stages)
        ^IStage or-stage (second stages)
        or-participants (.getParticipants or-stage)
        result (query/execute stages)]
    (testing "another pattern grounds a proper subset of the OR variables"
      (is (= '[?e] (.getAdded first-stage))))

    (testing "the OR is the sole proposer for every remaining variable"
      (is (= '[?name ?amount] (.getAdded or-stage)))
      (is (= '[?e ?name ?amount] (.getTargetVariables or-stage)))
      (is (= 1 (count or-participants)))
      (is (instance? OrPattern (first or-participants))))

    (is (= #{[:alice "Alice" 30]
             [:bob "Bob" 40]}
           (set (map vec result))))))

(deftest or-groundability-follows-each-branches-planned-stage-order-test
  (h/transact fix/*node* [{:db/id :alice :age 30}
                          {:db/id :bob :salary 40}])

  (let [stages (plan-query '{:find [?e ?amount ?incremented]
                             :where [(or (and [?e :age ?amount]
                                              [(inc ?amount) ?incremented])
                                         (and [?e :salary ?amount]
                                              [(inc ?amount) ?incremented]))]})
        ^IStage stage (first stages)
        result (query/execute stages)]
    (is (= 1 (count stages)))
    (is (= '[?e ?amount ?incremented] (.getAdded stage)))
    (is (= #{[:alice 30 31]
             [:bob 40 41]}
           (set (map vec result))))))

(deftest or-groundability-requires-every-branch-to-produce-a-variable-test
  (let [query '{:find [?e ?amount ?incremented]
                :in [?e ?amount]
                :where [(or (and [?e :age ?amount]
                                 [(inc ?amount) ?incremented])
                            (and [?e :salary ?amount]
                                 [(> ?incremented 0)]))]}
        conformed-query (s/conform ::query/query query)
        variable-order (vec (query/query->variable-order conformed-query))]
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"\?incremented not bound"
         (plan/plan (h/db fix/*node*) conformed-query [:alice 30] variable-order)))))

(deftest or-pattern-validates-only-after-all-of-its-variables-are-grounded-test
  (h/transact fix/*node* [{:db/id :alice
                           :name "Alice"
                           :sex :male
                           :age 30
                           :salary 40}])

  (let [stages (plan-query '{:find [?e ?name ?amount]
                             :where [[?e :name ?name]
                                     [?e :salary ?amount]
                                     (or (and [?e :name ?name]
                                              [?e :age ?amount])
                                         (and [?e :sex ?name]
                                              [?e :salary ?amount]))]})
        ^IStage name-stage (first (filter #(= '[?name] (.getAdded ^IStage %)) stages))
        ^IStage amount-stage (first (filter #(= '[?amount] (.getAdded ^IStage %)) stages))
        result (query/execute stages)]
    (testing "the OR does not validate a partial tuple"
      (is (not-any? #(instance? OrPattern %) (.getParticipants name-stage))))

    (testing "the OR validates the complete tuple without leaking values across branches"
      (is (some #(instance? OrPattern %) (.getParticipants amount-stage)))
      (is (empty? result)))))

(deftest grouped-or-proposal-runs-other-validators-in-a-following-stage-test
  (h/transact fix/*node* [{:db/id :alice :name "Alice" :age 30}
                          {:db/id :bob :name "Bob" :salary 40}])

  (let [stages (plan-query '{:find [?e ?name ?amount]
                             :where [(or (and [?e :name ?name]
                                              [?e :age ?amount])
                                         (and [?e :salary ?amount]
                                              [?e :name ?name]))
                                     [(> ?amount 35)]]})
        ^IStage or-stage (first stages)
        ^IStage validation-stage (second stages)
        result (query/execute stages)]
    (is (= 2 (count stages)))
    (is (= '[?e ?name ?amount] (.getAdded or-stage)))
    (is (= 1 (count (.getParticipants or-stage))))
    (is (instance? OrPattern (first (.getParticipants or-stage))))
    (is (empty? (.getAdded validation-stage)))
    (is (= [[:bob "Bob" 40]]
           (mapv vec result)))))
