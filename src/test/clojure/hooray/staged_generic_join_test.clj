(ns hooray.staged-generic-join-test
  (:require
   [clojure.spec.alpha :as s]
   [clojure.test :as t]
   [hooray.core :as h]
   [hooray.fixtures :as fix]
   [hooray.plan2 :as plan2]
   [hooray.query :as query]
   [hooray.staged-generic-join :as staged-generic-join])
  (:import
   (org.hooray.algo PrefixExtender)
   (org.hooray.engine BindingSet)))

(def unit-bindings (BindingSet. [] [[]]))

(defn- staged-query [db query-form & args]
  (let [conformed-query (s/conform ::query/query query-form)
        requested-variable-order (vec (query/query->variable-order conformed-query))
        scope (plan2/plan db conformed-query args requested-variable-order)
        var->idx (zipmap (:variable-order scope) (range))
        rows (.getRows ^BindingSet (staged-generic-join/execute scope unit-bindings))]
    ((query/compile-find (:find conformed-query) var->idx) rows)))

(t/use-fixtures :each fix/with-node fix/with-people-schema)

(t/deftest generic-stage-replays-incoming-bindings-from-level-zero-test
  (let [scope {:input-variables ['?seed]
               :variable-order ['?seed '?value]
               :extenders [(PrefixExtender/createSingleLevel ["x"] 1)]
               :stages [{:kind :generic
                         :target-variables ['?seed '?value]}]}
        input (BindingSet. ['?seed] [[1] [2]])
        result (staged-generic-join/execute scope input)]
    (t/is (= ['?seed '?value] (.getVariables ^BindingSet result)))
    (t/is (= [[1 "x"] [2 "x"]] (.getRows ^BindingSet result)))))

(t/deftest consecutive-generic-stages-preserve-layout-and-multiplicity-test
  (let [scope {:input-variables []
               :variable-order ['?number '?label]
               :extenders [(PrefixExtender/createSingleLevel [1] 0)
                           (PrefixExtender/createSingleLevel ["x" "y"] 1)]
               :stages [{:kind :generic
                         :target-variables ['?number]}
                        {:kind :generic
                         :target-variables ['?number '?label]}]}
        result (staged-generic-join/execute scope unit-bindings)]
    (t/is (= ['?number '?label] (.getVariables ^BindingSet result)))
    (t/is (= [[1 "x"] [1 "y"]] (.getRows ^BindingSet result)))))

(t/deftest empty-input-retains-the-planned-final-layout-test
  (let [scope {:input-variables ['?seed]
               :variable-order ['?seed '?value]
               :extenders [(PrefixExtender/createSingleLevel ["x"] 1)]
               :stages [{:kind :generic
                         :target-variables ['?seed '?value]}]}
        result (staged-generic-join/execute scope (BindingSet. ['?seed] []))]
    (t/is (= ['?seed '?value] (.getVariables ^BindingSet result)))
    (t/is (= [] (.getRows ^BindingSet result)))))

(t/deftest executor-rejects-invalid-runtime-boundaries-test
  (t/is (thrown-with-msg?
         IllegalArgumentException
         #"input variables"
         (staged-generic-join/execute
          {:input-variables ['?seed]
           :variable-order ['?seed]
           :extenders []
           :stages []}
          unit-bindings)))
  (t/is (thrown-with-msg?
         IllegalStateException
         #"Unknown stage kind"
         (staged-generic-join/execute
          {:input-variables []
           :variable-order []
           :extenders []
           :stages [{:kind :unknown
                     :target-variables []}]}
          unit-bindings))))

(t/deftest ordinary-extenders-execute-through-the-staged-plan-test
  (h/transact fix/*node* [{:db/id :alice :age 39}
                          {:db/id :bob :age 40}])

  (t/is (= #{[:alice 39 40]}
           (set (staged-query
                 (h/db fix/*node*)
                 '{:find [?e ?age ?next-age]
                   :where [[?e :age ?age]
                           [(< ?age 40)]
                           [(inc ?age) ?next-age]]})))))

(t/deftest predicate-order-and-function-dependencies-are-preserved-test
  (let [db (h/db fix/*node*)]
    (t/is (= [[10 11]]
             (staged-query
              db
              '{:find [?left ?right]
                :in [?right ?left]
                :where [[(<= ?left ?right)]]}
              11
              10)))
    (t/is (= [[1 2 3]]
             (staged-query
              db
              '{:find [?value ?next ?after]
                :in [?value]
                :where [[(inc ?value) ?next]
                        [(inc ?next) ?after]]}
              1)))))

(t/deftest relation-inputs-are-normalized-to-scope-order-test
  (h/transact fix/*node* [{:db/id :relation-witness :name "Relation witness"}])
  (let [db (h/db fix/*node*)]
    (t/is (= #{[1 2]}
             (set (staged-query
                   db
                   '{:find [?a ?b]
                     :in [[[?a ?b]] [[?b ?a]]]
                     :where [[?e :name "Relation witness"]]}
                   [[1 2] [3 4]]
                   [[2 1] [5 6]]))))
    (t/is (= #{[1 2 3 4]}
             (set (staged-query
                   db
                   '{:find [?a ?b ?c ?d]
                     :in [[[?b ?a ?c ?d]] [[?a ?b ?c ?d]]]
                     :where [[?e :name "Relation witness"]]}
                   [[2 1 3 4]]
                   [[1 2 3 4]]))))
    (t/is (= #{[1 2 3]}
             (set (staged-query
                   db
                   '{:find [?a ?b ?c]
                     :in [[[?b ?a]] [[?a ?b ?c]]]
                     :where [[?e :name "Relation witness"]]}
                   [[2 1]]
                   [[1 2 3]]))))))

(t/deftest or-branch-predicates-remain-attached-to-their-branch-test
  (h/transact fix/*node* [{:db/id :db/limit
                           :db/ident :limit
                           :db/valueType :db.type/long
                           :db/cardinality :db.cardinality/one}])
  (h/transact fix/*node* [{:db/id 1 :name "valid" :age 5 :salary 10 :limit 5}
                          {:db/id 2 :name "leaked" :age 20 :salary 10 :limit 5}])

  (t/is (= [["valid"]]
           (staged-query
            (h/db fix/*node*)
            '{:find [?name]
              :where [[?e :name ?name]
                      [?e :age ?age]
                      [?e :salary ?score]
                      [?e :limit ?limit]
                      (or (and [(< ?age ?score)]
                               [(< ?limit 10)])
                          (and [(> ?age ?score)]
                               [(< ?limit 0)]))]}))))

(t/deftest or-branches-can-produce-their-common-layout-in-different-orders-test
  (h/transact fix/*node* [{:db/id :alice :age 30}
                          {:db/id :bob :salary 40}])
  (let [db (h/db fix/*node*)
        query-form '{:find [?e ?a ?b]
                     :where [(or (and [?e :age ?a]
                                      [(inc ?a) ?b])
                                 (and [?e :salary ?b]
                                      [(dec ?b) ?a]))]}]
    (t/is (= #{[:alice 30 31]
               [:bob 39 40]}
             (set (staged-query db query-form))))
    (t/is (= #{[:alice 30 31]
               [:bob 39 40]}
             (set (staged-query
                   db
                   (assoc query-form :in '[[[?e ?a]]])
                   [[:alice 30]
                    [:bob 39]]))))))

(t/deftest nested-or-groundability-remains-all-or-nothing-test
  (h/transact fix/*node* [{:db/id 1 :age 35}
                          {:db/id 2 :age 35}
                          {:db/id 3 :age 35}])
  (let [db (h/db fix/*node*)]
    (t/is (= [[2 1]]
             (sort
              (staged-query
               db
               '{:find [?v ?e]
                 :where [(or (and (or [?v :next ?e]
                                      (and [?e :age 35]
                                           [(< ?v 3)]))
                                  [(inc ?e) ?v]))
                         [?e :age 35]]}))))
    (t/is (= [[2 1] [3 2] [4 3]]
             (sort
              (staged-query
               db
               '{:find [?v ?e]
                 :where [(or (and (or (and [(< ?v 3)] [?e :age 35])
                                      (and [(> ?v 1)] [?e :age 35]))
                                  [(inc ?e) ?v]))
                         [?e :age 35]]}))))))

(t/deftest or-materialization-uses-set-semantics-and-preserves-outer-columns-test
  (h/transact fix/*node* [{:db/id :alice :age 30}])

  (t/is (= [["outer" :alice 30]]
           (staged-query
            (h/db fix/*node*)
            '{:find [?tag ?e ?age]
              :in [?tag]
              :where [(or [?e :age ?age]
                          [?e :age ?age])]}
            "outer"))))

(t/deftest validating-or-preserves-duplicate-outer-rows-test
  (let [scope {:input-variables []
               :variable-order ['?value '?tag]
               :extenders [(PrefixExtender/createSingleLevel [1 1] 0)
                           (PrefixExtender/createSingleLevel ["outer"] 1)]
               :stages [{:kind :generic
                         :target-variables ['?value '?tag]}
                        {:kind :or
                         :variables ['?value]
                         :added []
                         :target-variables ['?value '?tag]
                         :branches [{:input-variables ['?value]
                                     :variable-order ['?value]
                                     :extenders []
                                     :stages [{:kind :generic
                                               :target-variables ['?value]}]}]}]}
        result (staged-generic-join/execute scope unit-bindings)]
    (t/is (= ['?value '?tag] (.getVariables ^BindingSet result)))
    (t/is (= [[1 "outer"] [1 "outer"]]
             (.getRows ^BindingSet result)))))
