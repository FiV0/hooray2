(ns hooray.query.plan-test
  (:require
   [clojure.spec.alpha :as s]
   [clojure.test :refer [deftest is testing]]
   [hooray.core :as h]
   [hooray.query :as query]
   [hooray.query.plan :as plan]))

(def ^:private opts {:type :mem :storage :hash :algo :binding-set-wco})

(def ^:private schema
  [{:db/id -1
    :db/ident :name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/id -2
    :db/ident :age
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one}])

(defn- fresh-node []
  (doto (h/connect opts)
    (h/transact schema)))

(defn- conform [q]
  (let [conformed (s/conform ::query/query q)]
    (is (not= ::s/invalid conformed))
    conformed))

(deftest planner-seeds-input-bindings
  (let [p (plan/plan (conform '{:find [?e]
                                :in [?name]
                                :where [[?e :name ?name]]}))]
    (is (= '#{?name} (:initial-bound p)))
    (is (= [:triple] (mapv :kind (:patterns p))))))

(deftest planner-flattens-and-inside-or-branches
  (let [p (plan/plan (conform '{:find [?e ?name ?age]
                                :where [(or
                                         (and [?e :name ?name]
                                              [?e :age ?age])
                                         (and [?e :name ?name]
                                              [?e :age ?age]))]}))
        [or-pattern] (:patterns p)]
    (is (= :or (:kind or-pattern)))
    (is (= [[:triple :triple]
            [:triple :triple]]
           (mapv (fn [branch]
                   (mapv :kind (:patterns branch)))
                 (:branches or-pattern))))))

(deftest ordinary-stage-includes-full-and-partial-validators
  (let [p (plan/plan (conform '{:find [?e ?age]
                                :where [[?e :age ?age]
                                        [(< ?age 40)]
                                        [?e :name ?name]]}))
        [entity-stage age-stage name-stage] (:stages p)]
    (is (= '[?e] (:introduces entity-stage)))
    (is (= [:triple :triple]
           (mapv :kind (:participants entity-stage))))
    (is (= '[?age] (:introduces age-stage)))
    (is (= [:triple :predicate]
           (mapv :kind (:participants age-stage))))
    (is (= '[?name] (:introduces name-stage)))
    (is (= [:triple]
           (mapv :kind (:participants name-stage))))))

(deftest or-validates-when-an-ordinary-pattern-proposes-overlapping-variables
  (let [p (plan/plan (conform '{:find [?e ?age]
                                :where [[?e :age ?age]
                                        (or [?e :name "A"]
                                            [?e :title "A"])]}))
        [stage] (:stages p)]
    (is (= '[?e] (:introduces stage)))
    (is (= [:triple :or]
           (mapv :kind (:participants stage))))))

(deftest or-only-query-introduces-all-or-variables-together
  (let [p (plan/plan (conform '{:find [?a ?x]
                                :where [(or [?a :r ?x]
                                            [?a :s ?x])
                                        (or [?a :p ?x]
                                            [?a :q ?x])]}))
        [stage] (:stages p)]
    (is (= '[?a ?x] (:introduces stage)))
    (is (= [:or :or]
           (mapv :kind (:participants stage))))))

(deftest binding-set-wco-query-runs-all-triple-query
  (let [node (fresh-node)
        q '{:find [?age]
            :where [["a" :age ?age]]}]
    (h/transact node [{:db/id "a" :name "A" :age 35}
                      {:db/id "b" :name "B" :age 40}])
    (is (= [[35]]
           (h/q q (h/db node))))))

(deftest binding-set-wco-query-reuses-final-result-shaping
  (let [node (fresh-node)
        q '{:find [?age]
            :keys [age]
            :where [["a" :age ?age]]}]
    (h/transact node [{:db/id "a" :name "A" :age 35}])
    (is (= [{:age 35}]
           (h/q q (h/db node))))))

(deftest binding-set-wco-query-supports-scalar-in-binding
  (let [node (fresh-node)
        q '{:find [?e]
            :in [?name]
            :where [[?e :name ?name]]}]
    (h/transact node [{:db/id "a" :name "A" :age 35}
                      {:db/id "b" :name "B" :age 40}])
    (is (= [["a"]]
           (h/q q (h/db node) "A")))))

(deftest binding-set-wco-query-preserves-relation-in-correlation
  (let [node (fresh-node)
        q '{:find [?e]
            :in [[[?name ?age]]]
            :where [[?e :name ?name]
                    [?e :age ?age]]}
        input [["A" 35]
               ["B" 40]]]
    (h/transact node [{:db/id "a" :name "A" :age 35}
                      {:db/id "b" :name "A" :age 40}
                      {:db/id "c" :name "B" :age 40}])
    (is (= [["a"] ["c"]]
           (h/q q (h/db node) input)))))

(deftest binding-set-wco-query-preserves-or-branch-predicate-identity
  (let [node (fresh-node)]
    (h/transact node [{:db/id "a" :name "A" :age 35}
                      {:db/id "b" :name "B" :age 35}])
    (is (= [["B" 35]]
           (h/q '{:find [?name ?age]
                  :where
                  [[?e :age ?age]
                   (or
                    (and [?e :name "A"]
                         [(< ?age 30)])
                    (and [?e :name "B"]
                         [(< ?age 40)]))
                   [?e :name ?name]]}
                (h/db node))))))

(deftest binding-set-wco-query-validates-or-only-intersections
  (let [node (fresh-node)]
    (h/transact node [{:db/id "a" :name "A" :age 35}
                      {:db/id "b" :name "B" :age 40}
                      {:db/id "c" :name "A" :age 50}])
    (is (= [["a"] ["b"]]
           (h/q '{:find [?e]
                  :where
                  [(or [?e :name "A"]
                       [?e :name "B"])
                   (or [?e :age 35]
                       [?e :age 40])]}
                (h/db node))))))

(deftest binding-set-wco-query-supports-not-antijoin
  (let [node (fresh-node)
        q '{:find [?e]
            :where [[?e :name ?name]
                    (not [?e :age 40])]}]
    (h/transact node [{:db/id "a" :name "A" :age 35}
                      {:db/id "b" :name "B" :age 40}])
    (is (= [["a"]]
           (h/q q (h/db node))))))
