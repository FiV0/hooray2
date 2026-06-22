(ns hooray.query.plan-test
  (:require
   [clojure.spec.alpha :as s]
   [clojure.test :refer [deftest is testing]]
   [hooray.core :as h]
   [hooray.query :as query]
   [hooray.query.plan :as plan]))

(def ^:private opts {:type :mem :storage :hash :algo :generic})

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

(deftest planner-flattens-and-and-seeds-input-bindings
  (let [p (plan/plan (conform '{:find [?e]
                                :in [?name]
                                :where [(and [?e :name ?name]
                                             [?e :age ?age])]}))]
    (is (= '#{?name} (:initial-bound p)))
    (is (= [:triple :triple] (mapv :kind (:patterns p))))))

(deftest ordinary-stage-includes-full-and-partial-validators
  (let [p (plan/plan (conform '{:find [?e ?age]
                                :where [[?e :age ?age]
                                        [(< ?age 40)]
                                        [?e :name ?name]]}))
        [stage] (:stages p)]
    (is (= :ordinary (:kind stage)))
    (is (= '[?e ?age] (:introduces stage)))
    (is (= [[:triple :proposer]
            [:predicate :validator]
            [:triple :validator]]
           (mapv (juxt :kind :role) (:participants stage))))))

(deftest or-validates-when-an-ordinary-pattern-proposes-overlapping-variables
  (let [p (plan/plan (conform '{:find [?e ?age]
                                :where [[?e :age ?age]
                                        (or [?e :name "A"]
                                            [?e :title "A"])]}))
        [stage] (:stages p)]
    (is (= :ordinary (:kind stage)))
    (is (= [[:triple :proposer]
            [:or :validator]]
           (mapv (juxt :kind :role) (:participants stage))))))

(deftest or-only-query-uses-dedicated-or-proposal-boundary
  (let [p (plan/plan (conform '{:find [?a ?x]
                                :where [(or [?a :r ?x]
                                            [?a :s ?x])
                                        (or [?a :p ?x]
                                            [?a :q ?x])]}))
        [stage] (:stages p)]
    (is (= :or-proposal-boundary (:kind stage)))
    (is (= '[?a ?x] (:introduces stage)))
    (is (= [[:or :proposer]
            [:or :validator]]
           (mapv (juxt :kind :role) (:participants stage))))))

(deftest internal-binding-set-query-runs-all-triple-query
  (let [node (fresh-node)
        q '{:find [?age]
            :where [["a" :age ?age]]}]
    (h/transact node [{:db/id "a" :name "A" :age 35}
                      {:db/id "b" :name "B" :age 40}])
    (is (= [[35]]
           (plan/execute-query (h/db node) q [])))))

(deftest internal-binding-set-query-reuses-final-result-shaping
  (let [node (fresh-node)
        q '{:find [?age]
            :keys [age]
            :where [["a" :age ?age]]}]
    (h/transact node [{:db/id "a" :name "A" :age 35}])
    (is (= (h/q q (h/db node))
           (plan/execute-query (h/db node) q [])))))

(deftest internal-binding-set-query-supports-scalar-in-binding
  (let [node (fresh-node)
        q '{:find [?e]
            :in [?name]
            :where [[?e :name ?name]]}]
    (h/transact node [{:db/id "a" :name "A" :age 35}
                      {:db/id "b" :name "B" :age 40}])
    (is (= (h/q q (h/db node) "A")
           (plan/execute-query (h/db node) q ["A"])))))

(deftest internal-binding-set-query-preserves-relation-in-correlation
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
    (is (= (h/q q (h/db node) input)
           (plan/execute-query (h/db node) q [input])))))

(deftest internal-binding-set-query-preserves-or-branch-predicate-identity
  (let [node (fresh-node)]
    (h/transact node [{:db/id "a" :name "A" :age 35}
                      {:db/id "b" :name "B" :age 35}])
    (is (= [["B" 35]]
           (plan/execute-query
            (h/db node)
            '{:find [?name ?age]
              :where
              [[?e :age ?age]
               (or
                (and [?e :name "A"]
                     [(< ?age 30)])
                (and [?e :name "B"]
                     [(< ?age 40)]))
               [?e :name ?name]]}
            [])))))

(deftest internal-binding-set-query-validates-or-only-intersections
  (let [node (fresh-node)]
    (h/transact node [{:db/id "a" :name "A" :age 35}
                      {:db/id "b" :name "B" :age 40}
                      {:db/id "c" :name "A" :age 50}])
    (is (= [["a"] ["b"]]
           (plan/execute-query
            (h/db node)
            '{:find [?e]
              :where
              [(or [?e :name "A"]
                   [?e :name "B"])
               (or [?e :age 35]
                   [?e :age 40])]}
            [])))))

(deftest internal-binding-set-query-supports-not-antijoin
  (let [node (fresh-node)
        q '{:find [?e]
            :where [[?e :name ?name]
                    (not [?e :age 40])]}]
    (h/transact node [{:db/id "a" :name "A" :age 35}
                      {:db/id "b" :name "B" :age 40}])
    (is (= (h/q q (h/db node))
           (plan/execute-query (h/db node) q [])))))
