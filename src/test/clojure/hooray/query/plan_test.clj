(ns hooray.query.plan-test
  (:require
   [clojure.spec.alpha :as s]
   [clojure.test :refer [deftest is testing]]
   [hooray.query :as query]
   [hooray.query.plan :as plan]))

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
