(ns hooray.dbsp-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [hooray.dbsp :as dbsp]))

(defn- patterns [query]
  (:patterns (dbsp/parse query)))

(defn- order-indices [query]
  (mapv :index (dbsp/left-deep-order (patterns query))))

;; --------------------------------------------------------------------------
;; Pattern descriptors
;; --------------------------------------------------------------------------

(deftest compile-pattern-test
  (testing "two variables"
    (let [[p] (patterns '{:find [name] :where [[?e :name name]]})]
      (is (= :name (:attr p)))
      (is (= {:kind :variable :var '?e} (:e p)))
      (is (= {:kind :variable :var 'name} (:v p)))
      (is (= '[?e name] (:vars p)))))

  (testing "constant value"
    (let [[p] (patterns '{:find [?e] :where [[?e :name "Ivan"]]})]
      (is (= {:kind :constant :value "Ivan"} (:v p)))
      (is (= '[?e] (:vars p)))))

  (testing "constant entity"
    (let [[p] (patterns '{:find [name] :where [[1 :name name]]})]
      (is (= {:kind :constant :value 1} (:e p)))
      (is (= '[name] (:vars p)))))

  (testing "indices follow :where position"
    (is (= [0 1 2]
           (mapv :index (patterns '{:find [name]
                                    :where [[?e :name name]
                                            [?e :age age]
                                            [?e :last-name ln]]}))))))

(deftest rejects-unsupported-clauses-test
  (testing "predicate clauses are rejected"
    (is (thrown? clojure.lang.ExceptionInfo
                 (dbsp/parse '{:find [?x] :where [[?x :age ?y] [(= ?y 1)]]})))))

;; --------------------------------------------------------------------------
;; Left-deep join order
;; --------------------------------------------------------------------------

(deftest left-deep-order-test
  (testing "patterns all sharing one variable keep query order"
    (is (= [0 1 2]
           (order-indices '{:find [name]
                            :where [[?e :name name]
                                    [?e :age age]
                                    [?e :last-name ln]]}))))

  (testing "connectivity reorders patterns ahead of disconnected ones"
    ;; p0 [?a ?b], p1 [?c ?d], p2 [?b ?c]: 0 -> 2 (shares ?b) -> 1 (shares ?c)
    (is (= [0 2 1]
           (order-indices '{:find [?a]
                            :where [[?a :foo ?b]
                                    [?c :bar ?d]
                                    [?b :baz ?c]]}))))

  (testing "fully disconnected patterns fall back to index order (Cartesian)"
    (is (= [0 1]
           (order-indices '{:find [?a]
                            :where [[?a :foo ?b]
                                    [?c :bar ?d]]}))))

  (testing "single pattern"
    (is (= [0] (order-indices '{:find [name] :where [[?e :name name]]}))))

  (testing "deterministic — same query yields the same order"
    (let [q '{:find [?a]
              :where [[?a :foo ?b]
                      [?c :bar ?d]
                      [?b :baz ?c]
                      [?d :qux ?a]]}]
      (is (= (order-indices q) (order-indices q))))))
