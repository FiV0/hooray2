(ns hooray.incremental-test
  (:require [clojure.test :refer [deftest is testing]]
            [hooray.core :as h]
            [hooray.incremental :as incremental]
            [hooray.fixtures :as fix])
  (:import (org.hooray.incremental IntegerWeight)))

(def ^:private one IntegerWeight/ONE)
(def ^:private minus-one (IntegerWeight. -1))

(defn- weight-at
  "Get the weight at a path in an indexed zset. Returns nil if path doesn't exist."
  [indexed-zset & path]
  (loop [current indexed-zset
         [k & ks] path]
    (if (nil? k)
      current
      (let [next-val (get current k)]
        (if (nil? next-val)
          nil
          (if ks
            (recur next-val ks)
            next-val))))))

(defn- create-node []
  (h/connect {:type :mem :storage :hash :algo :hash}))

(deftest db->zset-indices-does-not-use-persistent-index-triple-test
  (testing "Initial index construction does not route through persistent index-triple updates"
    (let [node (create-node)
          _ (h/transact node fix/people-schema2)
          _ (h/transact node [{:db/id 1 :person/name "Alice"}])
          db (h/db node)]
      (with-redefs [incremental/index-triple
                    (fn [& _]
                      (throw (ex-info "index-triple should not be used during transient construction" {})))]
        (let [result (incremental/db->zset-indices db)]
          (is (= one (weight-at (:aev result) :person/name 1 "Alice")))
          (is (= one (weight-at (:ave result) :person/name "Alice" 1))))))))

(deftest calc-zset-indices-does-not-use-persistent-index-triple-test
  (testing "Delta index construction does not route through persistent index-triple updates"
    (let [node (create-node)
          db-before (h/db node)]
      (with-redefs [incremental/index-triple
                    (fn [& _]
                      (throw (ex-info "index-triple should not be used during transient construction" {})))]
        (let [result (incremental/calc-zset-indices db-before {:add [[1 :person/name "Alice"]]
                                                               :retract []})]
          (is (= one (weight-at (:aev result) :person/name 1 "Alice")))
          (is (= one (weight-at (:ave result) :person/name "Alice" 1))))))))

;; Test 1: Add with cardinality/one (default)
(deftest calc-zset-indices-add-cardinality-one-test
  (testing "Adding a triple with default cardinality/one creates positive weights in AEV and AVE"
    (let [node (create-node)
          db-before (h/db node)
          result (incremental/calc-zset-indices db-before {:add [[1 :person/name "Alice"]]
                                                           :retract []})]
      (testing "AEV index"
        (is (= one (weight-at (:aev result) :person/name 1 "Alice"))))
      (testing "AVE index"
        (is (= one (weight-at (:ave result) :person/name "Alice" 1)))))))

;; Test 2: Add with cardinality/many
(deftest calc-zset-indices-add-cardinality-many-test
  (testing "Adding a triple with cardinality/many creates positive weights in AEV and AVE"
    (let [node (create-node)
          _ (h/transact node [{:db/id :hobby-attr
                               :db/ident :person/hobby
                               :db/valueType :db.type/string
                               :db/cardinality :db.cardinality/many}])
          db-before (h/db node)
          result (incremental/calc-zset-indices db-before {:add [[1 :person/hobby "reading"]]
                                                           :retract []})]
      (testing "AEV index"
        (is (= one (weight-at (:aev result) :person/hobby 1 "reading"))))
      (testing "AVE index"
        (is (= one (weight-at (:ave result) :person/hobby "reading" 1)))))))

;; Test 3: Retract existing value
(deftest calc-zset-indices-retract-existing-test
  (testing "Retracting an existing triple creates negative weights in AEV and AVE"
    (let [node (create-node)
          _ (h/transact node fix/people-schema2)
          _ (h/transact node [{:db/id 1 :person/name "Alice"}])
          db-before (h/db node)
          result (incremental/calc-zset-indices db-before {:add []
                                                           :retract [[1 :person/name "Alice"]]})]
      (testing "AEV index has weight -1"
        (is (= minus-one (weight-at (:aev result) :person/name 1 "Alice"))))
      (testing "AVE index has weight -1"
        (is (= minus-one (weight-at (:ave result) :person/name "Alice" 1)))))))

;; Test 4: Retract non-existing value (no-op)
(deftest calc-zset-indices-retract-nonexisting-test
  (testing "Retracting a non-existing triple results in empty indices"
    (let [node (create-node)
          db-before (h/db node)
          result (incremental/calc-zset-indices db-before {:add []
                                                           :retract [[1 :person/name "Alice"]]})]
      (testing "AEV and AVE are empty"
        (is (empty? (:aev result)))
        (is (empty? (:ave result)))))))

;; Test 5: Add duplicate with cardinality/many (no-op)
(deftest calc-zset-indices-add-duplicate-many-test
  (testing "Adding a duplicate value with cardinality/many results in empty indices"
    (let [node (create-node)
          _ (h/transact node [{:db/id :hobby-attr
                               :db/ident :person/hobby
                               :db/valueType :db.type/string
                               :db/cardinality :db.cardinality/many}])
          _ (h/transact node [[:db/add 1 :person/hobby "reading"]])
          db-before (h/db node)
          result (incremental/calc-zset-indices db-before {:add [[1 :person/hobby "reading"]]
                                                           :retract []})]
      (testing "AEV and AVE are empty (no change needed)"
        (is (empty? (:aev result)))
        (is (empty? (:ave result)))))))

;; Test 6: Add overwrites with cardinality/one
(deftest calc-zset-indices-overwrite-one-test
  (testing "Adding a new value with cardinality/one retracts old and adds new"
    (let [node (create-node)
          _ (h/transact node fix/people-schema2)
          _ (h/transact node [{:db/id 1 :person/name "Alice"}])
          db-before (h/db node)
          result (incremental/calc-zset-indices db-before {:add [[1 :person/name "Bob"]]
                                                           :retract []})]
      (testing "AEV index has -1 for Alice and +1 for Bob"
        (is (= minus-one (weight-at (:aev result) :person/name 1 "Alice")))
        (is (= one (weight-at (:aev result) :person/name 1 "Bob"))))
      (testing "AVE index has -1 for Alice and +1 for Bob"
        (is (= minus-one (weight-at (:ave result) :person/name "Alice" 1)))
        (is (= one (weight-at (:ave result) :person/name "Bob" 1)))))))
