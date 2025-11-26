(ns hooray.db-test
  (:require [clojure.test :refer [deftest is testing]]
            [hooray.core :as core]))

(deftest transact-single-entity-test
  (testing "Transacting a single entity populates all indexes correctly"
    (doseq [storage [:hash :avl #_:btree]]
      (testing (str "with storage type: " storage)
        (let [node (core/connect {:type :mem :storage storage :algo :hash})
              _ (core/transact node [{:db/id 1
                                      :person/name "Alice"
                                      :person/age 30}])
              db (core/db node)]

          ;; Test EAV index
          (testing "EAV index"
            (is (contains? (:eav db) 1))
            (is (contains? (get-in db [:eav 1]) :person/name))
            (is (contains? (get-in db [:eav 1]) :person/age))
            (is (contains? (get-in db [:eav 1 :person/name]) "Alice"))
            (is (contains? (get-in db [:eav 1 :person/age]) 30)))

          ;; Test AEV index
          (testing "AEV index"
            (is (contains? (:aev db) :person/name))
            (is (contains? (:aev db) :person/age))
            (is (contains? (get-in db [:aev :person/name 1]) "Alice"))
            (is (contains? (get-in db [:aev :person/age 1]) 30)))

          ;; Test AVE index
          (testing "AVE index"
            (is (contains? (:ave db) :person/name))
            (is (contains? (:ave db) :person/age))
            (is (contains? (get-in db [:ave :person/name "Alice"]) 1))
            (is (contains? (get-in db [:ave :person/age 30]) 1)))

          ;; Test VAE index
          (testing "VAE index"
            (is (contains? (:vae db) "Alice"))
            (is (contains? (:vae db) 30))
            (is (contains? (get-in db [:vae "Alice" :person/name]) 1))
            (is (contains? (get-in db [:vae 30 :person/age]) 1))))))))

(deftest transact-multiple-entities-test
  (testing "Transacting multiple entities"
    (doseq [storage [:hash :avl #_:btree]]
      (testing (str "with storage type: " storage)
        (let [node (core/connect {:type :mem :storage storage :algo :hash})
              _ (core/transact node [{:db/id 1
                                      :person/name "Alice"
                                      :person/age 30}
                                     {:db/id 2
                                      :person/name "Bob"
                                      :person/age 25}])
              db (core/db node)]

          (testing "Both entities exist in EAV"
            (is (contains? (:eav db) 1))
            (is (contains? (:eav db) 2))
            (is (contains? (get-in db [:eav 1 :person/name]) "Alice"))
            (is (contains? (get-in db [:eav 2 :person/name]) "Bob")))

          (testing "AVE index contains both entities for person/name"
            (is (contains? (get-in db [:ave :person/name "Alice"]) 1))
            (is (contains? (get-in db [:ave :person/name "Bob"]) 2))))))))

(deftest transact-db-add-test
  (testing "Transacting with :db/add transaction form"
    (doseq [storage [:hash :avl #_:btree]]
      (testing (str "with storage type: " storage)
        (let [node (core/connect {:type :mem :storage storage :algo :hash})
              _ (core/transact node [[:db/add 1 :person/name "Charlie"]
                                     [:db/add 1 :person/age 35]])
              db (core/db node)]

          (testing "EAV index"
            (is (contains? (get-in db [:eav 1 :person/name]) "Charlie"))
            (is (contains? (get-in db [:eav 1 :person/age]) 35)))

          (testing "AVE index"
            (is (contains? (get-in db [:ave :person/name "Charlie"]) 1))
            (is (contains? (get-in db [:ave :person/age 35]) 1))))))))

(deftest transact-multiple-values-same-attribute-test
  (testing "Multiple values for same attribute"
    (doseq [storage [:hash :avl #_:btree]]
      (testing (str "with storage type: " storage)
        (let [node (core/connect {:type :mem :storage storage :algo :hash})
              _ (core/transact node [{:db/id :hobby-attribute
                                      :db/ident :person/hobby
                                      :db/valueType :db.type/string
                                      :db/cardinality :db.cardinality/many}])
              _ (core/transact node [[:db/add 1 :person/hobby "reading"]
                                     [:db/add 1 :person/hobby "swimming"]
                                     [:db/add 1 :person/hobby "coding"]])
              db (core/db node)]

          (testing "EAV index contains all hobbies"
            (is (contains? (get-in db [:eav 1 :person/hobby]) "reading"))
            (is (contains? (get-in db [:eav 1 :person/hobby]) "swimming"))
            (is (contains? (get-in db [:eav 1 :person/hobby]) "coding"))
            (is (= 3 (count (get-in db [:eav 1 :person/hobby])))))

          (testing "AVE index maps each hobby back to entity"
            (is (contains? (get-in db [:ave :person/hobby "reading"]) 1))
            (is (contains? (get-in db [:ave :person/hobby "swimming"]) 1))
            (is (contains? (get-in db [:ave :person/hobby "coding"]) 1))))))))

(deftest transact-sequential-transactions-test
  (testing "Sequential transactions maintain history"
    (doseq [storage [:hash :avl #_:btree]]
      (testing (str "with storage type: " storage)
        (let [node (core/connect {:type :mem :storage storage :algo :hash})
              _ (core/transact node [{:db/id 1 :person/name "Alice" :person/age 30}])
              _ (core/transact node [{:db/id 2 :person/name "Bob" :person/age 25}])
              dbs (-> node :!dbs deref)
              db1 (nth dbs 1)
              db2 (nth dbs 2)]

          (testing "First db has only Alice"
            (is (contains? (:eav db1) 1))
            (is (not (contains? (:eav db1) 2))))

          (testing "Second db has both Alice and Bob"
            (is (contains? (:eav db2) 1))
            (is (contains? (:eav db2) 2))
            (is (contains? (get-in db2 [:eav 1 :person/name]) "Alice"))
            (is (contains? (get-in db2 [:eav 2 :person/name]) "Bob"))))))))

(deftest transact-mixed-transaction-forms-test
  (testing "Mixing entity maps and :db/add forms"
    (doseq [storage [:hash :avl #_:btree]]
      (testing (str "with storage type: " storage)
        (let [node (core/connect {:type :mem :storage storage :algo :hash})
              _ (core/transact node [{:db/id 1 :person/name "Alice"}
                                     [:db/add 1 :person/age 30]
                                     [:db/add 1 :person/hobby "reading"]])
              db (core/db node)]

          (testing "All data is in EAV"
            (is (contains? (get-in db [:eav 1 :person/name]) "Alice"))
            (is (contains? (get-in db [:eav 1 :person/age]) 30))
            (is (contains? (get-in db [:eav 1 :person/hobby]) "reading"))))))))

(deftest empty-db-test
  (testing "Empty database has empty indexes"
    (doseq [storage [:hash :avl #_:btree]]
      (testing (str "with storage type: " storage)
        (let [node (core/connect {:type :mem :storage storage :algo :hash})
              db (core/db node)]

          (is (empty? (:eav db)))
          (is (empty? (:aev db)))
          (is (empty? (:ave db)))
          (is (empty? (:vae db))))))))
