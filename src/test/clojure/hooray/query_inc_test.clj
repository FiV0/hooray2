(ns hooray.query-inc-test
  (:require [clojure.test :as t :refer [deftest is testing]]
            [hooray.fixtures :as fix]
            [hooray.core :as h])
  (:import (clojure.lang ExceptionInfo)))

(t/use-fixtures :each fix/with-node)

(def ^:dynamic *inc-q* nil)

(defmacro with-inc-q [q & body]
  `(let [inc-q# (h/q-inc fix/*node* ~q)]
     (binding [*inc-q* inc-q#]
       (try
         ~@body
         (finally
           (h/unregister-inc-q fix/*node* inc-q#))))))

(defmacro with-transaction-and-inc-q [tx-data q & body]
  `(let [inc-q# (h/q-inc fix/*node* ~q)]
     (h/transact fix/*node* ~tx-data)
     (binding [*inc-q* inc-q#]
       (try
         ~@body
         (finally
           (h/unregister-inc-q fix/*node* inc-q#))))))

(deftest test-sanity-check
  (with-inc-q '{:find [e]
                :where [[e :name "Ivan"]]}

    (h/transact fix/*node* [{:db/id 1 :name "Ivan"}])

    (is (= [[[1] 1]] (h/consume-delta! *inc-q*)))))

(deftest with-previous-value
  (h/transact fix/*node* [{:db/id 1 :name "Ivan"}])
  (with-inc-q '{:find [name]
                :where [[1 :name name]]}

    (h/transact fix/*node* [{:db/id 1 :name "Ivanov"}])

    (is (= [[["Ivan"] -1]
            [["Ivanov"] 1]]
           (h/consume-delta! *inc-q*)))))

(deftest test-basic-query-1
  (with-transaction-and-inc-q
      [{:db/id :ivan :name "Ivan" :last-name "Ivanov"}
       {:db/id :petr :name "Petr" :last-name "Petrov"}]

      '{:find [name]
        :where [[e :name "Ivan"]
                [e :name name]]}

    (t/is (= [[["Ivan"] 1]] (h/consume-delta! *inc-q*)))))

(deftest test-basic-query-2
  (t/testing "Can query entity by single field"
    (with-transaction-and-inc-q
        [{:db/id :ivan :name "Ivan" :last-name "Ivanov"}
         {:db/id :petr :name "Petr" :last-name "Petrov"}]

        '{:find [e]
          :where [[e :name "Ivan"]]}

      (t/is (= [[[:ivan] 1]] (h/consume-delta! *inc-q*))))))

(deftest test-basic-query-3
  (t/testing "Can query using multiple terms"
    (with-transaction-and-inc-q
        [{:db/id :ivan :name "Ivan" :last-name "Ivanov"}
         {:db/id :petr :name "Petr" :last-name "Petrov"}]

        '{:find [name last-name]
          :where [[e :name name]
                  [e :last-name last-name]
                  [e :name "Ivan"]
                  [e :last-name "Ivanov"]]}

      (t/is (= [[["Ivan" "Ivanov"] 1]]
               (h/consume-delta! *inc-q*))))))

(deftest test-basic-query-4
  (t/testing "Negate query based on subsequent non-matching clause"
    (with-transaction-and-inc-q
        [{:db/id :ivan :name "Ivan" :last-name "Ivanov"}
         {:db/id :petr :name "Petr" :last-name "Petrov"}]

        '{:find [e]
          :where [[e :name "Ivan"]
                  [e :last-name "Ivanov-does-not-match"]]}

      (t/is (= [] (h/consume-delta! *inc-q*))))))

(deftest test-basic-query-5
  (t/testing "Can query for multiple results"
    (with-transaction-and-inc-q
        [{:db/id :ivan :name "Ivan"}
         {:db/id :petr :name "Petr"}]

        '{:find [name]
          :where [[e :name name]]}

      (t/is (= [[["Ivan"] 1]
                [["Petr"] 1]]
               (h/consume-delta! *inc-q*))))))

(deftest test-basic-query-6
  (t/testing "Can query across fields for same value"
    (with-transaction-and-inc-q
        [{:db/id :ivan :name "Ivan" :last-name "Ivanov"}
         {:db/id :petr :name "Petr" :last-name "Petrov"}
         {:db/id :smith :name "Smith" :last-name "Smith"}]

        '{:find [p1] :where [[p1 :name name]
                             [p1 :last-name name]]}

      (t/is (= [[[:smith] 1]]
               (h/consume-delta! *inc-q*))))))

(deftest test-basic-query-7
  (t/testing "Can query across fields for same value when value is passed in"
    (with-transaction-and-inc-q
        [{:db/id :ivan :name "Ivan" :last-name "Ivanov"}
         {:db/id :petr :name "Petr" :last-name "Petrov"}
         {:db/id :smith :name "Smith" :last-name "Smith"}]

        '{:find [p1] :where [[p1 :name name]
                             [p1 :last-name name]
                             [p1 :name "Smith"]]}
      (t/is (= [[[:smith] 1]]
               (h/consume-delta! *inc-q*))))))

(deftest test-basic-retractions-1
  (h/transact fix/*node* [{:db/id :ivan :name "Ivan" :last-name "Ivanov"}
                          {:db/id :petr :name "Petr" :last-name "Petrov"}])
  (with-transaction-and-inc-q
      [[:db/add :ivan :name "Ivanova"]]

      '{:find [name]
        :where [[e :name name]]}

    (t/is (= [[["Ivan"] -1] [["Ivanova"] 1]]
             (h/consume-delta! *inc-q*)))))

(deftest test-basic-retractions-2
  (h/transact fix/*node* [{:db/id :ivan :name "Ivan" :last-name "Ivanov"}
                          {:db/id :petr :name "Petr" :last-name "Petrov"}])
  (with-transaction-and-inc-q
      [[:db/retractEntity :ivan]]

      '{:find [name]
        :where [[e :name name]]}

    (t/is (= [[["Ivan"] -1]]
             (h/consume-delta! *inc-q*)))))
