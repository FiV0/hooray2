(ns hooray.query-inc-test
  (:require [clojure.test :as t :refer [deftest is testing]]
            [clojure.spec.alpha :as s]
            [hooray.query :as query]
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

    (t/is (= [[[:ivan] 1]] (h/consume-delta! *inc-q*))))



  #_#_#_#_#_#_

  (t/testing "Can query using multiple terms"
    (t/is (= #{["Ivan" "Ivanov"]} (h/q '{:find [name last-name]
                                         :where [[e :name name]
                                                 [e :last-name last-name]
                                                 [e :name "Ivan"]
                                                 [e :last-name "Ivanov"]]} (h/db fix/*node*)))))

  (t/testing "Negate query based on subsequent non-matching clause"
    (t/is (= #{} (h/q '{:find [e]
                        :where [[e :name "Ivan"]
                                [e :last-name "Ivanov-does-not-match"]]} (h/db fix/*node*)))))

  (t/testing "Can query for multiple results"
    (t/is (= #{["Ivan"] ["Petr"]}
             (h/q '{:find [name] :where [[e :name name]]} (h/db fix/*node*)))))

  (h/transact fix/*node* [{:db/id :smith :name "Smith" :last-name "Smith"}])

  (t/testing "Can query across fields for same value"
    (t/is (= #{[:smith]}
             (h/q '{:find [p1] :where [[p1 :name name]
                                       [p1 :last-name name]]} (h/db fix/*node*)))))

  (t/testing "Can query across fields for same value when value is passed in"
    (t/is (= #{[:smith]}
             (h/q '{:find [p1] :where [[p1 :name name]
                                       [p1 :last-name name]
                                       [p1 :name "Smith"]]} (h/db fix/*node*))))))

(deftest test-basic-query-2
  (t/testing "Can query entity by single field"
    (with-transaction-and-inc-q
        [{:db/id :ivan :name "Ivan" :last-name "Ivanov"}
         {:db/id :petr :name "Petr" :last-name "Petrov"}]

        '{:find [e]
          :where [[e :name "Ivan"]]}

      (t/is (= [[[:ivan] 1]] (h/consume-delta! *inc-q*))))))

#_
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

      (t/is (= #{["Ivan" "Ivanov"]}
               (h/consume-delta! *inc-q*))))))
