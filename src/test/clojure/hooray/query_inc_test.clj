(ns hooray.query-inc-test
  (:require [clojure.test :as t :refer [deftest is testing]]
            [hooray.fixtures :as fix]
            [hooray.core :as h]
            [hooray.graph-gen :as g]
            [hooray.incremental :as incremental]))

(defn with-circuit-version [f]
  (doseq [version [:pipeline :stream]]
    (testing (str "circuit version " version)
      (binding [incremental/*circuit-version* version]
        (fix/with-node
          (fn []
            (fix/with-people-schema f)))))))

(t/use-fixtures :each with-circuit-version)

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

(deftest test-dbsp-distinct-semantics-retractions
  (h/transact fix/*node* [{:db/id :alice :name "Alice" :city "NYC"}
                          {:db/id :bob :name "Bob" :city "NYC"}
                          {:db/id :carol :name "Carol" :city "LA"}])

  (with-transaction-and-inc-q
    [[:db/retractEntity :bob]]

    '{:find [city]
      :where [[e :city city]]}

    (t/is (= [[["NYC"] -1]] (h/consume-delta! *inc-q*)))
    (h/transact fix/*node* [[:db/retractEntity :alice]])
    (t/is (= [[["NYC"] -1]] (h/consume-delta! *inc-q*)))))

(deftest test-prefix-stable-extension-addition
  (h/transact fix/*node* g/triangle-relation-schema)
  (h/transact fix/*node*
              [[:db/add 1 :r/to 2]
               [:db/add 2 :s/to 4]])

  (with-inc-q '{:find  [a b c]
                :where [[a :r/to b]
                        [b :s/to c]]}
    (h/transact fix/*node* [[:db/add 2 :s/to 5]])
    (t/is (= #{[[1 2 5] 1]}
             (set (h/consume-delta! *inc-q*))))))

(deftest test-prefix-stable-extension-retraction
  (h/transact fix/*node* g/triangle-relation-schema)
  (h/transact fix/*node*
              [[:db/add 1 :r/to 2]
               [:db/add 2 :s/to 4]
               [:db/add 2 :s/to 5]])

  (with-inc-q '{:find  [a b c]
                :where [[a :r/to b]
                        [b :s/to c]]}
    (h/transact fix/*node* [[:db/retract 2 :s/to 5]])
    (t/is (= #{[[1 2 5] -1]}
             (set (h/consume-delta! *inc-q*))))))

(deftest test-triangle-edge-deletion
  (h/transact fix/*node* g/triangle-relation-schema)
  (h/transact fix/*node*
              [[:db/add 1 :r/to 2]
               [:db/add 2 :s/to 3]
               [:db/add 3 :t/to 1]])

  (with-transaction-and-inc-q
    [[:db/retract 2 :s/to 3]]

    '{:find  [a b c]
      :where [[a :r/to b]
              [b :s/to c]
              [c :t/to a]]}

    (t/is (= [[[1 2 3] -1]] (h/consume-delta! *inc-q*)))))

;; TODO move this to some benchmarking setup
(deftest test-triangle-wcoj-bad-case
  ;; Even though R and S each have O(n) tuples on the
  ;; join variable y, they share no value (R's y is odd, S's y is even),
  ;; so inserting T(1,1) produces no triangles and the delta is empty.
  ;; R = {(1,1) (1,3) (1,5)}
  ;; S = {(2,1) (4,1) (6,1)}
  ;; T = {} -> {(1,1)}
  (h/transact fix/*node* g/triangle-relation-schema)
  (h/transact fix/*node* (g/wcoj-ivm-bad-relations 3 #_10000))

  (with-transaction-and-inc-q
      [[:db/add 1 :t/to 1]]

      '{:find  [a b c]
        :where [[a :r/to b]
                [b :s/to c]
                [c :t/to a]]}

    (t/is (= [] (h/consume-delta! *inc-q*)))))
