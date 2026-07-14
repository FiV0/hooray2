(ns datomic-semantics
  (:require [clojure.test :as t :refer [deftest is testing]]
            [datomic.api :as d]))

(def ^:dynamic *conn* nil)

(defn with-conn [f]
  (let [uri (str "datomic:mem://test-" (random-uuid))]
    (d/create-database uri)
    (let [conn (d/connect uri)]
      (binding [*conn* conn]
        (try
          (f)
          (finally
            (d/delete-database uri)))))))

(defn transact! [tx-data]
  @(d/transact *conn* tx-data))

(t/use-fixtures :each with-conn)

(deftest in-tx-transfer-test
  (transact! [{:db/ident       :user/email
               :db/valueType   :db.type/string
               :db/cardinality :db.cardinality/one
               :db/unique      :db.unique/identity}
              {:db/ident       :user/handle
               :db/valueType   :db.type/string
               :db/cardinality :db.cardinality/one
               :db/unique      :db.unique/identity}
              {:db/ident       :user/primary-friend
               :db/valueType   :db.type/ref
               :db/cardinality :db.cardinality/one
               :db/unique      :db.unique/identity}])

  (let [{:keys [tempids]} (transact! [{:db/id "alice"   :user/email          "alice"}
                                      {:db/id "bob"     :user/handle         "bob"}
                                      {:db/id "carol"   :user/primary-friend "bob"}])]

    ;; We upsert on :user/email and at the same time update the unique reference
    ;; :user/primary-friend to "bob". This works because we retract the previous
    ;; :user/primary-friend datom ["carol" :user/primary-friend "bob"]
    (is (transact! [[:db/add     "u" :user/email          "alice"]
                    [:db/add     "u" :user/primary-friend "f"]
                    [:db/add     "f" :user/handle         "bob"]
                    [:db/retract (get tempids "carol") :user/primary-friend (get tempids "bob")]]))))

(deftest tempid-only-in-value-pos
  (transact! [{:db/ident       :user/name
               :db/valueType   :db.type/string
               :db/cardinality :db.cardinality/one
               :db/unique      :db.unique/identity}
              {:db/ident       :user/follows
               :db/valueType   :db.type/ref
               :db/cardinality :db.cardinality/many}])

  (is (thrown? Exception
               (transact! [[:db/add "alice" :user/name "alice"]
                           [:db/add "alice" :user/follows "bob"]]))))

(def ^:private people-schema
  [{:db/ident :name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :age
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one}])

(deftest or-bound-mentioned-example
  (transact! people-schema)

  (transact! [{:name "Ivan" :age 25}
              {:name "Bob" :age 35}])

  (t/is (thrown? Exception
                 (d/q '{:find [?e ?name]
                        :where [[?e :name ?name]
                                (or (and [?e :name "Bob"]
                                         [?e :age ?age])
                                    (and [?e :name "Ivan"]
                                         [(< ?age 30)]))]}
                      (d/db *conn*)))))


(def ^:private people-schema2
  [{:db/ident :name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :city
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :last-name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}])

(deftest cyclic-or-dependency-not-grounded-throws
  (transact! people-schema2)

  (transact! [{:name "left" :last-name "seed"}
              {:name "right"}])

  (t/is (thrown? Exception
                 (d/q '{:find [?x ?y]
                        :where [(or
                                 (and
                                  (or (and [?y :name "right"]
                                           (not [?x :city "blocked"])))
                                  (or (and [?x :name "left"]
                                           (not [?y :city "blocked"])))))
                                [?x :last-name "seed"]]}
                      (d/db *conn*)))))

#_
(deftest test-fn-clause-before-input-binding
  (transact! people-schema)
  (transact! [{:db/id "ivan" :name "Ivan" :age 30}])
  (t/is (= [[31]] (d/q '{:find [y]
                         :where [[(inc age) y]
                                 [e :age age]]}
                       (d/db *conn*)))))

(comment
  (t/run-all-tests)
  (t/run-test-var #'cyclic-or-dependency-not-grounded-throws)

  )
