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

(deftest in-tx-transfer-test
  (with-conn
    (fn []
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
                        [:db/retract (get tempids "carol") :user/primary-friend (get tempids "bob")]]))))))

(deftest tempid-only-in-value-pos
  (with-conn
    (fn []
      (transact! [{:db/ident       :user/name
                   :db/valueType   :db.type/string
                   :db/cardinality :db.cardinality/one
                   :db/unique      :db.unique/identity}
                  {:db/ident       :user/follows
                   :db/valueType   :db.type/ref
                   :db/cardinality :db.cardinality/many}])

      (is (thrown? Exception
                   (transact! [[:db/add "alice" :user/name "alice"]
                               [:db/add "alice" :user/follows "bob"]]))))))
