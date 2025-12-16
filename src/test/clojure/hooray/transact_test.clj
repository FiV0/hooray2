(ns hooray.transact-test
  (:require [clojure.test :as t :refer [deftest is use-fixtures]]
            [hooray.fixtures :as fix]
            [hooray.core :as h]
            [hooray.graph-gen :as g])
  (:import (clojure.lang ExceptionInfo)))

(use-fixtures :each fix/with-node fix/with-people-schema)

(deftest sanity-check
  (h/transact fix/*node* [{:db/id 1 :name "Ivan"}])
  (is (= #{[1]} (h/q '{:find [e]
                       :where [[e :name "Ivan"]]} (h/db fix/*node*)))))

(deftest illegal-attribute-name
  (is (thrown-with-msg?
       ExceptionInfo
       #"Transaction contains reserved keywords"
       (h/transact fix/*node* [{:db/id :db/ivan :name "Ivan"}])
       "reserved keyword in entity position"))
  #_#_
  (is (thrown-with-msg?
       ExceptionInfo
       #"Transaction contains reserved keywords"
       (h/transact fix/*node* [{:db/id 1 :db/name "Ivan"}])
       "reserved keyword in attribute position"))

  (is (thrown-with-msg?
       ExceptionInfo
       #"Transaction contains reserved keywords"
       (h/transact fix/*node* [{:db/id 1 :name :db/ivan}])
       "reserved keyword in attribute position")))

(deftest schema-tx
  (h/transact fix/*node* [g/edge-attribute])

  (t/is (= {:db/id :db/edge-attribute,
            :db/ident :g/to,
            :db/valueType :db.type/long,
            :db/cardinality :db.cardinality/many}
           (-> (h/db fix/*node*) :schema :g/to))))

(deftest cardinality-one-and-many
  (h/transact fix/*node* [{:db/id 1 :name "Ivan"}])
  (h/transact fix/*node* [{:db/id 1 :name "Petr"}])
  (is (= #{[1 "Petr"]} (h/q '{:find [e name]
                              :where [[e :name name]]} (h/db fix/*node*))))

  (h/transact fix/*node* [{:db/id :db/edge-attribute
                           :db/ident :g/to
                           :db/valueType :db.type/long
                           :db/cardinality :db.cardinality/many}])
  (h/transact fix/*node* [[:db/add 1 :g/to 2]
                          [:db/add 1 :g/to 3]])

  (is (= #{[1 2] [1 3]} (h/q '{:find [e to]
                               :where [[e :g/to to]]} (h/db fix/*node*)))))

(deftest test-update-name
  (h/transact fix/*node* [{:db/id 1 :name "Ivan"}])
  (is (= #{[1]} (h/q '{:find [e]
                       :where [[e :name "Ivan"]]} (h/db fix/*node*))))

  (h/transact fix/*node* [[:db/add 1 :name "Petr"]])
  (is (= #{[1]} (h/q '{:find [e]
                       :where [[e :name "Petr"]]} (h/db fix/*node*))))

  (is (= #{} (h/q '{:find [e]
                    :where [[e :name "Ivan"]]} (h/db fix/*node*)))))

(deftest test-retract
  (h/transact fix/*node* [{:db/id 1 :name "Ivan"}])
  (is (= #{[1]} (h/q '{:find [e]
                       :where [[e :name "Ivan"]]} (h/db fix/*node*))))

  (h/transact fix/*node* [[:db/retract 1 :name "Ivan"]])
  (is (= #{} (h/q '{:find [e]
                    :where [[e :name "Ivan"]]} (h/db fix/*node*))))

  (h/transact fix/*node* [{:db/id :db/edge-attribute
                           :db/ident :g/to
                           :db/valueType :db.type/long
                           :db/cardinality :db.cardinality/many}])
  (h/transact fix/*node* [[:db/add 2 :g/to 3]
                          [:db/add 2 :g/to 4]
                          [:db/add 2 :g/to 5]])

  (is (= #{[2 3] [2 4] [2 5]} (h/q '{:find [e to]
                                     :where [[e :g/to to]]} (h/db fix/*node*))))

  (h/transact fix/*node* [[:db/retract 2 :g/to 4]])
  (is (= #{[2 3] [2 5]} (h/q '{:find [e to]
                               :where [[e :g/to to]]} (h/db fix/*node*)))))

(deftest test-retract-entity
  (h/transact fix/*node* [{:db/id 1 :name "Ivan"}])
  (is (= #{[1]} (h/q '{:find [e]
                       :where [[e :name "Ivan"]]} (h/db fix/*node*))))

  (h/transact fix/*node* [[:db/retractEntity 1]])

  (is (= #{} (h/q '{:find [e]
                    :where [[e :name "Ivan"]]} (h/db fix/*node*)))))
