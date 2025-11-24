(ns hooray.transact-test
  (:require [clojure.test :as t :refer [deftest is use-fixtures]]
            [hooray.transact :as tr]
            [hooray.fixtures :as fix]
            [hooray.core :as h])
  (:import (clojure.lang ExceptionInfo)))

(use-fixtures :each fix/with-node)

(deftest test-sanity-check
  (h/transact fix/*node* [{:db/id 1 :name "Ivan"}])
  (is (= #{[1]} (h/q '{:find [e]
                       :where [[e :name "Ivan"]]} (h/db fix/*node*)))))

(deftest test-illegal-attribute-name
  (is (thrown-with-msg?
       ExceptionInfo
       #"Transaction contains reserved keywords"
       (h/transact fix/*node* [{:db/id :db/ivan :name "Ivan"}])
       "reserved keyword in entity position"))

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

(deftest test-schema-tx
  (h/transact fix/*node* [{:db/id :db/edge-attribute
                           :db/ident :g/to
                           :db/cardinality :db.cardinality/many}])

  (t/is (= {:g/to
            {:db/cardinality :db.cardinality/many,
             :db/id :db/edge-attribute,
             :db/ident :g/to}}
           @tr/schema)))

#_
(deftest test-update-name
  (h/transact fix/*node* [{:db/id 1 :name "Ivan"}])
  (is (= #{[1]} (h/q '{:find [e]
                       :where [[e :name "Ivan"]]} (h/db fix/*node*))))

  (h/transact fix/*node* [[:db/add 1 :name "Petr"]])
  (is (= #{[1]} (h/q '{:find [e]
                       :where [[e :name "Petr"]]} (h/db fix/*node*))))

  (is (= #{} (h/q '{:find [e]
                    :where [[e :name "Ivan"]]} (h/db fix/*node*)))))
