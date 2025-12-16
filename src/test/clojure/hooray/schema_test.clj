(ns schema-test
  (:require [clojure.test :as t :refer [deftest]]
            [hooray.fixtures :as fix :refer [*node*]]
            [hooray.core :as h]))

(t/use-fixtures :each fix/with-node)

(deftest query-initial-schema
  (t/is (= #{[:db/ident]
             [:db/index]
             [:db/cardinality]
             [:db/unique]
             [:db.attr/preds]
             [:db/valueType]
             [:db/doc]}
           (h/q '{:find [?ident]
                  :where [[?e :db/ident ?ident]]}
                (h/db *node*)))))

(deftest forbidden-test
  (t/testing "forbidden attribute names"
    (t/is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"Attribute schema contains reserved keywords"
           (h/transact *node* [{:db/id :db/edge-attribute
                                :db/ident :db/to
                                :db/valueType :db.type/long
                                :db/cardinality :db.cardinality/many}])))))
