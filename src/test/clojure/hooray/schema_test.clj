(ns hooray.schema-test
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

#_:clj-kondo/ignore
(def marital-attr #{:single :married :unknown})

(deftest schema-errors-test
  (t/is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"Attribute schema contains reserved keywords"
         (h/transact *node* [{:db/id :db/edge-attribute
                              :db/ident :db/to
                              :db/valueType :db.type/long
                              :db/cardinality :db.cardinality/many}]))
        "forbidden attribute names")

  (t/is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"Attribute is not defined in schema"
         (h/transact *node* [{:db/id 1 :unknown-attr "Hello"}]))
        "Unknown attribute")

  (h/transact *node* [{:db/id :db/marital-status
                       :db/ident :marital-status
                       :db/valueType :db.type/keyword
                       :db/cardinality :db.cardinality/one
                       :db.attr/preds 'hooray.schema-test/marital-attr}])

  (t/is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"Value married does not conform to type :db.type/keyword for attribute :marital-status"
         (h/transact *node* [[:db/add 1 :marital-status "married"]]))
        "type checking works")

  (t/is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"Value :invalid does not satisfy predicate hooray.schema-test/marital-attr for attribute :marital-status"
         (h/transact *node* [[:db/add 1 :marital-status :invalid]]))
        "db.attr/preds validation works"))
