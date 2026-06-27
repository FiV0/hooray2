(ns hooray.subscription-test
  (:require [clojure.test :as t :refer [deftest is testing]]
            [hooray.core :as h]
            [hooray.fixtures :as fix]))

(t/use-fixtures :each fix/with-each-dbsp-version fix/with-node fix/with-people-schema)

(def names-query
  '{:find [name]
    :where [[e :name name]]})

(deftest incremental-subscription-api-test
  (testing "subscription + take!"
    (with-open [sub (h/subscribe fix/*node* names-query)]
      (h/transact fix/*node* [{:db/id :ivan :name "Ivan"}])
      (is (= [[["Ivan"] 1]] (h/take! sub))))))

(comment
  ;; README examples

  (def node (h/connect {:type :mem :storage :hash :algo :generic}))
  (h/transact node [{:db/id -100
                     :db/ident :name
                     :db/valueType :db.type/string
                     :db/cardinality :db.cardinality/one}])

  (def names-query
    '{:find [name]
      :where [[e :name name]]})

  ;; Subscription + take!
  (with-open [sub (h/subscribe node names-query)]
    (h/transact node [{:db/id :ivan :name "Ivan"}])
    (h/take! sub))
  ;; => ([["Ivan"] 1])

  )
