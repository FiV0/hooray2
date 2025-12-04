(ns hooray.query-inc-test
  (:require [clojure.test :as t :refer [deftest is testing]]
            [clojure.spec.alpha :as s]
            [hooray.query :as query]
            [hooray.fixtures :as fix]
            [hooray.core :as h])
  (:import (clojure.lang ExceptionInfo)))

(t/use-fixtures :each fix/with-node)

#_
(defn with-inc-q [q]
  (let [inc-q (h/q-inc fix/*node* q)]
    (try
      (fn [f] (f inc-q))
      (finally
        (h/unregister-inc-q fix/*node* inc-q))))

  )

#_
(deftest test-sanity-check
  (let [inc-q (h/q-inc fix/*node* '{:find [e]
                                    :where [[e :name "Ivan"]]})]

    (h/transact fix/*node* [{:db/id 1 :name "Ivan"}])

    (is (= [[:db/add 1]] (h/consume-delta! inc-q)))

    (h/unregister-inc-q fix/*node* inc-q)

    (t/is (= #{[1]} (h/q '{:find [e]
                           :where [[e :name "Ivan"]]} (h/db fix/*node*))))))
