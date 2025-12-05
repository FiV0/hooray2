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

(deftest test-sanity-check
  (with-inc-q '{:find [e]
                :where [[e :name "Ivan"]]}

    (h/transact fix/*node* [{:db/id 1 :name "Ivan"}])

    (is (= [[[1] 1]] (h/consume-delta! *inc-q*)))))
