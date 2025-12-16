(ns hooray.api-test
  (:require [clojure.test :as t :refer [deftest is use-fixtures]]
            [hooray.fixtures :as fix]
            [hooray.core :as h]))

(use-fixtures :each fix/with-node fix/with-people-schema)

(deftest test-get-entity
  (h/transact fix/*node* [{:db/id 1 :name "Ivan" :age 30}])

  (is (= {:db/id 1 :name "Ivan" :age 30}
         (h/entity (h/db fix/*node*) 1)))

  (is (= nil
         (h/entity (h/db fix/*node*) 2))))
