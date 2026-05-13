(ns hooray.incremental.parity-probe-test
  "Smoke probe: does the existing sanity-check query_inc_test scenario
   pass under *circuit-version* :stream as well as :legacy?"
  (:require [clojure.test :as t :refer [deftest is testing]]
            [hooray.core :as h]
            [hooray.fixtures :as fix]
            [hooray.incremental :as inc]))

(t/use-fixtures :each fix/with-node fix/with-people-schema)

(deftest sanity-check-under-stream
  (binding [inc/*circuit-version* :stream]
    (let [inc-q (h/q-inc fix/*node* '{:find [e] :where [[e :name "Ivan"]]})]
      (try
        (h/transact fix/*node* [{:db/id 1 :name "Ivan"}])
        (is (= [[[1] 1]] (h/consume-delta! inc-q)))
        (finally
          (h/unregister-inc-q fix/*node* inc-q))))))

(deftest sanity-check-under-legacy-still-works
  (binding [inc/*circuit-version* :legacy]
    (let [inc-q (h/q-inc fix/*node* '{:find [e] :where [[e :name "Ivan"]]})]
      (try
        (h/transact fix/*node* [{:db/id 1 :name "Ivan"}])
        (is (= [[[1] 1]] (h/consume-delta! inc-q)))
        (finally
          (h/unregister-inc-q fix/*node* inc-q))))))
