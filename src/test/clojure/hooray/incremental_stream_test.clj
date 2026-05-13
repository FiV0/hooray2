(ns hooray.incremental-stream-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [hooray.core :as h]
            [hooray.fixtures :as fix]
            [hooray.incremental :as incremental]
            [hooray.incremental.stream :as stream]
            [hooray.zset :as zset])
  (:import (org.hooray.incremental IncrementalPipeline)
           (org.hooray.incremental.stream Circuit)))

(use-fixtures :each fix/with-node fix/with-people-schema)

(deftest compile-incremental-stream-q-projects-find-output
  (testing "stream compiler mirrors current find projection"
    (let [db-before (h/db fix/*node*)
          circuit (stream/compile-incremental-stream-q
                   db-before
                   '{:find [name]
                     :where [[e :name name]]})
          zset-indices (->> {:add [[1 :name "Ivan"]]
                             :retract []}
                            (incremental/calc-zset-indices db-before)
                            incremental/zset-indices-clj->kt)
          delta (-> (.step circuit zset-indices)
                    zset/zset->result-set)]
      (is (= [[["Ivan"] 1]] delta)))))

(deftest circuit-version-dispatch-uses-stream-compiler
  (testing "binding *circuit-version* selects the stream circuit path"
    (let [query '{:find [name]
                  :where [[e :name name]]}
          inc-q (binding [incremental/*circuit-version* :stream]
                  (h/q-inc fix/*node* query))]
      (try
        (h/transact fix/*node* [{:db/id 1 :name "Ivan"}])
        (is (= [[["Ivan"] 1]] (h/consume-delta! inc-q)))
        (finally
          (h/unregister-inc-q fix/*node* inc-q))))))

(deftest default-circuit-version-is-stream
  (let [inc-q (h/q-inc fix/*node* '{:find [name]
                                    :where [[e :name name]]})]
    (try
      (is (instance? Circuit (:pipeline inc-q)))
      (finally
        (h/unregister-inc-q fix/*node* inc-q)))))

(deftest pipeline-circuit-version-remains-available
  (let [inc-q (binding [incremental/*circuit-version* :pipeline]
                (h/q-inc fix/*node* '{:find [name]
                                      :where [[e :name name]]}))]
    (try
      (is (instance? IncrementalPipeline (:pipeline inc-q)))
      (finally
        (h/unregister-inc-q fix/*node* inc-q)))))
