(ns hooray.incremental-stream-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [hooray.core :as h]
            [hooray.fixtures :as fix]
            [hooray.incremental :as incremental]
            [hooray.incremental.stream :as stream]
            [hooray.zset :as zset]))

(use-fixtures :each fix/with-node fix/with-people-schema)

(deftest compile-incremental-stream-q-projects-find-output
  (testing "stream compiler mirrors current find projection"
    (let [db-before (h/db fix/*node*)
          circuit (stream/compile-incremental-q
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
