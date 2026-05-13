(ns hooray.incremental.stream-test
  (:require [clojure.test :as t :refer [deftest is testing]]
            [hooray.core :as h]
            [hooray.fixtures :as fix]
            [hooray.incremental :as inc]
            [hooray.incremental.stream :as inc-stream])
  (:import (org.hooray.incremental.stream Circuit)))

(t/use-fixtures :each fix/with-node fix/with-people-schema)

(defn- empty-indices ^org.hooray.incremental.ZSetIndices []
  (inc/zset-indices-clj->kt (inc/->zset-indices)))

(deftest compile-incremental-stream-q-returns-circuit
  (let [circuit (inc-stream/compile-incremental-stream-q
                  (h/db fix/*node*)
                  '{:find [e] :where [[e :name "Ivan"]]})]
    (is (instance? Circuit circuit))))

(deftest stream-circuit-smoke-step-on-empty-delta
  (let [circuit (inc-stream/compile-incremental-stream-q
                  (h/db fix/*node*)
                  '{:find [e] :where [[e :name "Ivan"]]})
        result (.step circuit (empty-indices))]
    (is (some? result))))

(deftest stream-circuit-handles-multi-pattern-query
  (let [circuit (inc-stream/compile-incremental-stream-q
                  (h/db fix/*node*)
                  '{:find [name last-name]
                    :where [[e :name name]
                            [e :last-name last-name]]})]
    (is (instance? Circuit circuit))))

(deftest stream-circuit-rejects-in-clauses
  (is (thrown? clojure.lang.ExceptionInfo
        (inc-stream/compile-incremental-stream-q
          (h/db fix/*node*)
          '{:find [e] :in [?n] :where [[e :name ?n]]}))))
