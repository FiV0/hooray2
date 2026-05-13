(ns hooray.incremental.stream
  "Stream-pipeline entry point for incremental queries.

   Mirrors `hooray.incremental/compile-incremental-q` but builds a
   `org.hooray.incremental.stream.Circuit` over the new circuit model
   instead of an `IncrementalPipeline`."
  (:require [clojure.spec.alpha :as s]
            [hooray.error :as err]
            [hooray.incremental :as inc]
            [hooray.query :as query]
            [hooray.zset :as zset])
  (:import (org.hooray.incremental.stream Circuit CircuitSpec CircuitTransform
                                          IncrementalWcojJoinSpec)
           (org.hooray.incremental.stream IncrementalWcojJoinSpecKt)))

(set! *warn-on-reflection* true)

(defn- compile-find-transform ^CircuitTransform [conformed-find var->index]
  (let [find-syms (mapv (fn [[find-type find-arg]]
                          (case find-type
                            :variable find-arg
                            :aggregate (throw (err/unsupported-ex
                                                "Aggregates not yet supported in incremental queries!"))))
                        conformed-find)
        order-fn (inc/order-result-fn find-syms var->index)]
    (reify CircuitTransform
      (eval [_ input-zset] (zset/project input-zset order-fn))
      (commit [_]))))

(defn compile-incremental-stream-q ^Circuit [db query]
  {:pre [(s/valid? ::query/query query)
         (query/validate-query (s/conform ::query/query query))]}
  (let [{:keys [find keys strs syms in where]} (s/conform ::query/query query)
        var-order (query/variable-order* where)
        var->index (zipmap var-order (range))
        compiled-patterns (mapv (partial inc/compile-inc-pattern var-order) where)
        levels (count var-order)]
    (when (seq in)
      (throw (ex-info "IN clauses not supported for incremental queries yet"
                      {:in in})))
    (when (or (seq keys) (seq strs) (seq syms))
      (throw (ex-info "KEYS, STRS, and SYMS not supported for incremental queries yet"
                      {:keys keys :strs strs :syms syms})))
    (let [canonical-order (mapv int (range levels))
          wcoj-spec (IncrementalWcojJoinSpec. compiled-patterns levels canonical-order)
          source (IncrementalWcojJoinSpecKt/buildWcojSource wcoj-spec)
          find-transform (compile-find-transform find var->index)
          circuit-spec (CircuitSpec. source [find-transform])
          circuit (Circuit. circuit-spec)
          initial-indices (inc/zset-indices-clj->kt (inc/db->zset-indices db))]
      ;; Seed the circuit from the current db state, matching compile-incremental-q.
      (.step circuit initial-indices)
      circuit)))
