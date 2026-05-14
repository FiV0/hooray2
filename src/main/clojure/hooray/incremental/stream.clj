(ns hooray.incremental.stream
  (:require [clojure.spec.alpha :as s]
            [hooray.error :as err]
            [hooray.incremental :as incremental]
            [hooray.query :as query])
  (:import (org.hooray.incremental.stream Circuit CircuitSpec IncrementalWcojJoinSpec InputHandle ProjectSpec)))

(set! *warn-on-reflection* true)

(defn- project-spec ^ProjectSpec [conformed-find var->idx]
  (let [output-levels (mapv (fn [[find-type find-arg]]
                              (case find-type
                                :variable (int (get var->idx find-arg))
                                :aggregate (throw (err/unsupported-ex "Aggregates not yet supported in incremental queries!"))))
                            conformed-find)]
    (ProjectSpec. output-levels)))

(defn compile-incremental-q ^Circuit [db query]
  {:pre [(s/valid? ::query/query query) (query/validate-query (s/conform ::query/query query))]}
  (let [{:keys [find keys strs syms in where] :as _conformed-query} (s/conform ::query/query query)
        var-order (query/variable-order* where)
        var->idx (zipmap var-order (range))
        compiled-patterns (mapv (partial incremental/compile-inc-pattern var-order) where)]
    (when (seq in)
      (throw (ex-info "IN clauses not supported for incremental queries yet" {:in in})))
    (when (or (seq keys) (seq strs) (seq syms))
      (throw (ex-info "KEYS, STRS, and SYMS not supported for incremental queries yet" {:keys keys :strs strs :syms syms})))
    (let [input (InputHandle.)
          source (IncrementalWcojJoinSpec. compiled-patterns (int (count var-order)) (mapv int (range (count var-order))))
          circuit (Circuit. (CircuitSpec. input source [(project-spec find var->idx)]))
          zset-indices (incremental/zset-indices-clj->kt (incremental/db->zset-indices db))]
      (.step circuit zset-indices)
      circuit)))
