(ns hooray.incremental
  (:require
   [clojure.core.match :refer [match]]
   [clojure.spec.alpha :as s]
   [hooray.query :as query]
   [hooray.transact :as t]
   [hooray.util :as util]
   [hooray.zset :as zset]
   [hooray.db :as db]
   [hooray.error :as err])
  (:import
   (org.hooray.incremental IntegerWeight IncrementalJoin IncrementalGenericJoin ZSetIndices IndexType)
   (org.hooray.incremental.iterator GenericIncrementalIndex)))

(def zero IntegerWeight/ZERO)
(def one IntegerWeight/ONE)

(defrecord ZSetIndicesClj [eav aev ave vae])

(defn ->zset-indices []
  (->ZSetIndicesClj zset/empty-indexed-zset zset/empty-indexed-zset zset/empty-indexed-zset zset/empty-indexed-zset))

(defn zset-indices-clj->kt ^ZSetIndices [{:keys [eav aev ave vae] :as _zset-indices}]
  (ZSetIndices. eav aev ave vae))

(def ^:private zset-update-in (util/create-update-in zset/empty-indexed-zset))

(defn index-triple [{:keys [eav schema] :as db}
                    {eav-zset :eav aev-zset :aev
                     ave-zset :ave vae-zset :vae :as zset-indices}
                    [op e a v :as _triple]]
  (let [cardinality (t/attribute-cardinality schema a)]
    (case [op cardinality]
      ([:retract :db.cardinality/one] [:retract :db.cardinality/many])
      (if (-> (get-in eav [e a]) (contains? v))
        ;; TODO this might not clean up empty nested structures in the indexed zsets
        (-> zset-indices
            (assoc :eav (zset-update-in eav-zset [e a] (fnil update zset/empty-zset) v (fnil zset/sub zero) one))
            (assoc :aev (zset-update-in aev-zset [a e] (fnil update zset/empty-zset) v (fnil zset/sub zero) one))
            (assoc :ave (zset-update-in ave-zset [a v] (fnil update zset/empty-zset) e (fnil zset/sub zero) one))
            (assoc :vae (zset-update-in vae-zset [v a] (fnil update zset/empty-zset) e (fnil zset/sub zero) one)))
        zset-indices)
      [:add :db.cardinality/one]
      (let [previous-v (first (get-in eav [e a]))
            zset-indices (if previous-v
                           (index-triple db zset-indices [:retract e a previous-v])
                           zset-indices)]
        (-> zset-indices
            (zset-update-in [:eav e a] (fnil update zset/empty-zset) v (fnil zset/add zero) one)
            (zset-update-in [:aev a e] (fnil update zset/empty-zset) v (fnil zset/add zero) one)
            (zset-update-in [:ave a v] (fnil update zset/empty-zset) e (fnil zset/add zero) one)
            (zset-update-in [:vae v a] (fnil update zset/empty-zset) e (fnil zset/add zero) one)))
      [:add :db.cardinality/many]
      (if (-> (get-in eav [e a]) (contains? v))
        zset-indices
        (-> zset-indices
            (zset-update-in [:eav e a] (fnil update zset/empty-zset) v (fnil zset/add zero) one)
            (zset-update-in [:aev a e] (fnil update zset/empty-zset) v (fnil zset/add zero) one)
            (zset-update-in [:ave a v] (fnil update zset/empty-zset) e (fnil zset/add zero) one)
            (zset-update-in [:vae v a] (fnil update zset/empty-zset) e (fnil zset/add zero) one))))))

(defn db->zset-indices [{:keys [eav opts] :as _db}]
  (let [empty-db (db/->db opts)
        triples (for [e (keys eav)
                      a (keys (get eav e))
                      v (get-in eav [e a])]
                  [:add e a v])]
    (reduce (fn [zset-indices triple]
              (index-triple empty-db zset-indices triple))
            (->zset-indices)
            triples)))

(defn calc-zset-indices [db-before {:keys [add retract] :as _triples-by-op}]
  (let [triples (concat (map (fn [t] (into [:add] t)) add)
                        (map (fn [t] (into [:retract] t)) retract))]
    (reduce (fn [zset-indices triple]
              (index-triple db-before zset-indices triple))
            (->zset-indices)
            triples)))

(defn compile-inc-pattern [{:keys [eav ave aev] :as _zset-indices} var-order [type pattern :as where-clause]]
  (let [var-to-index (zipmap var-order (range))]
    (case type
      :triple (let [{:keys [e a v]} pattern]
                (match [e a v]
                  [[:constant _] [:constant _] [:constant _]]
                  (err/unsupported-ex)

                  [[:constant e-const] [:constant a-const] [:variable v-var]]
                  (GenericIncrementalIndex. IndexType/EAV [e-const a-const]
                                            (get-in eav [e-const a-const] zset/empty-zset)
                                            [(get var-to-index v-var)])

                  [[:variable e-var] [:constant a-const] [:constant v-const]]
                  (GenericIncrementalIndex. IndexType/AVE [a-const v-const]
                                            (get-in ave [a-const v-const] zset/empty-zset)
                                            [(get var-to-index e-var)])

                  [[:variable e-var] [:constant a-const] [:variable v-var]]
                  (let [e-index (var-to-index e-var)
                        v-index (var-to-index v-var)
                        [index-type index participates-in-level] (if (< e-index v-index)
                                                                   [IndexType/AEV aev [e-index v-index]]
                                                                   [IndexType/AVE ave [v-index e-index]])]
                    (GenericIncrementalIndex. index-type [a-const]
                                              (get index a-const zset/empty-indexed-zset)
                                              participates-in-level))

                  :else (throw (ex-info "Unknown triple clause" {:triple pattern}))))
      (throw (ex-info "Unknown or not yet supported where clause type" {:where-clause where-clause})))))

(defn order-result-fn [find-syms var->index]
  (fn [row]
    (mapv (fn [var] (nth row (var->index var))) find-syms)))

(defn compile-find [conformed-find var->idx ^IncrementalJoin compiled-inner]
  (let [find-syms (mapv (fn [[find-type find-arg]]
                          (case find-type
                            :variable find-arg
                            :aggregate (throw (err/unsupported-ex "Aggregates not yet supported in incremental queries!"))))
                        conformed-find)
        order-fn (order-result-fn find-syms var->idx)]
    (reify IncrementalJoin
      (join [_ zset-indices]
        (let [inner-results (.join compiled-inner zset-indices)]
          (zset/update-keys inner-results order-fn))))))

(defn compile-query [conformed-find var->idx compiled-patterns levels]
  (->> (IncrementalGenericJoin. compiled-patterns levels)
       (compile-find conformed-find var->idx)))

;; TODO unify this somehow with query/query
(defn compile-incremental-q ^IncrementalJoin [db query]
  {:pre [(s/valid? ::query/query query) (query/validate-query (s/conform ::query/query query))]}
  (let [zset-indices (zset-indices-clj->kt (db->zset-indices db))
        {:keys [find keys strs syms in where] :as _conformed-query} (s/conform ::query/query query)
        var-order (query/variable-order* where)
        var->index (zipmap var-order (range))
        compiled-patterns (map (partial compile-inc-pattern zset-indices var-order) where)]
    (when (seq in)
      (throw (ex-info "IN clauses not supported for incremental queries yet" {:in in})))
    (when (or (seq keys) (seq strs) (seq syms))
      (throw (ex-info "KEYS, STRS, and SYMS not supported for incremental queries yet" {:keys keys :strs strs :syms syms})))
    (compile-query find var->index compiled-patterns (count var-order))))

(defn compute-delta! [{:keys [^IncrementalJoin compiled-q !queue] :as _inc-q} db-before _db-after tx-data]
  (let [triples-by-op (db/tx-data->triples db-before tx-data)
        zset-indices (zset-indices-clj->kt (calc-zset-indices db-before triples-by-op))
        delta (-> (.join compiled-q zset-indices)
                  zset/zset->result-set)]
    (swap! !queue conj delta)))

(defrecord IncrementalQuery [id query compiled-q !queue])

(defn ->incremental-query [query compiled-q]
  (->IncrementalQuery (random-uuid) query compiled-q (atom clojure.lang.PersistentQueue/EMPTY)))

(defn query [initial-db query & args]
  {:pre [(s/valid? ::query/query query) (query/validate-query (s/conform ::query/query query))]}
  (when (seq args)
    (throw (ex-info "Positional arguments not supported for incremental queries yet" {:args args})))
  (->incremental-query query (compile-incremental-q initial-db query)))

(defn pop-result! [{:keys [!queue] :as _inc-q}]
  (let [res (peek @!queue)]
    (swap! !queue pop)
    res))
