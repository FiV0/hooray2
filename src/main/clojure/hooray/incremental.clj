(ns hooray.incremental
  (:require
   [clojure.spec.alpha :as s]
   [hooray.query :as query]
   [hooray.transact :as t]
   [hooray.util :as util]
   [hooray.zset :as zset]
   [hooray.db :as db])
  (:import
   (org.hooray.incremental IntegerWeight IncrementalJoin IncrementalGenericJoin ZSetIndices)
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
            (assoc :eav (zset-update-in eav-zset [e a] (fnil update zset/empty-zset) v (fnil zset/subtraction zero) one))
            (assoc :aev (zset-update-in aev-zset [a e] (fnil update zset/empty-zset) v (fnil zset/subtraction zero) one))
            (assoc :ave (zset-update-in ave-zset [a v] (fnil update zset/empty-zset) e (fnil zset/subtraction zero) one))
            (assoc :vae (zset-update-in vae-zset [v a] (fnil update zset/empty-zset) e (fnil zset/subtraction zero) one)))
        zset-indices)
      [:add :db.cardinality/one]
      (let [previous-v (first (get-in eav [e a]))
            zset-indices (if previous-v
                           (index-triple db zset-indices [:retract e a previous-v])
                           zset-indices)]
        (-> zset-indices
            (zset-update-in [:eav e a] (fnil update zset/empty-zset) v (fnil zset/addition zero) one)
            (zset-update-in [:aev a e] (fnil update zset/empty-zset) v (fnil zset/addition zero) one)
            (zset-update-in [:ave a v] (fnil update zset/empty-zset) e (fnil zset/addition zero) one)
            (zset-update-in [:vae v a] (fnil update zset/empty-zset) e (fnil zset/addition zero) one)))
      [:add :db.cardinality/many]
      (if (-> (get-in eav [e a]) (contains? v))
        zset-indices
        (-> zset-indices
            (zset-update-in [:eav e a] (fnil update zset/empty-zset) v (fnil zset/addition zero) one)
            (zset-update-in [:aev a e] (fnil update zset/empty-zset) v (fnil zset/addition zero) one)
            (zset-update-in [:ave a v] (fnil update zset/empty-zset) e (fnil zset/addition zero) one)
            (zset-update-in [:vae v a] (fnil update zset/empty-zset) e (fnil zset/addition zero) one))))))

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

(defn compile-inc-pattern [& args]
  (throw (ex-info "Not implemented yet" {:args args})))

(defn compiled-find [order-fn compiled-inner]
  (reify IncrementalJoin
    (join [_ zset-indices]
      (let [inner-results (.join compiled-inner zset-indices)]
        (update-keys inner-results order-fn)))))

(defn compile-query [compiled-patterns levels]
  (IncrementalGenericJoin. compiled-patterns levels))

;; TODO unify this somehow with query/query
(defn compile-incremental-q ^IncrementalGenericJoin [db query]
  {:pre [(s/valid? ::query query) (query/validate-query (s/conform ::query query))]}
  (let [zset-indices (zset-indices-clj->kt (db->zset-indices db))
        {:keys [find keys strs syms in where] :as _conformed-query} (s/conform ::query/query query)
        var-order (query/variable-order* where)
        var-to-index (zipmap var-order (range))
        compiled-patterns (map (partial compile-inc-pattern zset-indices var-order) where)
        order-fn (query/order-result-fn find var-to-index)]
    (when (seq in)
      (throw (ex-info "IN clauses not supported for incremental queries yet" {:in in})))
    (when (or (seq keys) (seq strs) (seq syms))
      (throw (ex-info "KEYS, STRS, and SYMS not supported for incremental queries yet" {:keys keys :strs strs :syms syms})))
    (compile-query compiled-patterns (count var-order))))

(defn compute-delta! [{:keys [^IncrementalGenericJoin compiled-q !queue] :as _inc-q} db-before _db-after tx-data]
  (let [triples-by-op (db/tx-data->triples tx-data)
        zset-indices (zset-indices-clj->kt (calc-zset-indices db-before triples-by-op))
        delta (-> (.join compiled-q zset-indices)
                  zset/indexed-zset->result-set)]
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
