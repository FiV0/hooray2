(ns hooray.db
  (:require [clojure.data.avl :as avl]
            [hooray.util.persistent-map :as btree-map]
            [hooray.util :as util]
            [hooray.transact :as t]
            [me.tonsky.persistent-sorted-set :as btree-set])
  (:import (org.hooray UniversalComparator)
           (clojure.data.avl AVLMap)
           (hooray.util.persistent_map PersistentSortedMap)))

(defrecord Db [eav aev ave vae opts schema])

(def ^:private universal-comp (cast java.util.Comparator UniversalComparator/INSTANCE))

(defn set* [type]
  (case type
    :hash (hash-set)
    :avl (avl/sorted-set-by universal-comp)
    :btree (btree-set/sorted-set-by universal-comp)))

(defn map* [type]
  (case type
    :hash (hash-map)
    :avl (avl/sorted-map-by universal-comp)
    :btree (btree-map/sorted-map-by universal-comp)))

(defn ->update-in-fn [type]
  (case type
    :hash (util/create-update-in (hash-map))
    :avl (util/create-update-in (avl/sorted-map-by universal-comp))
    :btree (util/create-update-in (btree-map/sorted-map universal-comp))))

(defn ->db [{:keys [storage] :as opts}]
  (->Db (map* storage) (map* storage) (map* storage) (map* storage) opts {}))

(defn db? [x]
  (instance? Db x))

(defn- map->triples [m]
  (let [eid (or (:db/id m) (throw (ex-info "Entity map must have :db/id" {:entity m})))]
    (->> (dissoc m :db/id)
         (map (fn [[k v]] (vector eid k v))))))

(defn- db->type [db]
  (cond
    (instance? AVLMap (:eav db)) :avl
    (instance? PersistentSortedMap (:eav db)) :btree
    :else :hash))

(defn entity [db eid]
  {:pre [(db? db)]}
  (let [attrs (get (:eav db) eid)]
    (when attrs
      (reduce (fn [ent [attr vals]]
                (assoc ent attr (if (= 1 (count vals))
                                  (first vals)
                                  vals)))
              {:db/id eid}
              attrs))))

;; TODO: Do this with transcients
(defn index-triple-add [{:keys [eav ave vae schema] :as db} [e a v :as _triple]]
  (let [type (db->type db)
        update-in (->update-in-fn type)
        empty-set (set* type)
        cardinality (t/attribute-cardinality schema a)
        previous-v (first (get-in eav [e a]))]
    (case cardinality
      :db.cardinality/one (let [db (if previous-v
                                     (assoc db
                                            :ave (update-in ave [a previous-v] disj e)
                                            :vae (update-in vae [previous-v a] disj e))
                                     db)]
                            (-> db
                                (update-in [:eav e a] (fn [_] (conj empty-set v)))
                                (update-in [:aev a e] (fn [_] (conj empty-set v)))
                                (update-in [:ave a v] (fnil conj empty-set) e)
                                (update-in [:vae v a] (fnil conj empty-set) e)))
      :db.cardinality/many (-> db
                               (update-in [:eav e a] (fnil conj empty-set) v)
                               (update-in [:aev a e] (fnil conj empty-set) v)
                               (update-in [:ave a v] (fnil conj empty-set) e)
                               (update-in [:vae v a] (fnil conj empty-set) e)))))

(defn index-triple-retract [{:keys [eav aev ave vae] :as db} [e a v :as _triple]]
  (let [update-in (->update-in-fn (db->type db))]
    (assoc db
           :eav (update-in eav [e a] disj v)
           :aev (update-in aev [a e] disj v)
           :ave (update-in ave [a v] disj e)
           :vae (update-in vae [v a] disj e))))

(defn tx-datum->triples [db tx-datum]
  (cond
    (map? tx-datum) {:op :add :triples (map->triples tx-datum)}
    (= :db/add (first tx-datum)) {:op :add :triples [(vec (rest tx-datum))]}
    (= :db/retract (first tx-datum)) {:op :retract :triples [(vec (rest tx-datum))]}

    (= :db/retractEntity (first tx-datum))
    (let [eid (second tx-datum)]
      {:op :retract
       :triples (map->triples (entity db eid))})))

(defn tx-data->triples [db tx-data]
  (-> (->> (map (partial tx-datum->triples db) tx-data)
           (group-by :op))
      (update-vals (fn [ops] (mapcat :triples ops)))))

(defn reserved-keyword? [k]
  (and (keyword? k) (= (namespace k) "db")))

(defn check-triple [triple]
  (when (some reserved-keyword? triple)
    (throw (ex-info "Transaction contains reserved keywords" {:triple triple}))))

;; TODO: properly support attribute schema as first class entities and initialize the db with
;; the attribute schema attributes
(defn transact [{:keys [schema] :as db} tx-data]
  {:pre [(db? db)]}
  (let [{:keys [add retract] :as _triples-by-op} (tx-data->triples db tx-data)
        new-schema (cond-> schema
                     (t/schema-tx? tx-data) (t/index-schema tx-data))]
    (when-not (t/schema-tx? tx-data)
      (run! check-triple (concat add retract)))
    (as-> db db
      (reduce index-triple-add db add)
      (reduce index-triple-retract db retract)
      (assoc db :schema new-schema))))
