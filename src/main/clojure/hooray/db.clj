(ns hooray.db
  (:require [clojure.data.avl :as avl]
            [hooray.util.persistent-map :as btree-map]
            [hooray.util :as util]
            [me.tonsky.persistent-sorted-set :as btree-set])
  (:import (org.hooray UniversalComparator)
           (clojure.data.avl AVLMap)
           (hooray.util.persistent_map PersistentSortedMap)))

(defrecord Db [eav aev ave vae])

(defn set* [type]
  (case type
    :hash-map (hash-set)
    :avl (avl/sorted-set-by UniversalComparator)
    :btree (btree-set/sorted-set-by UniversalComparator)))

(defn map* [type]
  (case type
    :hash-map (hash-map)
    :avl (avl/sorted-map-by UniversalComparator)
    :btree (btree-map/sorted-map-by UniversalComparator)))

(defn ->update-in-fn [type]
  (case type
    :hash-map (util/create-update-in hash-map)
    :avl (util/create-update-in avl/sorted-map)
    :btree (util/create-update-in btree-map/sorted-map)))

(defn ->db [{:keys [storage]}]
  (->Db (map* storage) (map* storage) (map* storage) (map* storage)))

(defn- map->triples [m]
  (let [eid (or (:db/id m) (throw (ex-info "Entity map must have :db/id" {:entity m})))]
    (->> (dissoc m :db/id)
         (map (fn [[k v]] (vector eid k v))))))

(defn- db->type [db]
  (cond
    (instance? AVLMap (:eav db)) :avl
    (instance? PersistentSortedMap (:eav db)) :btree
    :else :hash-map))

;; TODO: Do this with transcients
(defn index-triple-add [db [e a v :as _triple]]
  (let [type (db->type db)
        update-in (->update-in-fn type)]
    (-> db
        (update-in [:eav e a] (fnil conj (set* type)) v)
        (update-in [:aev a e] (fnil conj (set* type)) v)
        (update-in [:ave a v] (fnil conj (set* type)) e)
        (update-in [:vae v a] (fnil conj (set* type)) e))))

(defn transaction->triples [transaction]
  (cond
    (map? transaction) (map->triples transaction)
    (= :db/add (first transaction)) [(vec (rest transaction) )]
    (= :db/retract (first transaction)) [(vec (rest transaction))]))

(defn transact [db tx-data]
  {:pre [(instance? Db db)]}
  (let [triples (mapcat #(transaction->triples %) tx-data)]
    (reduce index-triple-add db triples)))
