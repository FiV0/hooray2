(ns hooray.db
  (:require [clojure.data.avl :as avl]
            [hooray.util.persistent-map :as btree-map]
            [hooray.util :as util]
            [hooray.transact :as t]
            [me.tonsky.persistent-sorted-set :as btree-set])
  (:import (org.hooray UniversalComparator)
           (clojure.data.avl AVLMap)
           (hooray.util.persistent_map PersistentSortedMap)))

(defrecord Db [eav aev ave vae opts])

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
    :hash (util/create-update-in hash-map)
    :avl (util/create-update-in #(avl/sorted-map-by universal-comp))
    :btree (util/create-update-in #(btree-map/sorted-map universal-comp))))

(defn ->db [{:keys [storage] :as opts}]
  (->Db (map* storage) (map* storage) (map* storage) (map* storage) opts))

(defn- map->triples [m]
  (let [eid (or (:db/id m) (throw (ex-info "Entity map must have :db/id" {:entity m})))]
    (->> (dissoc m :db/id)
         (map (fn [[k v]] (vector eid k v))))))

(defn- db->type [db]
  (cond
    (instance? AVLMap (:eav db)) :avl
    (instance? PersistentSortedMap (:eav db)) :btree
    :else :hash))

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

(defn reserved-keyword? [k]
  (and (keyword? k) (= (namespace k) "db")))

(defn check-triple [triple]
  (when (some reserved-keyword? triple)
    (throw (ex-info "Transaction contains reserved keywords" {:triple triple}))))

;; TODO: propeerly support attribute schema as first class entities and initialize the db with
;; the attribute schema attributes
(defn transact [db tx-data]
  {:pre [(instance? Db db)]}
  (let [triples (mapcat #(transaction->triples %) tx-data)]
    (prn (t/schema-tx? tx-data))
    (if (t/schema-tx? tx-data)
      (t/index-schema! tx-data)
      (run! check-triple triples))
    (reduce index-triple-add db triples)))
