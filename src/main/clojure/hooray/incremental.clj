(ns hooray.incremental
  (:require [hooray.zset]
            [hooray.util :as util]
            [hooray.transact :as t])
  (:import (org.hooray.incremental ZSet IndexedZSet IntegerWeight)))

(defn ->int-weight [i] (IntegerWeight. i))

(defn addition [^IntegerWeight w1 ^IntegerWeight w2]
  (.add w1 w2))

(defn subtraction [^IntegerWeight w1 ^IntegerWeight w2]
  (.add w1 (.negate w2)))

(def empty-zset (ZSet/empty))

(def empty-indexed-zset (IndexedZSet/empty))

(def ^:private zero IntegerWeight/ZERO)
(def ^:private one IntegerWeight/ONE)

(defrecord ZSetIndices [eav aev ave vae])

(defn ->zset-indices []
  (->ZSetIndices (empty-indexed-zset) (empty-indexed-zset) (empty-indexed-zset) (empty-indexed-zset)))

(def ^:private zset-update-in (util/create-update-in (empty-indexed-zset)))

(defn index-triple [{:keys [eav schema] :as db}
                    {eav-zset :eav aev-zset :aev
                     ave-zset :ave vae-zset :vae :as zset-indices}
                    [type e a v :as _triple]]
  (let [cardinality (t/attribute-cardinality schema a)]
    (case [type cardinality]
      ([:db/retract :db.cardinality/one] [:db/retract :db.cardinality/many])
      (if (-> (get-in eav [e a]) (contains? v))
        ;; TODO this might not clean up empty nested structures in the indexed zsets
        (-> zset-indices
            (assoc :eav (zset-update-in eav-zset [e a] (fnil update empty-zset) v (fnil subtraction zero) one))
            (assoc :aev (zset-update-in aev-zset [a e] (fnil update empty-zset) v (fnil subtraction zero) one))
            (assoc :ave (zset-update-in ave-zset [a v] (fnil update empty-zset) v (fnil subtraction zero) one))
            (assoc :vae (zset-update-in vae-zset [v a] (fnil update empty-zset) e (fnil subtraction zero) one)))
        zset-indices)
      [:db/add :db.cardinality/one]
      (let [previous-v (first (get-in eav [e a]))
            zset-indices (index-triple db zset-indices [:db/retract e a previous-v])]
        (-> zset-indices
            (zset-update-in [:eav e a] (fnil update empty-zset) v (fnil addition zero) one)
            (zset-update-in [:aev a e] (fnil update empty-zset) v (fnil addition zero) one)
            (zset-update-in [:ave a v] (fnil update empty-zset) e (fnil addition zero) one)
            (zset-update-in [:vae v a] (fnil update empty-zset) e (fnil addition zero) one)))
      [:db/add :db.cardinality/many]
      (if (-> (get-in eav [e a]) (contains? v))
        zset-indices
        (-> zset-indices
            (zset-update-in [:eav e a] (fnil update empty-zset) v (fnil addition zero) one)
            (zset-update-in [:aev a e] (fnil update empty-zset) v (fnil addition zero) one)
            (zset-update-in [:ave a v] (fnil update empty-zset) e (fnil addition zero) one)
            (zset-update-in [:vae v a] (fnil update empty-zset) e (fnil addition zero) one))))))

(comment
  (def eav empty-indexed-zset)

  (def zset-update-in (util/create-update-in empty-indexed-zset))

  (def indexed-zset (zset-update-in eav [1 2] (fnil assoc empty-zset) 3 (->int-weight 5)))
  (-> indexed-zset
      (zset-update-in [1 2] (fnil update empty-zset) 3 (fnil addition IntegerWeight/ZERO) (->int-weight 5))
      (zset-update-in [1 2] (fnil update empty-zset) 2 (fnil addition IntegerWeight/ZERO) (->int-weight 5))
      (zset-update-in [:foo :bar] (fnil update empty-zset) 2 (fnil addition IntegerWeight/ZERO) (->int-weight 5))))
