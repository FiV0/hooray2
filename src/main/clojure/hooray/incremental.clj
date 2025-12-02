(ns hooray.incremental
  (:require [hooray.db :as db]
            [hooray.util :as util]))


;; (defrecord ZSetIndices [eav aev ave vae])

;; (defn triple-)


;; (defn index-triple-add [{:keys [eav ave vae schema] :as db} [e a v :as _triple]]
;;   (let [type (db->type db)
;;         update-in (->update-in-fn type)
;;         empty-set (set* type)
;;         cardinality (t/attribute-cardinality schema a)
;;         previous-v (first (get-in eav [e a]))]
;;     (case cardinality
;;       :db.cardinality/one (let [db (if previous-v
;;                                      (assoc db
;;                                             :ave (update-in ave [a previous-v] disj e)
;;                                             :vae (update-in vae [previous-v a] disj e))
;;                                      db)]
;;                             (-> db
;;                                 (update-in [:eav e a] (fn [_] (conj empty-set v)))
;;                                 (update-in [:aev a e] (fn [_] (conj empty-set v)))
;;                                 (update-in [:ave a v] (fnil conj empty-set) e)
;;                                 (update-in [:vae v a] (fnil conj empty-set) e)))
;;       :db.cardinality/many (-> db
;;                                (update-in [:eav e a] (fnil conj empty-set) v)
;;                                (update-in [:aev a e] (fnil conj empty-set) v)
;;                                (update-in [:ave a v] (fnil conj empty-set) e)
;;                                (update-in [:vae v a] (fnil conj empty-set) e)))))

;; (defn index-triple-retract [{:keys [eav aev ave vae] :as db} [e a v :as _triple]]
;;   (let [update-in (->update-in-fn (db->type db))]
;;     (assoc db
;;            :eav (update-in eav [e a] disj v)
;;            :aev (update-in aev [a e] disj v)
;;            :ave (update-in ave [a v] disj e)
;;            :vae (update-in vae [v a] disj e))))
