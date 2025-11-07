(ns hooray.query
  (:require [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [clojure.core.match :refer [match]])
  (:import (org.hooray.algo GenericJoin LeapfrogJoin)
           (org.hooray.iterator
            AVLLeapfrogIndex AVLPrefixExtender BTreeLeapfrogIndex BTreePrefixExtender GenericPrefixExtender
            SealedIndex SealedIndex$MapIndex SealedIndex$SetIndex
            BTreeIndex
            AVLIndex AVLIndex$AVLMapIndex AVLIndex$AVLSetIndex)))

(s/def ::variable symbol?)
(s/def ::constant (complement symbol?))

(s/def ::pattern-element (s/or :variable ::variable
                               :constant ::constant))

(s/def ::triple-pattern (s/and vector?
                               (s/cat :e ::pattern-element
                                      :a ::pattern-element
                                      :v ::pattern-element)))

(s/def ::find (s/and vector?
                     (s/+ ::variable)))

(s/def ::where (s/and vector?
                      (s/+ ::triple-pattern)))

(s/def ::query (s/keys :req-un [::find ::where]))

;; We don't (yet) support a where clause of the form
;; [1 x "Alice"]
;; This is a very unlikely with Datomic as you will likely know you attributes
;; [x y "Alice"] -> vae, in this case attribute variable needs to come before entity
;; [1 x y] -> eav entity needs to come before attribute
;; For simplicity let's just forget about attribute variables for now

(defn variable-order [{wheres :where :as _conformed-query}]
  (-> (for [{:keys [e a v] :as where} wheres]
        (match [e a v]
          [[:constant _] [:constant _] [:constant _]] []
          [[:constant _] [:constant _] [:variable value-var]] [value-var]
          [[:variable entity-var] [:constant _] [:constant _]] [entity-var]
          [[:variable entity-var] [:constant _] [:variable value-var]] [entity-var value-var]
          [_ [:variable _] _] (throw (UnsupportedOperationException. "Currently varialbles in attribute position are not supported"))
          :else (throw (ex-info "Unknown where clause" {:where where}))))
      flatten
      distinct))

;; (defrecord Db [eav aev ave vae opts])

(defn unsupported-ex
  ([] (throw (UnsupportedOperationException.)))
  ([msg] (throw (UnsupportedOperationException. msg))))

(defn create-iterator [{:keys [storage algo] :as _opts} index var-order participates-in-level]
  (match [storage algo (count participates-in-level)]
    [:hash  _ depth]
    (GenericPrefixExtender. (if (< depth 2) (SealedIndex$SetIndex. index) (SealedIndex$MapIndex.  index)) participates-in-level)

    [:avl :generic depth]
    (GenericPrefixExtender. (if (< depth 2) (SealedIndex$SetIndex. index) (SealedIndex$MapIndex.  index)) participates-in-level)

    [:btree :generic depth]
    (GenericPrefixExtender. (if (< depth 2) (SealedIndex$SetIndex. index) (SealedIndex$MapIndex.  index)) participates-in-level)

    [:avl :leapfrog depth]
    (AVLLeapfrogIndex. (if (< depth 2) (AVLIndex$AVLSetIndex. index) (AVLIndex$AVLMapIndex. index))
                       var-order (set (for [level participates-in-level]
                                        (nth var-order level))))

    [:btree :leapfrog _depth]
    (unsupported-ex "BTrees not yet supported!")

    :else (throw (ex-info "not yet supported storage + algo type" {:storage storage :algo algo}))))

(defn- where-to-iterator [{:keys [eav ave aev opts] :as _db} var-order {:keys [e a v] :as where}]
  (let [var-to-index (zipmap var-order (range))]
    (match [e a v]
      [[:constant _] [:constant _] [:constant _]]
      (unsupported-ex)

      [[:constant e-const] [:constant a-const] [:variable v-var]]
      (create-iterator opts (get-in eav [e-const a-const]) var-order [(get var-to-index v-var)])

      [[:constant e-var] [:constant a-const] [:constant v-const]]
      (create-iterator opts (get-in ave [a-const v-const]) var-order [(get var-to-index e-var)])

      [[:variable e-var] [:constant a-const] [:variable v-var]]
      (let [e-index (var-to-index e-var)
            v-index (var-to-index v-var)
            [index participates-in-level] (if (< e-index v-index)  [aev [e-index v-index]] [ave [v-index e-index]])]
        (create-iterator opts (get index a-const) var-order participates-in-level))

      :else (throw (ex-info "Unknown where clause" {:where where})))))

(defn- zipmap-keys-fn [find var-to-index]
  (let [keys-in-var-order (->> (sort-by var-to-index find)
                               (map keyword)) ]
    (fn [row]
      (zipmap keys-in-var-order row))))

(defn- order-result-fn [find var-to-index]
  (fn [row]
    (mapv (fn [var] (nth row (var-to-index var))) find)))

(defn join [iterators {:keys [storage algo] :as opts}])

(defn query [db query]
  {:pre [(s/valid? ::query query)]}
  (let [{:keys [where] :as conformed-query} (s/conform ::query query)
        var-order (variable-order conformed-query)
        iterators (map (partial where-to-iterator db var-order) where)]))

(comment
  (def q '{:find [x y z]
           :where [[x :person/name y]
                   [x :person/age z]]})


  (s/valid? ::query q)

  (s/conform ::query q)

  (variable-order (s/conform ::query q)))
