(ns hooray.query
  (:require [clojure.spec.alpha :as s]
            [clojure.core.match :refer [match]]))

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
          [[:constant _] [:constant _] [:variable value]] [value]
          [[:variable entity] [:constant _] [:constant _]] [entity]
          [[:variable entity] [:constant _] [:variable value]] [entity value]
          [_ [:variable _] _] (throw (UnsupportedOperationException. "Currently varialbles in attribute position are not supported"))
          :else (throw (ex-info "Unknown where clause" {:where where}))))
      flatten
      distinct))

;; (defrecord Db [eav aev ave vae opts])

(defn- where-to-index [{:keys [e a v] :as where} var-to-index]
  (match [e a v]
    [[:constant _] [:constant _] [:constant _]] :eav
    [[:constant _] [:constant _] [:variable v]] :eav
    [[:constant v] [:constant _] [:constant _]] :ave
    [[:variable v1] [:constant _] [:variable v2]] (if (< (var-to-index v1) (var-to-index v2)) :aev :ave)
    :else (throw (ex-info "Unknown where clause" {:where where}))))

(defn query [db query]
  {:pre [(s/valid? ::query query)]}
  (let [conformed-query (s/conform ::query query)
        var-order (variable-order conformed-query)
        var-to-index (zipmap var-order (range (count var-order)))]
    (throw (UnsupportedOperationException.))))

(comment
  (def q '{:find [x y z]
           :where [[x :person/name y]
                   [x :person/age z]]})


  (s/valid? ::query q)

  (s/conform ::query q)

  (variable-order (s/conform ::query q)))
