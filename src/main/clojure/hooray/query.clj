(ns hooray.query
  (:require [clojure.spec.alpha :as s]))

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

;; (defrecord Db [eav aev ave vae opts])

(defn query [db query]
  {:pre [(s/valid? ::query query)]}
  (throw (UnsupportedOperationException.)))

(comment
  (def q '{:find [x y z]
           :where [[x :person/name y]
                   [x :person/age z]]})

  (s/valid? ::query q)

  (s/conform ::query q))
