(ns hooray.pull
  (:require [clojure.spec.alpha :as s]
            [hooray.db :as db]))

;; =============================================================================
;; Pull Pattern Spec Definitions
;; Based on Datomic Pull API grammar:
;; https://docs.datomic.com/query/query-pull.html
;; =============================================================================

;; -----------------------------------------------------------------------------
;; Basic building blocks
;; -----------------------------------------------------------------------------

;; Attribute name: an edn keyword that names an attribute
;; Can have leading underscore on the name part for reverse lookup
(s/def ::attr-name keyword?)

;; Wildcard: '*' symbol or "*" string
(s/def ::wildcard #{'* "*"})

;; Positive number for limits and recursion
(s/def ::positive-number pos-int?)

;; Recursion limit: positive number or '...' for unlimited
(s/def ::recursion-limit (s/or :depth ::positive-number
                               :unlimited #{'...}))

;; -----------------------------------------------------------------------------
;; Attribute options (modern syntax)
;; -----------------------------------------------------------------------------

;; :as option - rename attribute in result
;; [attr-name :as any-value]
(s/def ::as-option (s/cat :attr ::attr-name
                          :as #{:as}
                          :alias any?))

;; :limit option - limit cardinality-many results
;; [attr-name :limit (positive-number | nil)]
(s/def ::limit-option (s/cat :attr ::attr-name
                             :limit #{:limit}
                             :count (s/or :number ::positive-number
                                          :unlimited nil?)))

;; :default option - default value when attribute is missing
;; [attr-name :default any-value]
(s/def ::default-option (s/cat :attr ::attr-name
                               :default #{:default}
                               :value any?))

;; :xform option - transform the pulled value
;; [attr-name :xform symbol]
(s/def ::xform-option (s/cat :attr ::attr-name
                             :xform #{:xform}
                             :fn symbol?))

;; Combined attribute options (can have multiple options)
;; [attr-name attr-option+]
(s/def ::attr-options
  (s/cat :attr ::attr-name
         :options (s/+ (s/alt :as (s/cat :key #{:as} :val any?)
                              :limit (s/cat :key #{:limit} :val (s/or :n ::positive-number :nil nil?))
                              :default (s/cat :key #{:default} :val any?)
                              :xform (s/cat :key #{:xform} :val symbol?)))))

;; -----------------------------------------------------------------------------
;; Attribute expression
;; -----------------------------------------------------------------------------

;; Attribute expression: [attr-name attr-option+]
(s/def ::attr-expr
  (s/and vector? ::attr-options))

;; -----------------------------------------------------------------------------
;; Map specification (forward declaration needed for recursion)
;; -----------------------------------------------------------------------------

;; Map spec key: attribute name or attribute expression with options
(s/def ::map-spec-key
  (s/or :attr ::attr-name
        :expr ::attr-expr))

;; Map spec value: pattern or recursion limit
(s/def ::map-spec-value
  (s/or :pattern ::pattern
        :recursion ::recursion-limit))

;; Map specification
;; { ((attr-name | attr-expr) (pattern | recursion-limit))+ }
(s/def ::map-spec
  (s/and map?
         (s/map-of ::map-spec-key ::map-spec-value :min-count 1)))

;; -----------------------------------------------------------------------------
;; Attribute specification
;; -----------------------------------------------------------------------------

;; Attribute spec: one of the four types
(s/def ::attr-spec
  (s/or :attr-name ::attr-name
        :wildcard ::wildcard
        :map-spec ::map-spec
        :attr-expr ::attr-expr))

;; -----------------------------------------------------------------------------
;; Pattern (top-level)
;; -----------------------------------------------------------------------------

;; Pattern: vector of one or more attribute specifications
(s/def ::pattern
  (s/and vector?
         (s/coll-of ::attr-spec :min-count 1)))

(defn pull [db pattern eid]
  {:pre [(db/db? db) (s/valid? ::pattern pattern)]}
  (let [conformed-pull (s/conform ::pattern pattern)]
    (throw (ex-info "Pull not yet implemented" {:db db :pattern conformed-pull :eid eid})))

  )

(comment
  (s/conform ::pattern [:artist/name :artist/startYear])

  )
