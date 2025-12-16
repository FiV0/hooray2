(ns hooray.schema
  (:require [clojure.spec.alpha :as s]))

(def type-predicates
  {:db.type/bigdec   #(instance? java.math.BigDecimal %)
   :db.type/bigint   #(instance? java.math.BigInteger %)
   :db.type/boolean  #(instance? Boolean %)
   :db.type/bytes    #(instance? (Class/forName "[B") %)  ;; byte array
   :db.type/double   #(instance? Double %)
   :db.type/float    #(instance? Float %)
   :db.type/instant  #(instance? java.util.Date %)
   :db.type/keyword  keyword?
   :db.type/long     #(instance? Long %)
   :db.type/ref      any?
   #_#(or (integer? %)     ;; entity id
          (keyword? %)     ;; ident
          (and (vector? %) ;; lookup ref
               (= 2 (count %))))
   :db.type/string   string?
   :db.type/symbol   symbol?
   :db.type/tuple    vector?
   :db.type/uuid     #(instance? java.util.UUID %)
   :db.type/uri      #(instance? java.net.URI %)})

(defn check-value [value-type value]
  (when-let [pred (get type-predicates value-type)]
    (pred value)))

(defn valid-value-type? [v]
  (contains? #{:db.type/bigdec :db.type/bigint :db.type/boolean
               :db.type/bytes :db.type/double :db.type/float
               :db.type/instant :db.type/keyword :db.type/long
               :db.type/ref :db.type/string :db.type/symbol
               :db.type/tuple :db.type/uuid :db.type/uri}
             v))

(defn valid-cardinality? [v]
  (contains? #{:db.cardinality/one :db.cardinality/many} v))

(defn valid-uniqueness? [v]
  (contains? #{:db.unique/identity :db.unique/value} v))


(s/def :db/ident keyword?)

(s/def :db/cardinality valid-cardinality?)

(s/def :db/valueType valid-value-type?)

(s/def :db/unique valid-uniqueness?)

(s/def :db/doc string?)

(s/def :db/index boolean?)

(s/def :db.attr/preds (s/coll-of symbol? :kind vector? :min-count 1))

(s/def ::attribute-entity-schema
  (s/and (s/keys :req [:db/id
                       :db/ident
                       :db/valueType
                       :db/cardinality]
                 :opt [:db/unique
                       :db/doc
                       :db/index
                       :db.attr/preds])
         #(or (neg-int? (:db/id %))
              (s/and keyword? (fn [k] (= "db" (namespace k)))))))

(defn attribute-schema? [m]
  (s/valid? ::attribute-entity-schema m))

(s/def ::schema-tx (s/and vector?
                          (s/every ::attribute-entity-schema :min-count 1)))

(s/def ::user-defined-attribute-schema
  (s/and ::attribute-entity-schema
         #(not= "db" (namespace (:db/ident %)))))

(defn check-user-defined-schema! [m]
  (or (s/valid? ::user-defined-attribute-schema m)
      (throw (ex-info "Attribute schema contains reserved keywords" {:attribute-entity m}))))

(def initial-schema
  [ ;; The identity attribute - names schema entities
   {:db/id          -1
    :db/ident       :db/ident
    :db/valueType   :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/unique      :db.unique/identity
    :db/doc         "Programmatic name for an entity"}

   ;; The value type attribute
   {:db/id          -2
    :db/ident       :db/valueType
    :db/valueType   :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/doc         "Type of value an attribute can hold"
    :db.attr/preds  ['hooray.schema/valid-value-type?]}

   ;; The cardinality attribute
   {:db/id          -3
    :db/ident       :db/cardinality
    :db/valueType   :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/doc         "Whether attribute is single or multi-valued"
    :db.attr/preds  ['hooray.schema/valid-cardinality?]}

   ;; The uniqueness attribute
   {:db/id          -4
    :db/ident       :db/unique
    :db/valueType   :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/doc         "Uniqueness constraint for attribute values"
    :db.attr/preds  ['hooray.schema/valid-uniqueness?]}

   ;; The doc attribute
   {:db/id          -5
    :db/ident       :db/doc
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one
    :db/doc         "Documentation string"}

   ;; The index attribute
   {:db/id          -6
    :db/ident       :db/index
    :db/valueType   :db.type/boolean
    :db/cardinality :db.cardinality/one
    :db/doc         "Whether to index this attribute"}

   ;; Attribute predicates
   {:db/id          -7
    :db/ident       :db.attr/preds
    :db/valueType   :db.type/symbol
    :db/cardinality :db.cardinality/many
    :db/doc         "Predicate functions constraining attribute values"}

   #_#_
   ;; The component attribute
   {:db/ident       :db/isComponent
    :db/valueType   :db.type/boolean
    :db/cardinality :db.cardinality/one
    :db/doc         "Whether ref attribute points to component entity"}

   ;; The no-history attribute
   {:db/ident       :db/noHistory
    :db/valueType   :db.type/boolean
    :db/cardinality :db.cardinality/one
    :db/doc         "Whether to retain history for this attribute"}


   #_#_
   ;; Entity spec - required attributes
   {:db/ident       :db.entity/attrs
    :db/valueType   :db.type/keyword
    :db/cardinality :db.cardinality/many
    :db/doc         "Required attributes for an entity spec"}

   ;; Entity spec - predicates
   {:db/ident       :db.entity/preds
    :db/valueType   :db.type/symbol
    :db/cardinality :db.cardinality/many
    :db/doc         "Predicate functions for entity validation"}

   ;; Tuple attributes
   #_#_#_{:db/ident       :db/tupleAttrs
          :db/valueType   :db.type/tuple
          :db/tupleType   :db.type/keyword ;; homogeneous tuple of keywords
          :db/cardinality :db.cardinality/one
          :db/doc         "Source attributes for composite tuple"}

   {:db/ident       :db/tupleTypes
    :db/valueType   :db.type/tuple
    :db/tupleType   :db.type/keyword ;; homogeneous tuple of keywords
    :db/cardinality :db.cardinality/one
    :db/doc         "Value types for heterogeneous tuple"}

   {:db/ident       :db/tupleType
    :db/valueType   :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/doc         "Value type for homogeneous tuple"}
   ])
