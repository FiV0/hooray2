(ns hooray.schema)

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

(def initial-schema
  [ ;; The identity attribute - names schema entities
   {:db/ident       :db/ident
    :db/valueType   :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/unique      :db.unique/identity
    :db/doc         "Programmatic name for an entity"}

   ;; The value type attribute
   {:db/ident       :db/valueType
    :db/valueType   :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/doc         "Type of value an attribute can hold"
    :db.attr/preds  'hooray.schema/valid-value-type?}

   ;; The cardinality attribute
   {:db/ident       :db/cardinality
    :db/valueType   :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/doc         "Whether attribute is single or multi-valued"
    :db.attr/preds  'hooray.schema/valid-cardinality?}

   ;; The uniqueness attribute
   {:db/ident       :db/unique
    :db/valueType   :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/doc         "Uniqueness constraint for attribute values"
    :db.attr/preds  'hooray.schema/valid-uniqueness?}

   ;; The doc attribute
   {:db/ident       :db/doc
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one
    :db/doc         "Documentation string"}

   ;; The index attribute
   {:db/ident       :db/index
    :db/valueType   :db.type/boolean
    :db/cardinality :db.cardinality/one
    :db/doc         "Whether to index this attribute"}

   ;; Attribute predicates
   {:db/ident       :db.attr/preds
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
