(ns hooray.transact
  (:require [clojure.spec.alpha :as s]))

(s/def ::entity (s/keys :req [:db/id]))
(s/def ::add-transaction #(and (= :db/add (first %)) (vector? %) (= 4 (count %))))
(s/def ::retract-transaction #(and (= :db/retract (first %)) (vector? %) (= 4 (count %))))
(s/def ::retract-entity #(and (= :db/retractEntity (first %)) (vector? %) (= 2 (count %))))
(s/def ::transaction (s/or :map ::entity
                           :add ::add-transaction
                           :retract ::retract-transaction
                           :retract-entity ::retract-entity))
(s/def ::tx-data (s/* ::transaction))

(comment
  (s/valid? ::tx-data [{:db/id "foo"
                        :foo/bar "x"}
                       [:db/add "foo" :is/cool true]])

  (s/valid? ::transaction
            {:db/id :db/edge-attribute
             :db/ident :g/to
             :db/cardinality :db.cardinality/many}))

(s/def :db/ident (s/and keyword? (fn [k] (not= "db" (namespace k)))))

(s/def :db/cardinality #{:db.cardinality/one :db.cardinality/many})

(s/def ::attribute-entity-schema (s/keys :req [:db/ident]
                                         :opt [:db/cardinality]))

(defn attribute-schema? [m]
  (s/valid? ::attribute-entity-schema m))

(s/def ::schema-tx (s/and vector?
                          (s/every ::attribute-entity-schema :min-count 1)))

(defn schema-tx? [tx-data]
  (if (and (every? #(s/valid? ::transaction %) tx-data)
           (some attribute-schema? tx-data)
           (some (comp not attribute-schema?) tx-data))
    (throw (ex-info "Currently schema transaction cannot be mixed with non-schema transactions" {:tx-data tx-data}))
    (s/valid? ::schema-tx tx-data)))

(defn index-schema [schema tx-data]
  (merge schema (-> (group-by :db/ident tx-data)
                    (update-vals first))))

(defn attribute-cardinality [schema attr]
  (get-in schema [attr :db/cardinality] :db.cardinality/one))
