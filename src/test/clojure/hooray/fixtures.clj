(ns hooray.fixtures
  (:require [hooray.core :as h]
            [hooray.test-util :as tu]
            [hooray.graph-gen :as g]
            [clojure.tools.logging :as log]))

(def ^:dynamic *opts* {:type :mem :storage :hash :algo :generic})
(def ^:dynamic *node* nil)

(defn with-node
  ([f] (with-node *opts* f))
  ([opts f]
   (binding [*node* (h/connect opts)]
     (f))))

#_:clj-kondo/ignore
(def ^:private sex-pred #{:male :female :other})

(def ^:private people-schema [{:db/id -100
                               :db/ident :name
                               :db/valueType :db.type/string
                               :db/cardinality :db.cardinality/one}
                              {:db/id -101
                               :db/ident :last-name
                               :db/valueType :db.type/string
                               :db/cardinality :db.cardinality/one}
                              {:db/id -102
                               :db/ident :sex
                               :db/valueType :db.type/keyword
                               :db/cardinality :db.cardinality/one
                               :db.attr/preds 'hooray.fixtures/sex-pred}
                              {:db/id -103
                               :db/ident :age
                               :db/valueType :db.type/long
                               :db/cardinality :db.cardinality/one}
                              {:db/id -104
                               :db/ident :salary
                               :db/valueType :db.type/long
                               :db/cardinality :db.cardinality/one}
                              {:db/id -105
                               :db/ident :city
                               :db/valueType :db.type/string
                               :db/cardinality :db.cardinality/one}])

(def people-schema2
  [{:db/id :db/person-name-attr
    :db/ident :person/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/id :db/person-age-attr
    :db/ident :person/age
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one}
   {:db/id :db/person-hobby-attr
    :db/ident :person/hobby
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}])


(defn with-people-schema [f]
  (h/transact *node* people-schema)
  (f))

(defn with-edge-attribute [f]
  (h/transact *node* [g/edge-attribute])
  (f))

(defn with-timing [f]
  (let [start-time-ms (System/currentTimeMillis)
        ret (try
              (f)
              (catch Exception e
                (log/error e "caught exception during")
                {:error (str e)}))]
    (merge (when (map? ret) ret)
           {:time-taken-ms (- (System/currentTimeMillis) start-time-ms)})))

(defmacro with-timing* [& body]
  `(with-timing (fn [] ~@body)))

(defn with-timing-logged [f]
  (let [{:keys [time-taken-ms]} (with-timing f)]
    (log/infof "Test took %s" (tu/format-time time-taken-ms))))

(defmacro with-timing-logged* [& body]
  `(let [{:keys [~'time-taken-ms]} (with-timing (fn [] ~@body))]
     (log/infof "Test took %s" (tu/format-time ~'time-taken-ms))))
