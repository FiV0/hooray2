(ns hooray.core
  (:require [clojure.tools.logging :as log]
            [clojure.spec.alpha :as s]))

(s/def ::type #{:mem})
(s/def ::storage #{:hash-map :avl :btree})
(s/def ::algo #{:hash :leapfrog :generic :combi})

(s/def ::conn-opts (s/keys :req-un [::type ::storage ::algo]))

(defrecord Node [opts ])

(defn connect [opts]
  {:pre [(s/valid? ::conn-opts opts)]}
  (throw (Exception. "Not implemented yet")))

(s/def ::entity (s/keys :req [:db/id]))
(s/def ::add-transaction #(and (= :db/add (first %)) (vector? %) (= 4 (count %))))
(s/def ::retract-transaction #(and (= :db/retract (first %)) (vector? %) (= 4 (count %))))
(s/def ::transaction (s/or :map ::entity
                           :add ::add-transaction
                           :retract ::retract-transaction))
(s/def ::tx-data (s/* ::transaction))

(comment
  (s/valid? ::tx-data [{:db/id "foo"
                        :foo/bar "x"}
                       [:db/add "foo" :is/cool true]]))


(defn transact [node tx-data]
  {:pre [(instance? Node node) (s/valid? ::tx-data tx-data)]}
  (throw (Exception. "Not implemented yet")))

(defn db [node]
  {:pre [(instance? Node node)]}
  (throw (Exception. "Not implemented yet")))

(defrecord Db [])

(defn q [query & inputs]
  {:pre [(>= (count inputs) 1) (instance? Db (first inputs))]}
  (when (> (count inputs) 1)
    (log/warn "Hooray currently only supports one source!"))
  (throw (Exception. "Not implemented yet")))
