(ns datascript
  (:require [datascript.core :as d]
            [hooray.triangle-test :as tt]
            [hooray.graph-gen :as g]))

(def schema {:g/to {:db/cardinality :db.cardinality/many}})

(defn complete-graph [n]
  (let [n (inc n)]
    (for [i (range 1 n) j (range (inc i) n)]
      [i j])))

(defn complete-bipartite [n]
  (let [n (inc n)
        n1 (quot n 2)]
    (for [i (range 1 n1) j (range n1 n)]
      [i j])))

(defn graph->datomic-txs [g]
  (let [ ;; Get all unique node IDs
        nodes (into #{} (mapcat identity g))
        ;; Create entity assertions for all nodes
        node-txs (map (fn [node] {:db/id node}) nodes)
        ;; Create edge assertions
        edge-txs (map (fn [[from to]]
                        [:db/add from :g/to to])
                      g)]
    {:node-txs node-txs
     :edge-txs edge-txs}))

(defn transact-graph [conn g]
  (let [{:keys [node-txs edge-txs]} (graph->datomic-txs g)]
    (d/transact conn node-txs)
    (d/transact conn edge-txs)))

(comment
  (def conn (d/create-conn schema))

  (transact-graph conn (complete-bipartite 300))

  (time (d/q
         '[:find ?a ?b ?c
           :where
           [?a :g/to ?b]
           [?a :g/to ?c]
           [?b :g/to ?c]]
         (d/db conn))))
