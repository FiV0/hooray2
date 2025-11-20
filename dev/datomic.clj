(ns datomic
  (:require [datomic.client.api :as d]))

(def client (d/client {:server-type :datomic-local
                       :system "datomic-samples"}))

(comment
  (d/list-databases client {})
  ;; => ["streets"
  ;;     "mbrainz-subset"
  ;;     "graph"
  ;;     "movies"
  ;;     "social-news"
  ;;     "friends"
  ;;     "decomposing-a-query"
  ;;     "solar-system"
  ;;     "dilithium-crystals"]
  )

(def conn (d/connect client {:db-name "mbrainz-subset"}))

(def db (d/db conn))

(comment
  (d/q '[:find ?title ?album ?year
         :in $ [?artist-name ...]
         :where
         [?a :artist/name   ?artist-name]
         [?t :track/artists ?a]
         [?t :track/name    ?title]
         [?m :medium/tracks ?t]
         [?r :release/media ?m]
         [?r :release/name  ?album]
         [?r :release/year  ?year]]
       db ["John Lennon"]))

;; Complete graph setup
(def graph-schema
  [{:db/ident :g/to
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/many
    :db/doc "Edge from one node to another in a graph"}])

(defn complete-graph [n]
  (for [i (range n) j (range (inc i) n)]
    [i j]))

(defn graph->datomic-txs [g]
  (let [;; Get all unique node IDs
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
    (d/transact conn {:tx-data node-txs})
    (d/transact conn {:tx-data edge-txs})))

(comment
  ;; Create a new database for the graph
  (d/create-database client {:db-name "complete-graph-100"})

  ;; Connect to the graph database
  (def graph-conn (d/connect client {:db-name "complete-graph-100"}))

  ;; Transact the schema
  (d/transact graph-conn {:tx-data graph-schema})

  ;; Generate and transact complete graph with 100 nodes
  (def complete-100 (complete-graph 100))
  (transact-graph graph-conn complete-100)

  ;; Triangle query
  (def triangle-query '[:find ?a ?b #_?c
                        :where
                        [?a :g/to ?b]
                        #_[?a :g/to ?c]
                        #_[?b :g/to ?c]])

  ;; Run the triangle query
  (def graph-db (d/db graph-conn))
  (d/q '[:find ?a :where [?a :g/to ?b]] graph-db)

  ;; Clean up
  (d/delete-database client {:db-name "complete-graph-100"}))
