(ns datomic
  (:require [datomic.client.api :as d]
            [hooray.graph-gen :as g]))

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

#_(def conn (d/connect client {:db-name "mbrainz-subset"}))

#_(def db (d/db conn))


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
       db ["John Lennon"])

  (d/q '[:find (pull ?attr [*])
         :where
         [?attr :db/ident]]
       db))

(comment
  (d/create-database client {:db-name "not-unbound"})

  (def not-unbound-conn (d/connect client {:db-name "not-unbound"}))

  (def person-schema
    [{:db/ident :person/name
      :db/valueType :db.type/string
      :db/cardinality :db.cardinality/one
      :db/doc "First name of a person"}
     {:db/ident :person/last-name
      :db/valueType :db.type/string
      :db/cardinality :db.cardinality/one
      :db/doc "Last name of a person"}
     {:db/ident :person/sex
      :db/valueType :db.type/keyword
      :db/cardinality :db.cardinality/one
      :db/doc "The sex of the person"}])

  (d/transact not-unbound-conn {:tx-data person-schema})

  (d/db-stats db)

  (def tx-data
    [{:db/id "ivan-ivanov-1" :person/name "Ivan" :person/last-name "Ivanov" :person/sex :male}
     {:db/id "ivan-ivanov-2" :person/name "Ivan" :person/last-name "Ivanov" :person/sex :male}
     #_{:db/id "ivan-ivanovtov-1" :person/name "Ivan" :person/last-name "Ivannotov" :person/sex :female}
     #_{:db/id "igor-1" :person/name "Igor" :person/last-name "Igorotov" :person/sex :male}])

  (def tx-data2
    [{:db/id 13194139533600 :person/name "Ivan" :person/last-name "Ivanov" :person/sex :male}
     #_{:db/id "ivan-ivanov-2" :person/name "Ivan" :person/last-name "Ivanov" :person/sex :male}
     #_{:db/id "ivan-ivanovtov-1" :person/name "Ivan" :person/last-name "Ivannotov" :person/sex :female}
     #_{:db/id "igor-1" :person/name "Igor" :person/last-name "Igorotov" :person/sex :male}])

  (d/en)

  (def res (d/transact not-unbound-conn {:tx-data tx-data2}))

  (d/q '{:find [(pull ?e [*])]
         :where [[?e :db/ident :db/ident]]}
       (d/db not-unbound-conn))

  (d/pull (d/db not-unbound-conn) '[*] 13194139533333)

  res

  (->> (d/q '{:find [(count ?sex)]
              :where [[?e :person/sex ?sex]]}
            (d/db not-unbound-conn)))
  ;; => [[1]]

  (->> (d/q '{:find [(count ?e) (count ?sex)]
              :where [[?e :person/sex ?sex]]}
            (d/db not-unbound-conn)))
  ;; => [[2 2]]

  (->> (d/q '{:find [(count ?e)]
              :where [[?e :person/name]
                      (or [?e :person/sex :male]
                          [?e :person/name "Ivan"])]}
            (d/db not-unbound-conn)))
  ;; => [[1]]

  (d/q '{:find [?e]
         :where [[?e :name ?name]
                 [?e :name "Ivan"]
                 (not [?e :last-name "Ivannotov"]
                      [?e :name ?unbound])]}
       (d/db not-unbound-conn))

  (d/delete-database client {:db-name "not-unbound"})
  )


;; graph setup
(def graph-schema
  [{:db/ident :g/to
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/many
    :db/doc "Edge from one node to another in a graph"}])

(defn graph->datomic-txs [g]
  (let [;; Get all unique node IDs
        nodes (into #{} (mapcat identity g))
        ;; Create entity assertions for all nodes using tempid strings
        node-txs (map (fn [node] {:db/id (str "node-" node)}) nodes)
        ;; Create edge assertions using tempid strings
        edge-txs (mapcat (fn [[from to]]
                           [[:db/add (str "node-" from) :g/to (str "node-" to)]
                            [:db/add (str "node-" to) :g/to (str "node-" from)]])
                         g)]
    {:node-txs node-txs
     :edge-txs edge-txs}))

(defn transact-graph [conn g]
  (let [{:keys [node-txs edge-txs]} (graph->datomic-txs g)]
    (d/transact conn {:tx-data (concat node-txs edge-txs)})
    ))

(comment
  ;; Create a new database for the graph
  (d/create-database client {:db-name "complete-graph-100"})

  ;; Connect to the graph database
  (def graph-conn (d/connect client {:db-name "complete-graph-100"}))

  ;; Transact the schema
  (d/transact graph-conn {:tx-data graph-schema})

  ;; Generate and transact complete graph with 100 nodes
  (def complete-100 (g/complete-graph 100))
  (def complete-bipartite-300 (g/complete-bipartite 300))
  (transact-graph graph-conn complete-bipartite-300)

  ;; Triangle query
  (def triangle-query '[:find ?a ?b ?c
                        :where
                        [?a :g/to ?b]
                        [?a :g/to ?c]
                        [?b :g/to ?c]])

  ;; Run the triangle query
  (def graph-db (d/db graph-conn))
  (time (d/q triangle-query graph-db))

  ;; Clean up
  (d/delete-database client {:db-name "complete-graph-100"}))
