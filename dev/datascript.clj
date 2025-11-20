(ns datascript
  (:require [datascript.core :as d]
            [hooray.triangle-test :as tt]
            [hooray.graph-gen :as g]))

(def schema {:g/to {:db/cardinality :db.cardinality/many}})

;; (def conn (d/create-conn schema))

(def db (-> (d/empty-db schema)
            (d/db-with [[:db/add 0 :g/to 1]] #_(g/graph->ops (g/complete-graph 100)))))
