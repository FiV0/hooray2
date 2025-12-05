(ns dev
  (:require [hooray.core :as h]
            [hooray.graph-gen :as g]
            [clojure.tools.logging :as log]
            [hooray.logging :as hooray-log]))

(set! *warn-on-reflection* true)

(comment

  (def node (h/connect {:type :mem :storage :hash :algo :generic}))
  (h/transact node [g/edge-attribute])
  (h/transact node (g/graph->ops (g/complete-bipartite 300)))

  (time (h/q
         '{:find [?a ?b ?c]
           :where
           [[?a :g/to ?b]
            [?a :g/to ?c]
            [?b :g/to ?c]]}
         (h/db node)))

  (hooray-log/set-log-level! "dev" :debug)
  (hooray-log/set-log-level! "hooray" :info)
  (log/info "Development REPL loaded")
  (log/debug "Debug message"))
