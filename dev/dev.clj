(ns dev
  (:require [clojure.tools.logging :as log]
            [hooray.logging :as hooray-log]))


(comment
  (hooray-log/set-log-level! "dev" :debug)
  (hooray-log/set-log-level! "dev" :info)
  (log/info "Development REPL loaded")
  (log/debug "Debug message"))
