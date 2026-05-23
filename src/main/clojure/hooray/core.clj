(ns hooray.core
  (:require [clojure.spec.alpha :as s]
            [clojure.core.async :as async]
            [hooray.db :as db]
            [hooray.query :as query]
            [hooray.pull :as pull]
            [hooray.transact :as t]
            [hooray.incremental :as incremental]
            [hooray.dbsp :as dbsp])
  (:import (java.io Closeable)))

;; (set! *print-namespace-maps* false)

(s/def ::type #{:mem})
(s/def ::storage #{:hash :avl :btree})
(s/def ::algo #{:hash :leapfrog :generic :combi})

(s/def ::conn-opts (s/keys :req-un [::type ::storage ::algo]))

(defrecord Node [!dbs opts !inc-qs]
  Closeable
  (close [_] nil))

(defn node? [x]
  (instance? Node x))

(defn connect [opts]
  {:pre [(s/valid? ::conn-opts opts)]}
  (->Node (atom [(db/->db opts)]) opts (atom {})))

(defn transact [{:keys [!dbs !inc-qs] :as node} tx-data]
  {:pre [(node? node) (s/valid? ::t/tx-data tx-data)]}
  (let [db-before (last @!dbs)
        db-after (last (swap! !dbs (fn [dbs]
                                     (conj dbs (db/transact db-before tx-data)))))]
    (when-let [inc-qs (seq @!inc-qs)]
      (doseq [inc-q (vals inc-qs)]
        (if (dbsp/dbsp-query? inc-q)
          (dbsp/compute-delta! inc-q db-before tx-data)
          (incremental/compute-delta! inc-q db-before db-after tx-data))))))

(defn db [{:keys [!dbs] :as node}]
  {:pre [(node? node)]}
  ;; TODO support time travel
  (last @!dbs))

(defn entity [db eid]
  {:pre [(db/db? db)]}
  (db/entity db eid))

(defn pull [db pattern eid]
  {:pre [(db/db? db)]}
  (pull/pull db pattern eid))

(defn q [query db & args]
  {:pre [(db/db? db)]}
  (query/query db query args))

(comment
  (def test-db (db (connect {:type :mem :storage :hash :algo :generic})))
  (db/db? test-db)
  (q '{:find [a]
       :where [[a :foo "bar"]]}
     test-db))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Incremental queries
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def ^:dynamic *dbsp-version*
  "Selects the incremental engine used by `q-inc`: `:wcoj` (default, the
  incremental WCOJ engine) or `:standard` (the DBSP-standard circuit engine).
  Rebind around `q-inc` to choose; the choice is recorded per registered query."
  :wcoj)

(defn q-inc [{:keys [!inc-qs] :as node} query]
  {:pre [(node? node)]}
  (let [inc-q (case *dbsp-version*
                :wcoj (incremental/query (db node) query)
                :standard (dbsp/compile-query (db node) query))]
    (swap! !inc-qs assoc (:id inc-q) inc-q)
    inc-q))

(defn unregister-inc-q [{:keys [!inc-qs] :as node} {:keys [id] :as inc-q}]
  {:pre [(node? node)]}
  (swap! !inc-qs dissoc id)
  node)

(defn consume-delta! [inc-q]
  (if (dbsp/dbsp-query? inc-q)
    (dbsp/pop-result! inc-q)
    (incremental/pop-result! inc-q)))


(defrecord IncrementalStream [conn inc-q]
  Closeable
  (close [_] (unregister-inc-q conn inc-q)))

(defn open-deltas ^Closeable [conn query]
  (->IncrementalStream conn (q-inc conn query)))

(defn take! [{:keys [inc-q]}]
  (consume-delta! inc-q))

(defn delta-chan [conn query]
  (let [inc-q (q-inc conn query)
        output-ch (async/chan 1024)]
    (async/thread
      (loop []
        ;; TODO This needs new deltas to arrive to close
        (when-let [delta (consume-delta! inc-q)]
          (when (async/>!! output-ch delta)
            (recur)))))
    output-ch))

(defrecord IncrementalSubscription [delta-ch]
  Closeable
  (close [_] (async/close! delta-ch)))

(defn subscribe ^Closeable [conn query callback]
  (let [delta-ch (delta-chan conn query)]
    (async/go-loop []
      (when-let [delta (async/<! delta-ch)]
        (callback delta)
        (recur)))
    (->IncrementalSubscription delta-ch)))
