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

(defrecord Node [!dbs opts !inc-qs !delta-listeners]
  Closeable
  (close [_] nil))

(defn node? [x]
  (instance? Node x))

(defn connect [opts]
  {:pre [(s/valid? ::conn-opts opts)]}
  (->Node (atom [(db/->db opts)]) opts (atom {}) (atom {})))

(defn- notify-delta-listeners! [{:keys [!delta-listeners]} inc-q delta]
  (doseq [listener (vals (get @!delta-listeners (:id inc-q)))]
    (listener delta)))

(defn transact [{:keys [!dbs !inc-qs] :as node} tx-data]
  {:pre [(node? node) (s/valid? ::t/tx-data tx-data)]}
  (let [db-before (last @!dbs)
        db-after (last (swap! !dbs (fn [dbs]
                                     (conj dbs (db/transact db-before tx-data)))))]
    (when-let [inc-qs (seq @!inc-qs)]
      (doseq [inc-q (vals inc-qs)]
        (let [delta (if (dbsp/dbsp-query? inc-q)
                      (dbsp/compute-delta! inc-q db-before tx-data)
                      (incremental/compute-delta! inc-q db-before db-after tx-data))]
          (when (seq delta)
            (notify-delta-listeners! node inc-q delta)))))))

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

(defn unregister-inc-q [{:keys [!inc-qs !delta-listeners] :as node} {:keys [id] :as inc-q}]
  {:pre [(node? node)]}
  (swap! !inc-qs dissoc id)
  (swap! !delta-listeners dissoc id)
  node)

(defn- register-delta-listener! [{:keys [!delta-listeners] :as node} {:keys [id] :as _inc-q} listener]
  {:pre [(node? node)]}
  (let [listener-id (random-uuid)]
    (swap! !delta-listeners update id assoc listener-id listener)
    listener-id))

(defn- unregister-delta-listener! [{:keys [!delta-listeners] :as node} {:keys [id] :as _inc-q} listener-id]
  {:pre [(node? node)]}
  (swap! !delta-listeners
         (fn [listeners]
           (let [remaining (not-empty (dissoc (get listeners id) listener-id))]
             (if remaining
               (assoc listeners id remaining)
               (dissoc listeners id)))))
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
        output-ch (async/chan 1024)
        listener-id (promise)]
    (deliver listener-id
             (register-delta-listener!
              conn inc-q
              (fn [delta]
                (when-not (async/>!! output-ch delta)
                  (unregister-delta-listener! conn inc-q @listener-id)
                  (unregister-inc-q conn inc-q)))))
    output-ch))

(defrecord IncrementalSubscription [conn inc-q listener-id]
  Closeable
  (close [_]
    (unregister-delta-listener! conn inc-q listener-id)
    (unregister-inc-q conn inc-q)))

(defn subscribe ^Closeable [conn query callback]
  (let [inc-q (q-inc conn query)
        listener-id (register-delta-listener! conn inc-q callback)]
    (->IncrementalSubscription conn inc-q listener-id)))
