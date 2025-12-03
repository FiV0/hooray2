(ns hooray.core
  (:require [clojure.tools.logging :as log]
            [clojure.spec.alpha :as s]
            [hooray.db :as db]
            [hooray.query :as query]
            [hooray.transact :as t]
            [hooray.incremental :as incremental])
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
    (when (seq @!inc-qs)
      (doseq [inc-q (vals !inc-qs)]
        (incremental/compute-delta! inc-q db-before db-after tx-data)))))

(defn db [{:keys [!dbs] :as node}]
  {:pre [(node? node)]}
  ;; TODO support time travel
  (last @!dbs))

(defn q [query db & args]
  {:pre [(db/db? db)]}
  (query/query db query args))

(comment
  (def test-db (db (connect {:type :mem :storage :hash :algo :generic})))
  (db/db? test-db)
  (q '{:find [a]
       :where [[a :foo "bar"]]}
     test-db))

(defn q-inc [query {:keys [!inc-qs] :as node}]
  {:pre [(node? node)]}
  (let [{:keys [!queue] :as inc-q} (incremental/query (db node) query)]
    (swap! !inc-qs assoc query inc-q)
    !queue))
