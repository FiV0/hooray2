(ns hooray.core
  (:require [clojure.tools.logging :as log]
            [clojure.spec.alpha :as s]
            [hooray.db :as db]
            [hooray.query :as query])
  (:import (hooray.db Db)))

(s/def ::type #{:mem})
(s/def ::storage #{:hash :avl :btree})
(s/def ::algo #{:hash :leapfrog :generic :combi})

(s/def ::conn-opts (s/keys :req-un [::type ::storage ::algo]))

(defrecord Node [!dbs opts])

(defn connect [opts]
  {:pre [(s/valid? ::conn-opts opts)]}
  (->Node (atom [(db/->db opts)]) opts))

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


(defn transact [{:keys [!dbs] :as node} tx-data]
  {:pre [(instance? Node node) (s/valid? ::tx-data tx-data)]}
  (swap! !dbs (fn [dbs]
                (conj dbs (db/transact (last dbs) tx-data)))))

(defn db [{:keys [!dbs] :as node}]
  {:pre [(instance? Node node)]}
  ;; TODO support time travel
  (last @!dbs))

(defn q [query & inputs]
  (prn (instance? Db (first inputs)))
  #_{:pre [(>= (count inputs) 1) (instance? Db (first inputs))]}
  (when (> (count inputs) 1)
    (log/warn "Hooray currently only supports one source!"))
  (query/query (first inputs) query))


(comment
  (def test-db (db (connect {:type :mem :storage :hash :algo :generic})))
  (instance? Db test-db)
  (q '{:find [a]
       :where [[a :foo "bar"]]}
     test-db)


  )
