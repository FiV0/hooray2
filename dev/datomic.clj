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
