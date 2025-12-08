(ns examples)

(require '[hooray.core :as h])

(def node (h/connect {:type :mem :storage :hash :algo :generic}))

(h/transact node [{:db/id :ada :name "Ada" :last-name "Lovelace"}
                  {:db/id :petr :name "Alan" :last-name "Turing"}])

(h/q '{:find [name]
       :where [[?e :name name]
               [?e :last-name "Lovelace"]]}
     (h/db node))
;; => #{["Ada"]}

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Incremental computation
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(with-open [node (h/connect {:type :mem :storage :hash :algo :generic})]

  (h/transact node [{:db/id :adam :name "Adam"}])

  (let [inc-q (h/q-inc node
                       '{:find [name]
                         :where [[?e :name name]]})]

    (try
      (h/transact node [{:db/id :ada :name "Ada"}
                        {:db/id :alan :name "Alan"}
                        [:db/retractEntity :adam]])

      (h/consume-delta! inc-q)

      (finally
        (h/unregister-inc-q node inc-q)))))
;; => ([["Ada"] 1] [["Alan"] 1] [["Adam"] -1])
