(ns xtdb
  (:require [xtdb.api :as xt]))

(comment

  (def node (xt/start-node {}))

  (xt/submit-tx node
                [[::xt/put {:xt/id :person-1
                            :person/department :it-derpartment
                            :person/salary 100.0}]
                 [::xt/put {:xt/id :person-2
                            :person/department :it-derpartment
                            :person/salary 100.0}]
                 [::xt/put {:xt/id :it-derpartment
                            :department/name "IT deparment"
                            :department/domain ["programming" "architecture desing"]}]])

  ;; Salary spending by department, domain not projected, but unified
  (xt/q  (xt/db node)
         '{:find [?dept (sum ?salary)]
           :where [[?e :person/department ?d]
                   [?e :person/salary     ?salary]
                   [?d :department/name   ?dept]
                   [?d :department/domain ?domain]]})
  ;; => #{["IT deparment" 400.0]}

  ;; domain not unifed
  (xt/q  (xt/db node)
         '{:find [?dept (sum ?salary)]
           :where [[?e :person/department ?d]
                   [?e :person/salary     ?salary]
                   [?d :department/name   ?dept]]})
  ;; => #{["IT deparment" 200.0]}

  )
