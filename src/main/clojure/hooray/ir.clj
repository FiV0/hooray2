(ns hooray.ir)

(def example-query
  '{:find [?name (pull ?e [:employee/email {:employee/department [:department/name]}]) (count ?e)]
    :where [[?e :employee/name ?name]
            [?e :employee/age ?age]
            [?e :employee/department ?dept]

            ;; Predicate
            [(>= ?age 21)]

            ;; Function
            [(str ?name " is " ?age) ?description]

            ;; or
            (or
             [?e :employee/status :status/active]
             [?e :employee/status :status/on-leave])

            ;; not
            (not [?e :employee/contractor true])]})

;; defining a node
{:type :data-pattern
 :pattern '[?e :employee/name ?name]}

{:type :or-join
 :in-bound []
 :children []}

{:type :and
 :children []}

{:type :not-join
 :in-bound []
 :children []}

'{:id 'unique-identifier
  :type :join
  :variable-order [?e ?name ?age ?dept]
  :children []
  :predicates? []}

'{:type :predicate
  :function '>=
  :args [?age 21]
  :in-vars [?age]}

'{:type :function
  :function 'str
  :args [?name " is " ?age]
  :in-vars [?name ?age]
  :out-var ?description}

'{:type :pull
  :entity-var ?e
  :pattern [:employee/email {:employee/department [:department/name]}]
  :out-var ?pull-result
  :child 'unique-child-plan}

'{:type :aggregate
  :function [{:agg-fn 'count
              :args [?e]
              :out-var ?gensym-count}
             {:agg-fn 'sum
              :args [?age]
              :out-var ?gensym-sum}]
  :child 'unique-child-plan}

'{:type :project
  :projections [?name ?pull-result ?count]
  :child 'unique-child-plan}
