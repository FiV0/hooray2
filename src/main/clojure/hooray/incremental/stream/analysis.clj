(ns hooray.incremental.stream.analysis
  (:require [clojure.spec.alpha :as s]
            [hooray.query :as query]))

{:op :n-ary-join
 :children []
 :canonical-var-order []}

{:op :triple
 ;; delta var order is not really necessary for a triple pattern as any canonical order
 ;; can be served
 :delta-var-order []
 :requested-var-orders [[]]}

{:op :or
 :children []
 :delta-var-order []
 :requested-var-orders [[]]}

;; not-pattern is usually an anti-join in the static query path
;; but here we need

{:op :not-pattern
 :children []
 :delta-var-order []
 :requested-var-orders [[]]}

{:op :and-pattern
 :children []
 :delta-var-order []
 :requested-var-orders [[]]}

;; both of these dont' have any child patterns
;; it's purely changes to input -> changes to outputs

{:op :predicate-pattern
 :delta-var-order []
 :requested-var-orders [[]]}
{:op :fn-pattern
 :delta-var-order []
 :requested-var-orders [[]]}

(defn query-analysis [{:keys [where] :as query}]
  (for [[type _ :as pattern] where]
    {:op type
     :delta-var-order (vec (query/variable-order pattern))}))


(comment

  (def query '{:find [a b c]
               :where [[a :x b]
                       (or [b :y c]
                           [b :z c])]})

  (query-analysis (s/conform ::query/query query) )

  )
