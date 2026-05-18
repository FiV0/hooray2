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

(defn delta-var-order [canonical-order pattern-var-order]
  (into pattern-var-order (remove (set pattern-var-order) canonical-order)))

(defn pattern-analysis [canonical-order patterns]
  (let [children (for [[type child-patterns :as pattern] patterns]
                   (case type
                     (:triple)
                     {:op type
                      :delta-var-order (delta-var-order canonical-order (query/variable-order pattern))}

                     (:or)
                     {:op type
                      :children (pattern-analysis canonical-order child-patterns)
                      :delta-var-order (delta-var-order canonical-order (query/variable-order pattern))}))
        child-orders (into #{} (map :delta-var-order children))]
    (mapv (fn [{:keys [delta-var-order] :as child}]
            (assoc child :requested-var-orders (vec (disj child-orders delta-var-order))))
          children)))

(defn query-analysis [{:keys [where] :as query}]
  (let [canonical-order (query/variable-order* where)]
    {:op :n-ary-join
     :children (pattern-analysis canonical-order where)
     :canonical-var-order canonical-order}))


(comment

  (def query '{:find [a b c]
               :where [[a :x b]
                       (or [b :y c]
                           [b :z c])]})

  (def query '{:find [a b c]
               :where [[a :x b]
                       (or [b :y c]
                           [b :z c])]})


  (query-analysis (s/conform ::query/query query) )

  )
