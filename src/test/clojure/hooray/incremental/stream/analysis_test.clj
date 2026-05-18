(ns hooray.incremental.stream.analysis-test
  (:require [clojure.spec.alpha :as s]
            [clojure.test :refer [deftest is]]
            [hooray.incremental.stream.analysis :as analysis]
            [hooray.query :as query]))

(deftest query-analysis-test
  (let [query (s/conform ::query/query
                         '{:find [first-name last-name]
                           :where [[e :first-name first-name]
                                   [e :last-name last-name]]})]
    (is (= '{:op :n-ary-join
             :canonical-var-order [e first-name last-name]
             :children [{:op :triple
                         :delta-var-order [e first-name last-name]
                         :requested-var-orders [[e last-name first-name]]}
                        {:op :triple
                         :delta-var-order [e last-name first-name]
                         :requested-var-orders [[e first-name last-name]]}]}
           (analysis/query-analysis query)))))

(deftest query-analysis-or-test
  (let [query (s/conform ::query/query
                         '{:find [a b c]
                           :where [[a :x b]
                                   (or [b :y c]
                                       [b :z c])]})]
    (is (= '{:op :n-ary-join,
             :children
             [{:op :triple,
               :delta-var-order [a b c],
               :requested-var-orders [[b c a]]}
              {:op :or,
               :children
               [{:op :triple, :delta-var-order [b c], :requested-var-orders []}
                {:op :triple, :delta-var-order [b c], :requested-var-orders []}],
               :delta-var-order [b c a],
               :requested-var-orders [[a b c]]}],
             :canonical-var-order [a b c]}
           (analysis/query-analysis query)))))

(deftest query-analysis-or+and-test
  (let [query (s/conform ::query/query
                         '{:find [?e]
                           :where [[?e :x ?a]
                                   (or (and [?e :y ?b]
                                            [?e :yy ?c])
                                       (and [?e :z ?b]
                                            [?e :zz ?c]))]})]

    (is (= '{:op :n-ary-join
             :canonical-var-order [?e ?a ?b ?c],
             :children
             [{:op :triple,
               :delta-var-order [?e ?a ?b ?c],
               :requested-var-orders [[?e ?b ?c ?a]]}
              {:op :or,
               :delta-var-order [?e ?b ?c ?a],
               :requested-var-orders [[?e ?a ?b ?c]]
               :children
               [{:op :and,
                 :delta-var-order [?e ?b ?c],
                 :requested-var-orders []
                 :children
                 [{:op :triple,
                   :delta-var-order [?e ?b ?c],
                   :requested-var-orders [[?e ?c ?b]]}
                  {:op :triple,
                   :delta-var-order [?e ?c ?b],
                   :requested-var-orders [[?e ?b ?c]]}]}
                {:op :and,
                 :delta-var-order [?e ?b ?c],
                 :requested-var-orders []
                 :children
                 [{:op :triple,
                   :delta-var-order [?e ?b ?c],
                   :requested-var-orders [[?e ?c ?b]]}
                  {:op :triple,
                   :delta-var-order [?e ?c ?b],
                   :requested-var-orders [[?e ?b ?c]]}]}]}]}
           (analysis/query-analysis query)))))

(comment
  '(or (and [?e :y ?b]
            [?e :yy ?c])
       (and [?e :z ?b]
            [?e :zz ?c]))

  ;; canonical order
  '[?e ?a ?b ?c]


  )
