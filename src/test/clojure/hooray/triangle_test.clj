(ns hooray.triangle-test
  (:require [clojure.edn :as edn]
            [clojure.test :refer [deftest testing is] :as t]
            [hooray.core :as h]
            [hooray.fixtures :as fix :refer [*node*]]
            [hooray.graph-gen :as g]))

(t/use-fixtures :each fix/with-node)

(def triangle-query '{:find [?a ?b ?c]
                      :where [[?a :g/to ?b]
                              [?a :g/to ?c]
                              [?b :g/to ?c]]})

(deftest complete-graph-test
  (testing "triangle query complete graph 100"
    (h/transact *node* (g/graph->ops (g/complete-graph 100)))
    (fix/with-timing-logged*
      (is (= 161700
             (count (h/q triangle-query (h/db *node*))))))))

(deftest complete-bipartite-test
  (testing "triangle query bipartite 200"
    (h/transact *node* (g/graph->ops (g/complete-bipartite 200)))
    (fix/with-timing-logged*
      (is (= 0
             (count (h/q triangle-query (h/db *node*))))))))

(deftest star-graph-test
  (testing "triangle query star graph 1000"
    (h/transact *node* (g/graph->ops (g/star-graph 1000)))
    (fix/with-timing-logged*
      (is (= 0
             (count (h/q triangle-query (h/db *node*))))))))

(deftest star-with-ring-graph-test
  (testing "triangle query star graph with ring 1000"
    (h/transact *node* (g/graph->ops (g/star-with-ring 1000)))
    (fix/with-timing-logged*
      (is (= 998
             (count (h/q triangle-query (h/db *node*))))))))

(deftest complete-independents-test
  (testing "triangle query with complete independents 100 10"
    (h/transact *node* (g/graph->ops (g/complete-independents 100 10)))
    (fix/with-timing-logged*
      (is (= 120000
             (count (h/q triangle-query (h/db *node*))))))))

(comment
  (def random-graph (g/random-graph 300 0.3))
  (def random-independents (g/random-independents 300 10 0.3))
  (spit "src/main/resources/random-graph-100-0.3.edn" (apply list random-graph))
  (spit "src/main/resources/random-independents-graph-100-0.3.edn" (apply list random-independents)))

(def random-graph (clojure.edn/read-string (slurp "src/main/resources/random-graph-100-0.3.edn")))
(def random-independents (clojure.edn/read-string (slurp "src/main/resources/random-independents-graph-100-0.3.edn")))

(deftest random-graph-test
  (testing "triangle query with random graph 300 0.3"
    (h/transact *node* (g/graph->ops random-graph))
    (fix/with-timing-logged*
      (is (= 118266
             (count (h/q triangle-query (h/db *node*))))))))

(deftest random-independents-test
  (testing "triangle query with random independent graph 300 10 0.3"
    (h/transact *node* (g/graph->ops random-independents))
    (fix/with-timing-logged*
      (is (= 88637
             (count (h/q triangle-query (h/db *node*))))))))
