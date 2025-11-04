(ns hooray.query-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.spec.alpha :as s]
            [hooray.query :as query]))

(deftest variable-order-test
  (testing "no variables - all constants"
    (let [conformed-query (s/conform ::query/query
                                     '{:find [x]
                                       :where [[1 :person/name "Alice"]]})]
      (is (= [] (query/variable-order conformed-query)))))

  (testing "single variable in value position"
    (let [conformed-query (s/conform ::query/query
                                     '{:find [x]
                                       :where [[1 :person/name x]]})]
      (is (= '[x] (query/variable-order conformed-query)))))

  (testing "single variable in entity position (constant value)"
    (let [conformed-query (s/conform ::query/query
                                     '{:find [x]
                                       :where [[x :person/name "Alice"]]})]
      (is (= '[x] (query/variable-order conformed-query)))))

  (testing "two variables - entity and value positions"
    (let [conformed-query (s/conform ::query/query
                                     '{:find [x y]
                                       :where [[x :person/name y]]})]
      (is (= '[x y] (query/variable-order conformed-query)))))

  (testing "multiple where clauses with same variable"
    (let [conformed-query (s/conform ::query/query
                                     '{:find [x y z]
                                       :where [[x :person/name y]
                                               [x :person/age z]]})]
      (is (= '[x y z] (query/variable-order conformed-query)))
      (is (= 3 (count (query/variable-order conformed-query)))
          "Should have distinct variables")))

  (testing "variable in attribute position throws exception"
    (let [conformed-query (s/conform ::query/query
                                     '{:find [x y]
                                       :where [[x y "Alice"]]})]
      (is (thrown? UnsupportedOperationException
                   (query/variable-order conformed-query))
          "Should throw when variable is in attribute position"))))
