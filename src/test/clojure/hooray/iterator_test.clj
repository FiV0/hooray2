(ns hooray.iterator-test
  (:require [clojure.test :as t :refer [deftest]]
            [clojure.data.avl :as avl]
            [me.tonsky.persistent-sorted-set :as btree-set]
            [hooray.util.persistent-map :as btree-map])
  (:import (org.hooray.iterator
            AVLLeapfrogIndex AVLPrefixExtender BTreeLeapfrogIndex BTreePrefixExtender GenericPrefixExtender
            SealedIndex SealedIndex$MapIndex SealedIndex$SetIndex
            BTreeIndex
            AVLIndex AVLIndex$AVLMapIndex AVLIndex$AVLSetIndex)))

(deftest generic-prefix-extender-test
  (doseq [[map-fn set-fn implementation]
          [[hash-map hash-set "Standard Clojure Map/Set"]
           [avl/sorted-map avl/sorted-set "AVL Tree Map/Set"]
           ;; sorted-map is broken
           #_[btree-map/sorted-map btree-set/sorted-set "B-Tree Map/Set"]]]

    (t/testing implementation
      (let [index (set-fn 4 8 12)
            extender (GenericPrefixExtender. (SealedIndex$SetIndex. index) [0])]
        (t/is (= 3 (.count extender [])))

        (t/is (= #{4 12 8} (set (.propose extender [])))))

      ;; two levels
      (let [index (map-fn 3 (set-fn 4 8 12) 6 (set-fn 16 20 24))
            extender (GenericPrefixExtender. (SealedIndex$MapIndex. index) [0 1])]
        (t/is (= 2 (.count extender [])))
        (t/is (= 3 (.count extender [3])))

        (t/is (= #{3 6} (set (.propose extender []))))
        (t/is (= #{4 12 8} (set (.propose extender [3]))))
        (t/is (= #{20 24 16} (set (.propose extender [6]))))
        (t/is (= [] (.propose extender [4])))

        (t/is (= [4 8 12] (.extend extender [3] [4 8 12])))
        (t/is (= [8] (.extend extender [3] [8])))
        (t/is (= [] (.extend extender [3] [5 7 9])))
        (t/is (= [8] (.extend extender [3] [5 8 9])))

        (t/is (= [16 20 24] (.extend extender [6] [16 20 24])))
        (t/is (= [20] (.extend extender [6] [20])))
        (t/is (= [] (.extend extender [6] [])))
        (t/is (= [20] (.extend extender [6] [19 20 21])))

        (t/is (= [] (.extend extender [4] [19 20 21])))))))

(deftest avl-prefix-extender-test
  (t/testing "AVLPrefixExtender"
    (let [index (avl/sorted-set 4 8 12)
          extender (AVLPrefixExtender. (AVLIndex$AVLSetIndex. index) [0])]
      (t/is (= 3 (.count extender [])))

      (t/is (= #{4 12 8} (set (.propose extender [])))))

    ;; two levels
    (let [index (avl/sorted-map 3 (avl/sorted-set 4 8 12) 6 (avl/sorted-set 16 20 24))
          extender (AVLPrefixExtender. (AVLIndex$AVLMapIndex. index) [0 1])]
      (t/is (= 2 (.count extender [])))
      (t/is (= 3 (.count extender [3])))

      (t/is (= [3 6] (.propose extender [])))
      (t/is (= [4 8 12] (.propose extender [3])))
      (t/is (= [16 20 24] (.propose extender [6])))
      (t/is (= [] (.propose extender [4])))

      (t/is (= [4 8 12] (.extend extender [3] [4 8 12])))
      (t/is (= [8] (.extend extender [3] [8])))
      (t/is (= [] (.extend extender [3] [5 7 9])))
      (t/is (= [8] (.extend extender [3] [5 8 9])))

      (t/is (= [16 20 24] (.extend extender [6] [16 20 24])))
      (t/is (= [20] (.extend extender [6] [20])))
      (t/is (= [] (.extend extender [6] [])))
      (t/is (= [20] (.extend extender [6] [19 20 21])))

      (t/is (= [] (.extend extender [4] [19 20 21]))))))
