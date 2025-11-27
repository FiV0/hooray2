(ns hooray.iterator-test
  (:require [clojure.test :as t :refer [deftest]]
            [clojure.data.avl :as avl]
            [me.tonsky.persistent-sorted-set :as btree-set]
            [hooray.util.persistent-map :as btree-map])
  (:import (org.hooray.iterator
            AVLLeapfrogIndex AVLPrefixExtender BTreeLeapfrogIndex BTreePrefixExtender GenericPrefixExtender
            GenericPrefixExtenderOr AVLPrefixExtenderOr
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

        (t/is (= [4 8 12] (.intersect extender [3] [4 8 12])))
        (t/is (= [8] (.intersect extender [3] [8])))
        (t/is (= [] (.intersect extender [3] [5 7 9])))
        (t/is (= [8] (.intersect extender [3] [5 8 9])))

        (t/is (= [16 20 24] (.intersect extender [6] [16 20 24])))
        (t/is (= [20] (.intersect extender [6] [20])))
        (t/is (= [] (.intersect extender [6] [])))
        (t/is (= [20] (.intersect extender [6] [19 20 21])))

        (t/is (= [] (.intersect extender [4] [19 20 21])))))))

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

      (t/is (= [4 8 12] (.intersect extender [3] [4 8 12])))
      (t/is (= [8] (.intersect extender [3] [8])))
      (t/is (= [] (.intersect extender [3] [5 7 9])))
      (t/is (= [8] (.intersect extender [3] [5 8 9])))

      (t/is (= [16 20 24] (.intersect extender [6] [16 20 24])))
      (t/is (= [20] (.intersect extender [6] [20])))
      (t/is (= [] (.intersect extender [6] [])))
      (t/is (= [20] (.intersect extender [6] [19 20 21])))

      (t/is (= [] (.intersect extender [4] [19 20 21]))))))

(deftest generic-prefix-extender-or-test
  (doseq [[map-fn set-fn implementation]
          [[hash-map hash-set "Standard Clojure Map/Set"]
           [avl/sorted-map avl/sorted-set "AVL Tree Map/Set"]]]

    (t/testing (str implementation " - GenericPrefixExtenderOr")
      ;; Test OR of two set indices
      (let [index1 (set-fn 4 8 12)
            index2 (set-fn 6 12 18)
            extender1 (GenericPrefixExtender. (SealedIndex$SetIndex. index1) [0])
            extender2 (GenericPrefixExtender. (SealedIndex$SetIndex. index2) [0])
            or-extender (GenericPrefixExtenderOr. [extender1 extender2])]

        ;; count should sum both children
        (t/is (= 6 (.count or-extender [])))

        ;; propose should return union of both sets
        (t/is (= #{4 6 8 12 18} (set (.propose or-extender []))))

        ;; extend should return union of extensions from both children
        (t/is (= #{4 8 12} (set (.intersect or-extender [] [4 8 12]))))
        (t/is (= #{6 12 18} (set (.intersect or-extender [] [6 12 18]))))
        (t/is (= #{4 6 8 12 18} (set (.intersect or-extender [] [4 6 8 12 18]))))
        (t/is (= #{12} (set (.intersect or-extender [] [12]))))
        (t/is (= [] (.intersect or-extender [] [1 2 3]))))

      ;; Test OR of two map indices (two levels)
      (let [index1 (map-fn 3 (set-fn 4 8 12) 6 (set-fn 16 20))
            index2 (map-fn 3 (set-fn 6 10) 9 (set-fn 24 28))
            extender1 (GenericPrefixExtender. (SealedIndex$MapIndex. index1) [0 1])
            extender2 (GenericPrefixExtender. (SealedIndex$MapIndex. index2) [0 1])
            or-extender (GenericPrefixExtenderOr. [extender1 extender2])]

        ;; count at level 0: index1 has {3, 6}, index2 has {3, 9} -> 4 total
        (t/is (= 4 (.count or-extender [])))

        ;; propose at level 0 should return {3, 6, 9}
        (t/is (= #{3 6 9} (set (.propose or-extender []))))

        ;; count at level 1 for prefix [3]: index1 has {4, 8, 12}, index2 has {6, 10} -> 5 total
        (t/is (= 5 (.count or-extender [3])))

        ;; propose at level 1 for prefix [3]
        (t/is (= #{4 6 8 10 12} (set (.propose or-extender [3]))))

        ;; propose at level 1 for prefix [6]
        (t/is (= #{16 20} (set (.propose or-extender [6]))))

        ;; propose at level 1 for prefix [9]
        (t/is (= #{24 28} (set (.propose or-extender [9]))))

        ;; extend at level 1 for prefix [3]
        (t/is (= #{4 8 12} (set (.intersect or-extender [3] [4 8 12]))))
        (t/is (= #{6 10} (set (.intersect or-extender [3] [6 10]))))
        (t/is (= #{4 6 8 10 12} (set (.intersect or-extender [3] [4 6 8 10 12]))))

        ;; intersect at level 1 for prefix [6]
        (t/is (= #{16 20} (set (.intersect or-extender [6] [16 20]))))

        ;; intersect at level 1 for prefix [9]
        (t/is (= #{24 28} (set (.intersect or-extender [9] [24 28])))))

      ;; Test OR of three extenders
      (let [index1 (set-fn 1 2)
            index2 (set-fn 3 4)
            index3 (set-fn 5 6)
            extender1 (GenericPrefixExtender. (SealedIndex$SetIndex. index1) [0])
            extender2 (GenericPrefixExtender. (SealedIndex$SetIndex. index2) [0])
            extender3 (GenericPrefixExtender. (SealedIndex$SetIndex. index3) [0])
            or-extender (GenericPrefixExtenderOr. [extender1 extender2 extender3])]

        (t/is (= 6 (.count or-extender [])))
        (t/is (= #{1 2 3 4 5 6} (set (.propose or-extender []))))
        (t/is (= #{2 4 6} (set (.intersect or-extender [] [2 4 6]))))))))

(deftest avl-prefix-extender-or-test
  (t/testing "AVLPrefixExtenderOr"
    ;; Test OR of two AVL set indices
    (let [index1 (avl/sorted-set 4 8 12)
          index2 (avl/sorted-set 6 12 18)
          extender1 (AVLPrefixExtender. (AVLIndex$AVLSetIndex. index1) [0])
          extender2 (AVLPrefixExtender. (AVLIndex$AVLSetIndex. index2) [0])
          or-extender (AVLPrefixExtenderOr. [extender1 extender2])]

      ;; count should sum both children
      (t/is (= 6 (.count or-extender [])))

      ;; propose should return merged sorted list from both sets
      (t/is (= [4 6 8 12 12 18] (.propose or-extender [])))

      ;; extend should return merged sorted extensions from both children
      (t/is (= [4 8 12 12] (.intersect or-extender [] [4 8 12])))
      (t/is (= [6 12 12 18] (.intersect or-extender [] [6 12 18])))
      (t/is (= [4 6 8 12 12 18] (.intersect or-extender [] [4 6 8 12 18])))
      (t/is (= [12 12] (.intersect or-extender [] [12])))
      (t/is (= [] (.intersect or-extender [] [1 2 3]))))

    ;; Test OR of two AVL map indices (two levels)
    (let [index1 (avl/sorted-map 3 (avl/sorted-set 4 8 12) 6 (avl/sorted-set 16 20))
          index2 (avl/sorted-map 3 (avl/sorted-set 6 10) 9 (avl/sorted-set 24 28))
          extender1 (AVLPrefixExtender. (AVLIndex$AVLMapIndex. index1) [0 1])
          extender2 (AVLPrefixExtender. (AVLIndex$AVLMapIndex. index2) [0 1])
          or-extender (AVLPrefixExtenderOr. [extender1 extender2])]

      ;; count at level 0: index1 has {3, 6}, index2 has {3, 9} -> 4 total
      (t/is (= 4 (.count or-extender [])))

      ;; propose at level 0 should return sorted [3, 3, 6, 9]
      (t/is (= [3 3 6 9] (.propose or-extender [])))

      ;; count at level 1 for prefix [3]: index1 has {4, 8, 12}, index2 has {6, 10} -> 5 total
      (t/is (= 5 (.count or-extender [3])))

      ;; propose at level 1 for prefix [3] - sorted merge
      (t/is (= [4 6 8 10 12] (.propose or-extender [3])))

      ;; propose at level 1 for prefix [6]
      (t/is (= [16 20] (.propose or-extender [6])))

      ;; propose at level 1 for prefix [9]
      (t/is (= [24 28] (.propose or-extender [9])))

      ;; extend at level 1 for prefix [3]
      (t/is (= [4 8 12] (.intersect or-extender [3] [4 8 12])))
      (t/is (= [6 10] (.intersect or-extender [3] [6 10])))
      (t/is (= [4 6 8 10 12] (.intersect or-extender [3] [4 6 8 10 12])))

      ;; intersect at level 1 for prefix [6]
      (t/is (= [16 20] (.intersect or-extender [6] [16 20])))

      ;; intersect at level 1 for prefix [9]
      (t/is (= [24 28] (.intersect or-extender [9] [24 28]))))

    ;; Test OR of three AVL extenders
    (let [index1 (avl/sorted-set 1 2)
          index2 (avl/sorted-set 3 4)
          index3 (avl/sorted-set 5 6)
          extender1 (AVLPrefixExtender. (AVLIndex$AVLSetIndex. index1) [0])
          extender2 (AVLPrefixExtender. (AVLIndex$AVLSetIndex. index2) [0])
          extender3 (AVLPrefixExtender. (AVLIndex$AVLSetIndex. index3) [0])
          or-extender (AVLPrefixExtenderOr. [extender1 extender2 extender3])]

      (t/is (= 6 (.count or-extender [])))
      (t/is (= [1 2 3 4 5 6] (.propose or-extender [])))
      (t/is (= [2 4 6] (.intersect or-extender [] [2 4 6]))))

    ;; Test with overlapping values to verify merge behavior
    (let [index1 (avl/sorted-set 1 3 5 7)
          index2 (avl/sorted-set 2 3 6 7)
          extender1 (AVLPrefixExtender. (AVLIndex$AVLSetIndex. index1) [0])
          extender2 (AVLPrefixExtender. (AVLIndex$AVLSetIndex. index2) [0])
          or-extender (AVLPrefixExtenderOr. [extender1 extender2])]

      ;; Should include duplicates in sorted order: [1, 2, 3, 3, 5, 6, 7, 7]
      (t/is (= [1 2 3 3 5 6 7 7] (.propose or-extender [])))
      (t/is (= [3 3 7 7] (.intersect or-extender [] [3 7]))))))
