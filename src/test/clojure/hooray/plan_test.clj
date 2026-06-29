(ns hooray.plan-test
  (:require [clojure.test :refer [deftest is testing]]
            [hooray.plan :as plan]))

(defn- extender [id]
  {:type :extender
   :id id})

(defn- or-node [id variables levels branch-count]
  {:type :or
   :id id
   :variables variables
   :levels levels
   :branches (mapv (fn [branch-id]
                     [(extender [id branch-id])])
                   (range branch-count))})

(deftest no-or-plan-test
  (testing "ordinary generic joins remain one join phase"
    (let [phases (plan/plan [(extender :outer)] 3)]
      (is (= [:join] (mapv :type phases)))
      (is (= [0 2 [0 1 2]] ((juxt :start :end :levels) (first phases)))))))

(deftest join-fork-join-plan-test
  (testing "a single OR component is surrounded by ordinary joins"
    (let [phases (plan/plan [(extender :outer)
                             (or-node :or-b '#{b} [1] 2)]
                            3)]
      (is (= [:join :fork :join] (mapv :type phases)))
      (is (= [0 0 [0]] ((juxt :start :end :levels) (first phases))))
      (is (= [1 1 '#{b} 2 [1]]
             ((juxt :start :end :variables #(count (:children %)) #(get-in % [:children 0 :phases 0 :levels]))
              (second phases))))
      (is (= [2 2 [2]] ((juxt :start :end :levels) (nth phases 2)))))))

(deftest same-variable-ors-plan-as-one-branch-product-fork-test
  (testing "multiple OR components introduced at the same level fork together"
    (let [phases (plan/plan [(or-node :or-1 '#{b} [1] 2)
                             (or-node :or-2 '#{b} [1] 3)]
                            3)
          fork (second phases)]
      (is (= [:join :fork :join] (mapv :type phases)))
      (is (= [1 1] ((juxt :start :end) fork)))
      (is (= 6 (count (:children fork)))))))

(deftest overlapping-ors-plan-as-one-connected-component-test
  (testing "staggered overlapping ORs become one connected component fork"
    (let [phases (plan/plan [(extender :outer)
                             (or-node :or-1 '#{b d} [1 3] 2)
                             (or-node :or-2 '#{c d} [2 3] 2)]
                            5)
          fork (second phases)]
      (is (= [:join :fork :join] (mapv :type phases)))
      (is (= [1 3 '#{b c d} 4]
             ((juxt :start :end :variables #(count (:children %))) fork))))))

(deftest overlapping-ors-use-latest-component-end-test
  (testing "a later overlapping OR can extend the connected component"
    (let [phases (plan/plan [(extender :outer)
                             (or-node :or-1 '#{b d} [1 3] 2)
                             (or-node :or-2 '#{c d e} [2 3 4] 2)]
                            6)
          fork (second phases)]
      (is (= [:join :fork :join] (mapv :type phases)))
      (is (= [1 4 '#{b c d e} 4 [1 2 3 4]]
             ((juxt :start :end :variables #(count (:children %)) #(get-in % [:children 0 :phases 0 :levels]))
              fork))))))

(deftest non-contiguous-or-vars-stay-in-one-fork-test
  (testing "non-contiguous OR variables keep branch identity through the whole component span"
    (let [phases (plan/plan [(extender :outer)
                             (or-node :or-1 '#{b d} [1 3] 2)]
                            5)
          fork (second phases)]
      (is (= [:join :fork :join] (mapv :type phases)))
      (is (= [1 3 '#{b d} 2 [1 2 3]]
             ((juxt :start :end :variables #(count (:children %)) #(get-in % [:children 0 :phases 0 :levels]))
              fork))))))
