(ns hooray.staged-generic-join-test
  (:require
   [clojure.test :as t]
   [hooray.staged-generic-join :as staged-generic-join])
  (:import
   (org.hooray.algo PrefixExtender)
   (org.hooray.engine BindingSet)))

(def unit-bindings (BindingSet. [] [[]]))

(t/deftest generic-stage-replays-incoming-bindings-from-level-zero-test
  (let [scope {:input-variables ['?seed]
               :variable-order ['?seed '?value]
               :extenders [(PrefixExtender/createSingleLevel ["x"] 1)]
               :stages [{:kind :generic
                         :target-variables ['?seed '?value]}]}
        input (BindingSet. ['?seed] [[1] [2]])
        result (staged-generic-join/execute scope input)]
    (t/is (= ['?seed '?value] (.getVariables ^BindingSet result)))
    (t/is (= [[1 "x"] [2 "x"]] (.getRows ^BindingSet result)))))

(t/deftest consecutive-generic-stages-preserve-layout-and-multiplicity-test
  (let [scope {:input-variables []
               :variable-order ['?number '?label]
               :extenders [(PrefixExtender/createSingleLevel [1] 0)
                           (PrefixExtender/createSingleLevel ["x" "y"] 1)]
               :stages [{:kind :generic
                         :target-variables ['?number]}
                        {:kind :generic
                         :target-variables ['?number '?label]}]}
        result (staged-generic-join/execute scope unit-bindings)]
    (t/is (= ['?number '?label] (.getVariables ^BindingSet result)))
    (t/is (= [[1 "x"] [1 "y"]] (.getRows ^BindingSet result)))))

(t/deftest empty-input-retains-the-planned-final-layout-test
  (let [scope {:input-variables ['?seed]
               :variable-order ['?seed '?value]
               :extenders [(PrefixExtender/createSingleLevel ["x"] 1)]
               :stages [{:kind :generic
                         :target-variables ['?seed '?value]}]}
        result (staged-generic-join/execute scope (BindingSet. ['?seed] []))]
    (t/is (= ['?seed '?value] (.getVariables ^BindingSet result)))
    (t/is (= [] (.getRows ^BindingSet result)))))

(t/deftest executor-rejects-invalid-runtime-boundaries-test
  (t/is (thrown-with-msg?
         IllegalArgumentException
         #"input variables"
         (staged-generic-join/execute
          {:input-variables ['?seed]
           :variable-order ['?seed]
           :extenders []
           :stages []}
          unit-bindings)))
  (t/is (thrown-with-msg?
         IllegalStateException
         #"Unknown stage kind"
         (staged-generic-join/execute
          {:input-variables []
           :variable-order []
           :extenders []
           :stages [{:kind :unknown
                     :target-variables []}]}
          unit-bindings))))

(t/deftest proposing-or-distincts-branches-and-preserves-outer-columns-test
  (let [branch {:input-variables []
                :variable-order ['?value]
                :extenders [(PrefixExtender/createSingleLevel [1] 0)]
                :stages [{:kind :generic
                          :target-variables ['?value]}]}
        scope {:input-variables []
               :variable-order ['?tag '?value]
               :extenders [(PrefixExtender/createSingleLevel ["outer"] 0)]
               :stages [{:kind :generic
                         :target-variables ['?tag]}
                        {:kind :or
                         :variables ['?value]
                         :added ['?value]
                         :target-variables ['?tag '?value]
                         :branches [branch branch]}]}
        result (staged-generic-join/execute scope unit-bindings)]
    (t/is (= ['?tag '?value] (.getVariables ^BindingSet result)))
    (t/is (= [["outer" 1]] (.getRows ^BindingSet result)))))

(t/deftest validating-or-preserves-duplicate-outer-rows-test
  (let [scope {:input-variables []
               :variable-order ['?value '?tag]
               :extenders [(PrefixExtender/createSingleLevel [1 1] 0)
                           (PrefixExtender/createSingleLevel ["outer"] 1)]
               :stages [{:kind :generic
                         :target-variables ['?value '?tag]}
                        {:kind :or
                         :variables ['?value]
                         :added []
                         :target-variables ['?value '?tag]
                         :branches [{:input-variables ['?value]
                                     :variable-order ['?value]
                                     :extenders []
                                     :stages [{:kind :generic
                                               :target-variables ['?value]}]}]}]}
        result (staged-generic-join/execute scope unit-bindings)]
    (t/is (= ['?value '?tag] (.getVariables ^BindingSet result)))
    (t/is (= [[1 "outer"] [1 "outer"]]
             (.getRows ^BindingSet result)))))

(t/deftest not-antijoin-preserves-duplicate-nonmatching-outer-rows-test
  (let [scope {:input-variables []
               :variable-order ['?value '?tag]
               :extenders [(PrefixExtender/createSingleLevel [1 1] 0)
                           (PrefixExtender/createSingleLevel ["outer"] 1)]
               :stages [{:kind :generic
                         :target-variables ['?value '?tag]}
                        {:kind :not
                         :variables ['?value]
                         :target-variables ['?value '?tag]
                         :body {:input-variables ['?value]
                                :variable-order ['?value]
                                :extenders [(PrefixExtender/createSingleLevel [2] 0)]
                                :stages [{:kind :generic
                                          :target-variables ['?value]}]}}]}
        result (staged-generic-join/execute scope unit-bindings)]
    (t/is (= ['?value '?tag] (.getVariables ^BindingSet result)))
    (t/is (= [[1 "outer"] [1 "outer"]]
             (.getRows ^BindingSet result)))))
