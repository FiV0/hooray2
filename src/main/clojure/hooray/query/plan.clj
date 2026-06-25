(ns hooray.query.plan
  (:require [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [hooray.error :as err]
            [hooray.query :as query])
  (:import (java.util.function Function)
           (org.hooray.engine
            BindingSet
            FunctionPattern
            InputPattern
            NotPattern
            OrPattern
            PatternValue$Constant
            PatternValue$Variable
            PredicatePattern
            Stage
            StageExecutor
            TriplePattern)))

(defn- in->variables [in]
  (->> in
       (mapcat (fn [[binding-type binding]]
                 (case binding-type
                   :scale-binding [binding]
                   :collection-binding [binding]
                   :tuple-binding binding
                   :relation-binding (first binding))))
       vec))

(defn- flatten-and [patterns]
  (mapcat (fn [[clause-type clause]]
            (if (= :and clause-type)
              (flatten-and clause)
              [[clause-type clause]]))
          patterns))

(defn- descriptor-variables [pattern]
  (vec (query/variable-order pattern)))

(declare compile-patterns)

(defn- compile-pattern [index [clause-type clause :as pattern]]
  (case clause-type
    :triple {:id index
             :kind :triple
             :variables (descriptor-variables pattern)
             :raw pattern}

    :predicate {:id index
                :kind :predicate
                :variables (descriptor-variables pattern)
                :raw pattern}

    :fn (let [[_ ret-var] clause]
          {:id index
           :kind :function
           :variables (descriptor-variables pattern)
           :return-variable ret-var
           :raw pattern})

    :or (let [branches (mapv (fn [branch]
                               {:patterns (compile-patterns (flatten-and [branch]))
                                :variables (descriptor-variables branch)})
                             clause)]
          {:id index
           :kind :or
           :variables (descriptor-variables pattern)
           :branches branches
           :raw pattern})

    :not {:id index
          :kind :not
          :variables (descriptor-variables pattern)
          :patterns (compile-patterns (flatten-and clause))
          :raw pattern}))

(defn- compile-patterns [patterns]
  (->> patterns
       flatten-and
       (map-indexed compile-pattern)
       vec))

(defn- pattern-vars [pattern]
  (set (:variables pattern)))

(defn- overlaps? [introduces pattern]
  (seq (set/intersection (set introduces) (pattern-vars pattern))))

(defn- groundable [bound pattern]
  (let [vars (pattern-vars pattern)]
    (case (:kind pattern)
      (:triple :input :or) (set/difference vars bound)
      :function (if (and (not (contains? bound (:return-variable pattern)))
                         (set/subset? (disj vars (:return-variable pattern)) bound))
                  #{(:return-variable pattern)}
                  #{})
      (:predicate :not) #{})))

(defn- groundable? [bound variable pattern]
  (contains? (groundable bound pattern) variable))

(defn- non-or-grounder? [bound variable pattern]
  (and (not= :or (:kind pattern))
       (groundable? bound variable pattern)))

(defn- participant? [bound introduces pattern]
  (let [vars (pattern-vars pattern)
        new-bound (set/union bound (set introduces))
        newly-covered? (and (set/subset? vars new-bound)
                            (not (set/subset? vars bound)))]
    (case (:kind pattern)
      (:triple :input :or) (overlaps? introduces pattern)
      (:predicate :function :not) newly-covered?
      false)))

(defn- stage-target [bound introduces]
  (vec (distinct (concat bound introduces))))

(defn- stage [bound patterns introduces]
  (let [participants (filterv #(participant? bound introduces %) patterns)]
    {:introduces introduces
     :participants participants
     :target-variables (stage-target bound introduces)}))

(defn- stage-for-variable [bound patterns variable]
  (if (some #(non-or-grounder? bound variable %) patterns)
    (stage bound patterns [variable])
    (if-let [or-pattern (first (filter #(and (= :or (:kind %))
                                             (groundable? bound variable %))
                                       patterns))]
      (stage bound patterns (vec (groundable bound or-pattern)))
      (err/unsupported-ex
       "BindingSet planner cannot ground variable"
       {:variable variable :bound bound}))))

(defn- plan-stages [initial-bound variable-order patterns]
  (loop [bound (set initial-bound)
         stages []]
    (let [required-vars (set variable-order)]
      (if (set/subset? required-vars bound)
        stages
        (let [variable (first (remove bound variable-order))
              stage (stage-for-variable bound patterns variable)]
          (recur (set/union bound (set (:introduces stage)))
                 (conj stages stage)))))))

(declare append-final-branch-validation-stage)

(defn plan [conformed-query]
  (let [initial-bound (set (in->variables (:in conformed-query)))
        variable-order (query/query->variable-order conformed-query)
        patterns (compile-patterns (:where conformed-query))
        stages (plan-stages initial-bound variable-order patterns)]
    {:initial-bound initial-bound
     :patterns patterns
     :stages (append-final-branch-validation-stage stages variable-order patterns)}))

(defn- pattern-value [[value-type value]]
  (case value-type
    :constant (PatternValue$Constant. value)
    :variable (PatternValue$Variable. value)))

(declare exec-stage)

(defn- call-function [f]
  (reify Function
    (apply [_ args]
      (apply f args))))

(defn- call-predicate [f]
  (reify Function
    (apply [_ args]
      (boolean (apply f args)))))

(defn- final-branch-validation-stage [target-variables patterns]
  {:introduces []
   :participants (filterv #(set/subset? (pattern-vars %) (set target-variables))
                          patterns)
   :target-variables target-variables})

(defn- append-final-branch-validation-stage [stages target-variables patterns]
  (let [stage (final-branch-validation-stage target-variables patterns)]
    (if (seq (:participants stage))
      (conj stages stage)
      stages)))

(defn- branch-stage-maps [seed-variables patterns]
  (let [variable-order (vec (distinct (concat seed-variables (mapcat :variables patterns))))
        stages (plan-stages (set seed-variables) variable-order patterns)
        target-variables (if (seq stages)
                           (:target-variables (last stages))
                           (vec seed-variables))]
    (append-final-branch-validation-stage stages target-variables patterns)))

(defn- groundable-closure [seed-variables patterns]
  (loop [bound (set seed-variables)]
    (let [next-bound (reduce set/union bound (map #(groundable bound %) patterns))]
      (if (= bound next-bound)
        bound
        (recur next-bound)))))

(defn- branch-validation-stage-maps [seed-variables patterns]
  (let [closed-vars (groundable-closure seed-variables patterns)
        variable-order (->> (concat seed-variables (mapcat :variables patterns))
                            distinct
                            (filter closed-vars)
                            vec)
        stages (plan-stages (set seed-variables) variable-order patterns)
        target-variables (if (seq stages)
                           (:target-variables (last stages))
                           (vec seed-variables))]
    (append-final-branch-validation-stage stages target-variables patterns)))

(defn- exec-or-pattern [indexes stage idx pattern]
  (let [proposal-seed-variables (vec (remove (set (:introduces stage))
                                             (:target-variables stage)))
        validation-seed-variables (:target-variables stage)
        proposal-branches (when (set/subset? (pattern-vars pattern)
                                             (set (:target-variables stage)))
                            (try
                              (mapv (fn [branch]
                                      (mapv (partial exec-stage indexes)
                                            (branch-stage-maps proposal-seed-variables
                                                               (:patterns branch))))
                                    (:branches pattern))
                              (catch clojure.lang.ExceptionInfo _ nil)))
        validation-branches (mapv (fn [branch]
                                    (mapv (partial exec-stage indexes)
                                          (branch-validation-stage-maps validation-seed-variables
                                                                        (:patterns branch))))
                                  (:branches pattern))]
    (OrPattern. idx
                (set (:variables pattern))
                (or proposal-branches [])
                validation-branches
                (boolean proposal-branches))))

(defn- exec-not-pattern [indexes stage idx pattern]
  (let [seed-variables (:target-variables stage)
        branch (mapv (partial exec-stage indexes)
                     (branch-stage-maps seed-variables (:patterns pattern)))]
    (NotPattern. idx (set (:variables pattern)) branch)))

(defn- exec-pattern [indexes stage idx pattern]
  (let [{:keys [kind raw]} pattern]
    (case kind
      :triple (let [[_ {:keys [e a v]}] raw]
                (TriplePattern. idx
                                (:eav indexes)
                                (:aev indexes)
                                (:ave indexes)
                                (pattern-value e)
                                (pattern-value a)
                                (pattern-value v)))

      :predicate (let [[_ {:keys [predicate args]}] raw]
                   (PredicatePattern. idx
                                      (mapv pattern-value args)
                                      (call-predicate (query/resolve-fn predicate))))

      :function (let [[_ [{:keys [fun args]} ret-var]] raw]
                  (FunctionPattern. idx
                                    (mapv pattern-value args)
                                    ret-var
                                    (call-function (query/resolve-fn fun))))

      :or (exec-or-pattern indexes stage idx pattern)

      :not (exec-not-pattern indexes stage idx pattern)

      (err/unsupported-ex
       "BindingSet internal query path does not support this pattern yet"
       {:kind kind :pattern pattern}))))

(defn- exec-stage [indexes {:keys [introduces participants target-variables] :as stage}]
  (Stage. introduces
          (mapv (fn [idx pattern]
                  (exec-pattern indexes stage idx pattern))
                (range)
                participants)
          target-variables))

(defn- input-relation [[binding-type binding] arg]
  (case binding-type
    :scale-binding {:variables [binding]
                    :rows [[arg]]}

    :collection-binding {:variables [binding]
                         :rows (mapv vector arg)}

    :tuple-binding (do
                     (when-not (= (count binding) (count arg))
                       (throw (IllegalArgumentException.
                               (format ":tuple %s and args %s must have same length!"
                                       (pr-str binding)
                                       (pr-str arg)))))
                     {:variables binding
                      :rows [(vec arg)]})

    :relation-binding (let [variables (first binding)
                            rows (mapv vec arg)]
                        (doseq [row rows]
                          (when-not (= (count variables) (count row))
                            (throw (IllegalArgumentException.
                                    (format ":relation tuple %s and binding %s must have same length!"
                                            (pr-str row)
                                            (pr-str variables))))))
                        {:variables variables
                         :rows rows})))

(defn- apply-input-relation [^BindingSet bindings {:keys [variables rows]}]
  (let [bound-vars (vec (.getVariables bindings))
        introduces (vec (remove (set bound-vars) variables))
        target-vars (vec (distinct (concat bound-vars variables)))
        pattern (InputPattern/relation 0 variables rows)]
    (if (seq introduces)
      (.propose pattern bindings introduces target-vars)
      (.validate pattern bindings))))

(defn- initial-bindings [conformed-query args]
  (let [in (:in conformed-query)]
    (when-not (= (count in) (count args))
      (throw (IllegalArgumentException.
              (format ":in %s and :args %s" (pr-str in) (pr-str args)))))
    (reduce apply-input-relation
            (BindingSet. [] [[]])
            (map input-relation in args))))

(defn execute-conformed-query [db conformed-query args]
  (let [planned (plan conformed-query)
        indexes {:eav (:eav db)
                 :aev (:aev db)
                 :ave (:ave db)}
        executor (StageExecutor.)
        result (reduce (fn [bindings stage]
                         (.execute executor (exec-stage indexes stage) bindings))
                       (initial-bindings conformed-query args)
                       (:stages planned))]
    (query/shape-results (.getRows result) conformed-query (.getVariables result))))

(defn execute-query [db raw-query args]
  (let [conformed-query (s/conform ::query/query raw-query)]
    (when (= ::s/invalid conformed-query)
      (throw (ex-info "Invalid query" {:query raw-query})))
    (query/validate-query conformed-query)
    (execute-conformed-query db conformed-query args)))
