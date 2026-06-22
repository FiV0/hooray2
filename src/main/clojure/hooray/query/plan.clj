(ns hooray.query.plan
  (:require
   [clojure.spec.alpha :as s]
   [clojure.set :as set]
   [hooray.error :as err]
   [hooray.query :as query])
  (:import
   (java.util.function Function)
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
    StageKind
    TriplePattern
    ValidatorOnlyPattern)))

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

(defn- ordinary-proposer [bound patterns]
  (first
   (filter
    (fn [pattern]
      (let [vars (pattern-vars pattern)
            unbound (seq (remove bound (:variables pattern)))]
        (case (:kind pattern)
          (:triple :input) unbound
          :function (and (not (contains? bound (:return-variable pattern)))
                         (set/subset? (disj vars (:return-variable pattern)) bound))
          false)))
    patterns)))

(defn- participant? [bound introduces pattern]
  (let [new-bound (set/union bound (set introduces))]
    (and (overlaps? introduces pattern)
         (case (:kind pattern)
          (:triple :input :or) true
          (:predicate :function :not) (set/subset? (pattern-vars pattern) new-bound)
          false))))

(defn- participant [role pattern]
  {:id (:id pattern)
   :kind (:kind pattern)
   :role role
   :variables (:variables pattern)
   :pattern pattern})

(defn- stage-target [bound introduces]
  (vec (distinct (concat bound introduces))))

(defn- ordinary-stage [bound patterns proposer]
  (let [introduces (vec (remove bound (:variables proposer)))
        participants (->> patterns
                          (filter #(or (= (:id %) (:id proposer))
                                       (participant? bound introduces %)))
                          (mapv #(participant (if (= (:id %) (:id proposer))
                                                :proposer
                                                :validator)
                                              %)))]
    {:kind :ordinary
     :introduces introduces
     :participants participants
     :target-variables (stage-target bound introduces)}))

(defn- or-proposal-stage [bound patterns]
  (let [or-patterns (filter #(= :or (:kind %)) patterns)
        proposer (first (filter #(seq (remove bound (:variables %))) or-patterns))
        introduces (vec (remove bound (:variables proposer)))
        participants (->> or-patterns
                          (filter #(or (= (:id %) (:id proposer))
                                       (participant? bound introduces %)))
                          (mapv #(participant (if (= (:id %) (:id proposer))
                                                :proposer
                                                :validator)
                                              %)))]
    {:kind :or-proposal-boundary
     :introduces introduces
     :participants participants
     :target-variables (stage-target bound introduces)}))

(defn- plan-stages [initial-bound patterns]
  (loop [bound (set initial-bound)
         stages []]
    (let [all-vars (set (mapcat :variables patterns))]
      (if (set/subset? all-vars bound)
        stages
        (let [stage (if-let [proposer (ordinary-proposer bound patterns)]
                      (ordinary-stage bound patterns proposer)
                      (or-proposal-stage bound patterns))]
          (recur (set/union bound (set (:introduces stage)))
                 (conj stages stage)))))))

(defn plan [conformed-query]
  (let [initial-bound (set (in->variables (:in conformed-query)))
        patterns (compile-patterns (:where conformed-query))]
    {:initial-bound initial-bound
     :patterns patterns
     :stages (plan-stages initial-bound patterns)}))

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
  {:kind :ordinary
   :introduces []
   :participants (mapv #(participant :validator %) patterns)
   :target-variables target-variables})

(defn- branch-stage-maps [seed-variables patterns]
  (let [stages (plan-stages (set seed-variables) patterns)
        target-variables (if (seq stages)
                           (:target-variables (last stages))
                           (vec seed-variables))]
    (conj stages (final-branch-validation-stage target-variables patterns))))

(defn- exec-or-pattern [indexes stage participant pattern]
  (let [seed-variables (if (= :proposer (:role participant))
                         (vec (remove (set (:introduces stage)) (:target-variables stage)))
                         (:target-variables stage))
        branches (mapv (fn [branch]
                         (mapv (partial exec-stage indexes)
                               (branch-stage-maps seed-variables (:patterns branch))))
                       (:branches pattern))]
    (OrPattern. (set (:variables pattern))
                branches
                (= :proposer (:role participant)))))

(defn- exec-not-pattern [indexes stage pattern]
  (let [seed-variables (:target-variables stage)
        branch (mapv (partial exec-stage indexes)
                     (branch-stage-maps seed-variables (:patterns pattern)))]
    (NotPattern. (set (:variables pattern)) branch)))

(defn- role-pattern [role exec-pattern]
  (if (= :validator role)
    (ValidatorOnlyPattern. exec-pattern)
    exec-pattern))

(defn- exec-pattern [indexes stage {:keys [role pattern] :as participant}]
  (let [{:keys [kind raw]} pattern
        executable (case kind
                     :triple (let [[_ {:keys [e a v]}] raw]
                               (TriplePattern. (:eav indexes)
                                               (:aev indexes)
                                               (:ave indexes)
                                               (pattern-value e)
                                               (pattern-value a)
                                               (pattern-value v)))

                     :predicate (let [[_ {:keys [predicate args]}] raw]
                                  (PredicatePattern. (mapv pattern-value args)
                                                     (call-predicate (query/resolve-fn predicate))))

                     :function (let [[_ [{:keys [fun args]} ret-var]] raw]
                                 (FunctionPattern. (mapv pattern-value args)
                                                   ret-var
                                                   (call-function (query/resolve-fn fun))))

                     :or (exec-or-pattern indexes stage participant pattern)

                     :not (exec-not-pattern indexes stage pattern)

                     (err/unsupported-ex
                      "BindingSet internal query path does not support this pattern yet"
                      {:kind kind :pattern pattern :role role}))]
    (role-pattern role executable)))

(defn- stage-kind [kind]
  (case kind
    :ordinary StageKind/ORDINARY
    :or-proposal-boundary StageKind/OR_PROPOSAL_BOUNDARY))

(defn- exec-stage [indexes {:keys [kind introduces participants target-variables] :as stage}]
  (Stage. introduces
          (mapv (partial exec-pattern indexes stage) participants)
          target-variables
          (stage-kind kind)))

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
        pattern (InputPattern/relation variables rows)]
    (if (seq introduces)
      (.propose pattern bindings introduces target-vars)
      (.validate pattern bindings target-vars))))

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
