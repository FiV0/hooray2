(ns hooray.plan
  (:require
   [clojure.set :as set]
   [hooray.error :as err]
   [hooray.util :as util])
  (:import
   (org.hooray UniversalComparator)
   (org.hooray.engine
    BindingSet
    ExecPattern
    FunctionPattern
    NotPattern
    OrPattern
    PatternValue$Constant
    PatternValue$Variable
    PredicatePattern
    RelationPattern
    Stage
    TriplePattern)))

(defn- distinctv [values]
  (vec (distinct values)))

(defonce ^:private next-pattern-index (atom 0))

(defn- next-index! []
  (swap! next-pattern-index inc))

;; variables is similar to orderedVariables in Kotlin
;; groundable returns the variables immediately groundable from the supplied bounds
(defrecord Pattern [idx variables groundable exec-pattern])

(defn- variable-names [values]
  (->> values
       (keep (fn [[value-type value]]
               (when (= :variable value-type)
                 value)))
       distinctv))

(defn- node-index [{{:keys [idx]} :pattern :as _node}]
  idx)

(defn- node-variables [{{:keys [variables]} :pattern :as _node}]
  variables)

(defn- node-exec-pattern [{{:keys [exec-pattern]} :pattern :as _node}]
  exec-pattern)

(defn- groundable-variables
  [{{groundable-fn :groundable} :pattern :as _node} bound]
  (vec (groundable-fn (set bound))))

(defn- pattern-value [[value-type value]]
  (case value-type
    :constant (PatternValue$Constant. value)
    :variable (PatternValue$Variable. value)))

(defn- kotlin-function [fn-symbol arity]
  (let [f (util/resolve-fn fn-symbol)]
    (case arity
      1 (util/->function (fn [arg] (f arg)))
      2 (util/->bifunction (fn [left right] (f left right)))
      (throw (IllegalArgumentException.
              "Hooray only supports unary and binary query functions for now.")))))

(defn- external-binding-pattern [idx variables]
  (let [variables (vec variables)
        variable-set (set variables)]
    (reify ExecPattern
      (getIdx [_] idx)
      (getVariables [_] variable-set)
      (count [_ _input _added proposals] proposals)
      (join [_ input added target-variables]
        (if (empty? added)
          input
          (BindingSet. target-variables []))))))

(defn- external-stage? [^Stage stage external-index]
  (let [participants (.getParticipants stage)]
    (and (= 1 (count participants))
         (= external-index
            (.getIdx ^ExecPattern (first participants))))))

(defn- branch-groundable [stages external-index bound]
  (let [bound-set (set bound)]
    (loop [[stage & remaining] stages
           grounded #{}]
      (if stage
        (let [added (set (.getAdded ^Stage stage))
              external? (external-stage? stage external-index)]
          (if (and external? (not (set/subset? added bound-set)))
            grounded
            (recur remaining
                   (if external?
                     grounded
                     (into grounded added)))))
        grounded))))

(defn- or-groundable [branch-stages external-index variables bound]
  (let [bound-set (set bound)
        common (->> branch-stages
                    (map #(branch-groundable % external-index bound-set))
                    (reduce set/intersection))]
    (->> variables
         (remove bound-set)
         (filter common)
         vec)))

(defn- relation-groundable [variables bound]
  (let [bound-set (set bound)
        bound-variables (vec (filter bound-set variables))]
    (if (= bound-variables (vec (take (count bound-variables) variables)))
      (if-let [variable (nth variables (count bound-variables) nil)]
        [variable]
        [])
      [])))

(declare compile-node plan-scope)

(defn- compile-scope [db clauses]
  (let [nodes (mapv #(compile-node db %) clauses)]
    {:nodes nodes
     :ordered-vars (distinctv (mapcat node-variables nodes))}))

(defn- compile-triple [idx {:keys [e a v] :as _triple} {:keys [aev ave] :as _db}]
  (let [[attribute-type attribute] a]
    (when-not (= :constant attribute-type)
      (err/unsupported-ex "Currently variables in attribute position are not supported"))
    (let [variables (variable-names [e v])
          exec-pattern (TriplePattern. idx aev ave (pattern-value e) attribute (pattern-value v))]
      {:kind :triple
       :pattern (->Pattern idx
                           variables
                           (fn [bound]
                             (vec (remove bound variables)))
                           exec-pattern)})))

(defn- compile-predicate [idx {:keys [predicate args] :as _predicate}]
  (let [variables (variable-names args)
        arguments (mapv pattern-value args)
        exec-pattern (PredicatePattern. idx arguments (kotlin-function predicate (count arguments)))]
    {:kind :predicate
     :pattern (->Pattern idx variables (constantly []) exec-pattern)}))

(defn- compile-function [idx [{:keys [fun args] :as _function} output]]
  (let [argument-vars (variable-names args)
        variables (conj argument-vars output)
        arguments (mapv pattern-value args)
        exec-pattern (FunctionPattern. idx arguments output (kotlin-function fun (count arguments)))]
    {:kind :function
     :pattern (->Pattern idx
                         variables
                         (fn [bound]
                           (if (and (set/subset? (set argument-vars) bound)
                                    (not (contains? bound output)))
                             [output]
                             []))
                         exec-pattern)}))

(defn- compile-or [db idx branches]
  (let [compiled-branches (mapv (fn [[branch-type branch]]
                                  (if (= :and branch-type)
                                    (compile-scope db branch)
                                    (compile-scope db [[branch-type branch]])))
                                branches)
        {:keys [ordered-vars] :as _first-branch} (first compiled-branches)
        seed-index (next-index!)
        seed-pattern (external-binding-pattern seed-index ordered-vars)
        branch-stages (mapv (fn [{:keys [nodes] :as _branch}]
                              (plan-scope nodes ordered-vars seed-pattern))
                            compiled-branches)
        exec-pattern (OrPattern. idx branch-stages)]
    {:kind :or
     :pattern (->Pattern idx
                         ordered-vars
                         (partial or-groundable branch-stages seed-index ordered-vars)
                         exec-pattern)}))

(defn- compile-not [db idx clauses]
  (let [{:keys [nodes ordered-vars] :as _scope} (compile-scope db clauses)
        seed-pattern (external-binding-pattern (next-index!) ordered-vars)
        exec-pattern (NotPattern. idx (plan-scope nodes ordered-vars seed-pattern))]
    {:kind :not
     :pattern (->Pattern idx ordered-vars (constantly []) exec-pattern)}))

(defn- compile-node [db [clause-type clause]]
  (let [idx (next-index!)]
    (case clause-type
      :triple (compile-triple idx clause db)
      :predicate (compile-predicate idx clause)
      :fn (compile-function idx clause)
      :or (compile-or db idx clause)
      :not (compile-not db idx clause))))

(defn- binding-relation [binding-type binding argument]
  (case binding-type
    :scale-binding [[binding] [[argument]]]
    :collection-binding [[binding] (mapv vector (sort UniversalComparator/INSTANCE argument))]
    :tuple-binding (do
                     (when-not (= (count binding) (count argument))
                       (throw (IllegalArgumentException.
                               (format ":tuple %s and args %s must have same length!"
                                       (pr-str binding)
                                       (pr-str argument)))))
                     [binding [(vec argument)]])
    :relation-binding (let [variables (first binding)
                            rows (mapv vec argument)]
                        [variables rows])))

(defn- compile-inputs [in args]
  (when-not (= (count in) (count args))
    (throw (IllegalArgumentException. (format ":in %s and :args %s" (pr-str in) (pr-str args)))))
  (mapv (fn [[[binding-type binding] argument]]
          (let [idx (next-index!)
                [variables rows] (binding-relation binding-type binding argument)
                variables (vec variables)
                exec-pattern (RelationPattern. idx (BindingSet. variables rows))]
            {:kind :relation
             :pattern (->Pattern idx
                                 variables
                                 (partial relation-groundable variables)
                                 exec-pattern)}))
        (map vector in args)))

(defn- bound-target [variable-order bound added]
  (let [target-set (into (set bound) added)]
    (vec (filter target-set variable-order))))

(defn- ordinary-can-propose? [{:keys [kind] :as node} bound variable]
  (and (not= :or kind)
       (some #{variable} (groundable-variables node bound))))

(defn- or-can-propose? [{:keys [kind] :as node} bound]
  (when (= :or kind)
    (let [missing (vec (remove (set bound) (node-variables node)))
          groundable (set (groundable-variables node bound))]
      (when (and (seq missing) (set/subset? (set missing) groundable))
        missing))))

(defn- fully-validatable? [node bound]
  (set/subset? (set (node-variables node)) (set bound)))

(defn- pending-validation-nodes [nodes completed bound]
  (->> nodes
       (remove #(contains? completed (node-index %)))
       (filter #(fully-validatable? % bound))
       vec))

(defn- proposing-stage-for
  [nodes completed bound variable variable-order external-pattern]
  (let [added [variable]
        target (bound-target variable-order bound added)
        eligible-nodes (remove #(contains? completed (node-index %)) nodes)
        proposers (filterv #(ordinary-can-propose? % bound variable) eligible-nodes)]
    (cond
      (seq proposers)
      (let [participants (->> eligible-nodes
                              (filter (fn [node]
                                        (or (ordinary-can-propose? node bound variable)
                                            (fully-validatable? node target))))
                              (mapv node-exec-pattern))
            participant-ids (set (map (fn [^ExecPattern pattern]
                                        (.getIdx pattern))
                                      participants))]
        {:stage (Stage. added participants target)
         :completed (->> eligible-nodes
                         (filter #(and (contains? participant-ids (node-index %))
                                       (fully-validatable? % target)))
                         (map node-index)
                         set)})

      :else
      (if-let [or-node (first (filter #(let [missing (or-can-propose? % bound)]
                                         (and missing (some #{variable} missing)))
                                      eligible-nodes))]
        (let [or-added (or-can-propose? or-node bound)
              or-target (bound-target variable-order bound or-added)]
          {:stage (Stage. or-added [(node-exec-pattern or-node)] or-target)
           :completed #{(node-index or-node)}})
        (when external-pattern
          {:stage (Stage. added [external-pattern] target)
           :completed #{}})))))

(defn- add-validation-stage [nodes completed bound stages]
  (let [validation-nodes (pending-validation-nodes nodes completed bound)]
    (if (empty? validation-nodes)
      [completed stages]
      [(into completed (map node-index validation-nodes))
       (conj stages
             (Stage. []
                     (mapv node-exec-pattern validation-nodes)
                     (vec bound)))])))

(defn- plan-scope [nodes variable-order external-pattern]
  (loop [bound []
         completed #{}
         stages []]
    (let [[completed stages] (add-validation-stage nodes completed bound stages)
          remaining-vars (vec (remove (set bound) variable-order))]
      (if (empty? remaining-vars)
        (do
          (when-not (= (set completed) (set (map node-index nodes)))
            (throw (IllegalStateException. "Not every pattern was lowered into a stage")))
          stages)
        (if-let [{:keys [stage] proposal-completed :completed :as _proposal}
                 (some #(proposing-stage-for nodes
                                             completed
                                             bound
                                             %
                                             variable-order
                                             external-pattern)
                       remaining-vars)]
          (recur (.getTargetVariables ^Stage stage)
                 (into completed proposal-completed)
                 (conj stages stage))
          (let [unbound (first remaining-vars)]
            (err/incorrect-ex (format "%s not bound" unbound)
                              {:unbound-var unbound :grounded (set bound)}
                              :db.error/insufficient-binding)))))))

(defn plan
  "Compiles a validated, conformed query into a vector of executable Stage values."
  [db {:keys [in where] :as _conformed-query} args variable-order]
  (let [input-nodes (compile-inputs in args)
        {:keys [nodes] :as _where-scope} (compile-scope db where)
        all-nodes (into input-nodes nodes)]
    (plan-scope all-nodes (vec variable-order) nil)))
