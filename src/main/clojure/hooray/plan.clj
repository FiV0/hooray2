(ns hooray.plan
  (:require
   [clojure.set :as set]
   [hooray.error :as err]
   [hooray.util :as util])
  (:import
   (org.hooray UniversalComparator)
   (org.hooray.engine
    BindingSet
    FunctionPattern
    NotPattern
    OrPattern
    Pattern
    PatternValue$Constant
    PatternValue$Variable
    PredicatePattern
    RelationPattern
    Stage
    TriplePattern)))

(defn- distinctv [values]
  (vec (distinct values)))

(defn- node-variables [{:keys [pattern] :as _node}]
  (vec (.getOrderedVariables pattern)))

(defonce ^:private next-pattern-index (atom 0))

(defn- next-index! []
  (swap! next-pattern-index inc))

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
    (reify Pattern
      (getIdx [_] idx)
      (getVariables [_] variable-set)
      (getOrderedVariables [_] variables)
      (groundable [_ _bound] [])
      (count [_ _input _added proposals] proposals)
      (join [_ input added target-variables]
        (if (empty? added)
          input
          (BindingSet. target-variables []))))))

(declare compile-node plan-scope)

(defn- compile-scope [db clauses]
  (let [nodes (mapv #(compile-node db %) clauses)]
    {:nodes nodes
     :ordered-vars (distinctv (mapcat node-variables nodes))}))

(defn- compile-triple [idx {:keys [e a v] :as _triple} {:keys [aev ave] :as _db}]
  (let [[attribute-type attribute] a]
    (when-not (= :constant attribute-type)
      (err/unsupported-ex "Currently variables in attribute position are not supported"))
    (let [pattern (TriplePattern. idx aev ave (pattern-value e) attribute (pattern-value v))]
      {:idx idx
       :kind :triple
       :pattern pattern})))

(defn- compile-predicate [idx {:keys [predicate args] :as _predicate}]
  (let [arguments (mapv pattern-value args)
        pattern (PredicatePattern. idx arguments (kotlin-function predicate (count arguments)))]
    {:idx idx
     :kind :predicate
     :pattern pattern}))

(defn- compile-function [idx [{:keys [fun args] :as _function} output]]
  (let [arguments (mapv pattern-value args)
        pattern (FunctionPattern. idx arguments output (kotlin-function fun (count arguments)))]
    {:idx idx
     :kind :function
     :output output
     :argument-vars (->> arguments
                         (keep #(when (instance? PatternValue$Variable %)
                                  (.getName ^PatternValue$Variable %)))
                         vec)
     :pattern pattern}))

(defn- compile-or [db idx branches]
  (let [compiled-branches (mapv (fn [[branch-type branch]]
                                  (if (= :and branch-type)
                                    (compile-scope db branch)
                                    (compile-scope db [[branch-type branch]])))
                                branches)
        {:keys [ordered-vars] :as _first-branch} (first compiled-branches)
        seed-pattern (external-binding-pattern (next-index!) ordered-vars)
        branch-stages (mapv (fn [{:keys [nodes] :as _branch}]
                              (plan-scope nodes ordered-vars seed-pattern))
                            compiled-branches)
        pattern (OrPattern. idx branch-stages)]
    {:idx idx
     :kind :or
     :pattern pattern}))

(defn- compile-not [db idx clauses]
  (let [{:keys [nodes ordered-vars] :as _scope} (compile-scope db clauses)
        seed-pattern (external-binding-pattern (next-index!) ordered-vars)
        pattern (NotPattern. idx (plan-scope nodes ordered-vars seed-pattern))]
    {:idx idx
     :kind :not
     :pattern pattern}))

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
                pattern (RelationPattern. idx (BindingSet. (vec variables) rows))]
            {:idx idx
             :kind :relation
             :pattern pattern}))
        (map vector in args)))

(defn- bound-target [variable-order bound added]
  (let [target-set (into (set bound) added)]
    (vec (filter target-set variable-order))))

(defn- relation-can-propose? [node bound added]
  (let [relation-vars (node-variables node)
        relation-set (set relation-vars)
        bound-prefix (vec (filter (set bound) relation-vars))
        proposed-prefix (into bound-prefix added)]
    (and (seq added)
         (set/subset? (set added) relation-set)
         (= proposed-prefix (subvec relation-vars 0 (count proposed-prefix))))))

(defn- can-propose? [{:keys [kind output argument-vars] :as node} bound added]
  (let [bound-set (set bound)
        variable-set (set (node-variables node))]
    (case kind
      :triple (and (seq added) (set/subset? (set added) variable-set))
      :relation (relation-can-propose? node bound added)
      :function (and (= [output] added)
                     (set/subset? (set argument-vars) bound-set))
      false)))

(defn- or-can-propose? [{:keys [kind pattern] :as node} bound]
  (when (= :or kind)
    (let [missing (vec (remove (set bound) (node-variables node)))
          groundable (set (.groundable ^Pattern pattern (set bound)))]
      (when (and (seq missing) (set/subset? (set missing) groundable))
        missing))))

(defn- can-validate-proposing-stage?
  [{:keys [kind pattern] :as node} target]
  (let [target-set (set target)
        variables (node-variables node)
        variable-set (set variables)]
    (case kind
      (:predicate :not :function) (set/subset? variable-set target-set)
      :or (let [missing (set/difference variable-set target-set)
                groundable (set (.groundable ^Pattern pattern target-set))]
            (set/subset? missing groundable))
      false)))

(defn- fully-validatable? [node bound]
  (set/subset? (set (node-variables node)) (set bound)))

(defn- pending-validation-nodes [nodes completed bound]
  (->> nodes
       (remove #(contains? completed (:idx %)))
       (filter #(fully-validatable? % bound))
       vec))

(defn- proposing-stage-for
  [nodes completed bound variable variable-order external-pattern]
  (let [added [variable]
        target (bound-target variable-order bound added)
        eligible-nodes (remove #(contains? completed (:idx %)) nodes)
        proposers (filterv #(can-propose? % bound added) eligible-nodes)]
    (cond
      (seq proposers)
      (let [participants (->> eligible-nodes
                              (filter (fn [node]
                                        (or (can-propose? node bound added)
                                            (and (contains? (set (node-variables node)) variable)
                                                 (can-validate-proposing-stage? node target)))))
                              (mapv :pattern))
            participant-ids (set (map (fn [^Pattern pattern] (.getIdx pattern)) participants))]
        {:stage (Stage. added participants target)
         :completed (->> eligible-nodes
                         (filter #(and (contains? participant-ids (:idx %))
                                       (fully-validatable? % target)))
                         (map :idx)
                         set)})

      :else
      (if-let [or-node (first (filter #(let [missing (or-can-propose? % bound)]
                                         (and missing (some #{variable} missing)))
                                      eligible-nodes))]
        (let [or-added (or-can-propose? or-node bound)
              or-target (bound-target variable-order bound or-added)]
          {:stage (Stage. or-added [(:pattern or-node)] or-target)
           :completed #{(:idx or-node)}})
        (when external-pattern
          {:stage (Stage. added [external-pattern] target)
           :completed #{}})))))

(defn- add-validation-stage [nodes completed bound stages]
  (let [validation-nodes (pending-validation-nodes nodes completed bound)]
    (if (empty? validation-nodes)
      [completed stages]
      [(into completed (map :idx validation-nodes))
       (conj stages
             (Stage. []
                     (mapv :pattern validation-nodes)
                     (vec bound)))])))

(defn- plan-scope [nodes variable-order external-pattern]
  (loop [bound []
         completed #{}
         stages []]
    (let [[completed stages] (add-validation-stage nodes completed bound stages)
          remaining-vars (vec (remove (set bound) variable-order))]
      (if (empty? remaining-vars)
        (do
          (when-not (= (set completed) (set (map :idx nodes)))
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
