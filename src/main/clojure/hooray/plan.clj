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
    IStage
    NotPattern
    OrPattern
    PatternValue$Constant
    PatternValue$Variable
    PredicatePattern
    RelationPattern
    TriplePattern)))

(defrecord Stage [added participants target-variables]
  IStage
  (getAdded [_] added)
  (getParticipants [_] participants)
  (getTargetVariables [_] target-variables))

(defn- distinctv [values]
  (vec (distinct values)))

(defonce ^:private next-pattern-index (atom 0))

(defn- next-index! []
  (swap! next-pattern-index inc))

(defn- variable-names [values]
  (->> values
       (keep (fn [[value-type value]]
               (when (= :variable value-type)
                 value)))
       distinctv))

(defn- groundable-variables
  [{:keys [groundable] :as _node} bound]
  (vec (groundable (set bound))))

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

(defn- branch-groundable [descriptors bound]
  (loop [grounded (set bound)]
    (let [next-grounded (reduce (fn [current descriptor]
                                  (into current (groundable-variables descriptor current)))
                                grounded
                                descriptors)]
      (if (= grounded next-grounded)
        grounded
        (recur next-grounded)))))

(defn- or-groundable [branches variables bound]
  (let [bound-set (set bound)
        common (->> branches
                    (map #(branch-groundable % bound-set))
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

(declare clause->descriptor)

(defn- descriptor-variables [descriptors]
  (->> descriptors
       (mapcat (fn [{:keys [variables]}]
                 variables))
       distinctv))

(defn- triple-descriptor [idx {:keys [e a v] :as clause}]
  (let [[attribute-type _attribute] a]
    (when-not (= :constant attribute-type)
      (err/unsupported-ex "Currently variables in attribute position are not supported"))
    (let [variables (variable-names [e v])]
      {:kind :triple
       :idx idx
       :variables variables
       :groundable (fn [bound]
                     (vec (remove bound variables)))
       :clause clause})))

(defn- predicate-descriptor [idx {:keys [args] :as clause}]
  (let [variables (variable-names args)]
    {:kind :predicate
     :idx idx
     :variables variables
     :groundable (constantly [])
     :clause clause}))

(defn- function-descriptor [idx [{:keys [args]} output :as clause]]
  (let [argument-vars (variable-names args)
        variables (conj argument-vars output)]
    {:kind :function
     :idx idx
     :variables variables
     :groundable (fn [bound]
                   (if (and (set/subset? (set argument-vars) bound)
                            (not (contains? bound output)))
                     [output]
                     []))
     :clause clause}))

(defn- branch->descriptors [[branch-type branch :as clause]]
  (if (= :and branch-type)
    (mapv clause->descriptor branch)
    [(clause->descriptor clause)]))

(defn- or-descriptor [idx clause]
  (let [branches (mapv branch->descriptors clause)
        variables (descriptor-variables (first branches))]
    {:kind :or
     :idx idx
     :variables variables
     :groundable (partial or-groundable branches variables)
     :clause clause
     :branches branches}))

(defn- not-descriptor [idx clause]
  (let [children (mapv clause->descriptor clause)]
    {:kind :not
     :idx idx
     :variables (descriptor-variables children)
     :groundable (constantly [])
     :clause clause
     :children children}))

(defn- clause->descriptor [[clause-type clause]]
  (let [idx (next-index!)]
    (case clause-type
      :triple (triple-descriptor idx clause)
      :predicate (predicate-descriptor idx clause)
      :fn (function-descriptor idx clause)
      :or (or-descriptor idx clause)
      :not (not-descriptor idx clause))))

(defn clauses->descriptors
  "Builds runtime-independent descriptors from conformed `:where` clauses."
  [clauses]
  (mapv clause->descriptor clauses))

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

(defn inputs->descriptors
  "Builds relation descriptors from conformed `:in` bindings and their arguments."
  [in args]
  (when-not (= (count in) (count args))
    (throw (IllegalArgumentException. (format ":in %s and :args %s" (pr-str in) (pr-str args)))))
  (mapv (fn [[[binding-type binding] argument]]
          (let [idx (next-index!)
                [variables rows] (binding-relation binding-type binding argument)
                variables (vec variables)]
            {:kind :relation
             :idx idx
             :variables variables
             :groundable (partial relation-groundable variables)
             :binding-set (BindingSet. variables rows)}))
        (map vector in args)))


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

(declare plan-scope descriptor->node)

(defn- descriptors->nodes [db descriptors]
  (mapv #(descriptor->node db %) descriptors))

(defn- descriptor->node
  [{:keys [aev ave] :as db}
   {:keys [kind idx variables clause branches children binding-set] :as descriptor}]
  (let [exec-pattern
        (case kind
          :triple (let [{:keys [e a v]} clause
                        [_ attribute] a]
                    (TriplePattern. idx aev ave (pattern-value e) attribute (pattern-value v)))

          :predicate (let [{:keys [predicate args]} clause
                           arguments (mapv pattern-value args)]
                       (PredicatePattern. idx arguments (kotlin-function predicate (count arguments))))

          :function (let [[{:keys [fun args]} output] clause
                          arguments (mapv pattern-value args)]
                      (FunctionPattern. idx arguments output (kotlin-function fun (count arguments))))

          :relation (RelationPattern. idx binding-set)

          :or (let [seed-pattern (external-binding-pattern (next-index!) variables)
                    branch-stages (mapv (fn [branch]
                                          (plan-scope (descriptors->nodes db branch)
                                                      variables
                                                      seed-pattern))
                                        branches)]
                (OrPattern. idx branch-stages))

          :not (let [seed-pattern (external-binding-pattern (next-index!) variables)
                     child-stages (plan-scope (descriptors->nodes db children)
                                              variables
                                              seed-pattern)]
                 (NotPattern. idx child-stages)))]
    (assoc descriptor :exec-pattern exec-pattern)))

(defn- bound-target [variable-order bound added]
  (let [target-set (into (set bound) added)]
    (vec (filter target-set variable-order))))

(defn- ordinary-can-propose? [{:keys [kind] :as node} bound variable]
  (and (not= :or kind)
       (some #{variable} (groundable-variables node bound))))

(defn- or-can-propose? [{:keys [kind variables] :as node}
                        bound]
  (when (= :or kind)
    (let [missing (vec (remove (set bound) variables))
          groundable (set (groundable-variables node bound))]
      (when (and (seq missing) (set/subset? (set missing) groundable))
        missing))))

(defn- fully-validatable? [{:keys [variables]} bound]
  (set/subset? (set variables) (set bound)))

(defn- pending-validation-nodes [nodes completed bound]
  (->> nodes
       (remove (fn [{:keys [idx]}]
                 (contains? completed idx)))
       (filter #(fully-validatable? % bound))
       vec))

(defn- proposing-stage-for
  [nodes completed bound variable variable-order external-pattern]
  (let [added [variable]
        target (bound-target variable-order bound added)
        eligible-nodes (remove (fn [{:keys [idx]}]
                                 (contains? completed idx))
                               nodes)
        proposers (filterv #(ordinary-can-propose? % bound variable) eligible-nodes)]
    (cond
      (seq proposers)
      (let [participants (->> eligible-nodes
                              (filter (fn [node]
                                        (or (ordinary-can-propose? node bound variable)
                                            (fully-validatable? node target))))
                              (mapv (fn [{:keys [exec-pattern]}]
                                      exec-pattern)))
            participant-ids (set (map (fn [^ExecPattern pattern]
                                        (.getIdx pattern))
                                      participants))]
        {:stage (->Stage added participants target)
         :completed (->> eligible-nodes
                         (filter (fn [{:keys [idx] :as node}]
                                   (and (contains? participant-ids idx)
                                        (fully-validatable? node target))))
                         (map (fn [{:keys [idx]}]
                                idx))
                         set)})

      :else
      (if-let [or-node (first (filter #(let [missing (or-can-propose? % bound)]
                                         (and missing (some #{variable} missing)))
                                      eligible-nodes))]
        (let [or-added (or-can-propose? or-node bound)
              or-target (bound-target variable-order bound or-added)
              {:keys [idx exec-pattern]} or-node]
          {:stage (->Stage or-added [exec-pattern] or-target)
           :completed #{idx}})
        (when external-pattern
          {:stage (->Stage added [external-pattern] target)
           :completed #{}})))))

(defn- add-validation-stage [nodes completed bound stages]
  (let [validation-nodes (pending-validation-nodes nodes completed bound)]
    (if (empty? validation-nodes)
      [completed stages]
      [(into completed (map (fn [{:keys [idx]}]
                              idx)
                            validation-nodes))
       (conj stages
             (->Stage []
                      (mapv (fn [{:keys [exec-pattern]}]
                              exec-pattern)
                            validation-nodes)
                      (vec bound)))])))

(defn- plan-scope [nodes variable-order external-pattern]
  (loop [bound []
         completed #{}
         stages []]
    (let [[completed stages] (add-validation-stage nodes completed bound stages)
          remaining-vars (vec (remove (set bound) variable-order))]
      (if (empty? remaining-vars)
        (do
          (when-not (= (set completed)
                       (set (map (fn [{:keys [idx]}]
                                   idx)
                                 nodes)))
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
          (recur (:target-variables stage)
                 (into completed proposal-completed)
                 (conj stages stage))
          (let [unbound (first remaining-vars)]
            (err/incorrect-ex (format "%s not bound" unbound)
                              {:unbound-var unbound :grounded (set bound)}
                              :db.error/insufficient-binding)))))))

;; clauses -> descriptors
;; descriptors -> runtime nodes (recursively)
;; runtime nodes -> stages

(defn plan
  "Compiles a validated, conformed query into a vector of executable Stage records."
  [db {:keys [in where] :as _conformed-query} args variable-order]
  (let [descriptors (into (inputs->descriptors in args)
                          (clauses->descriptors where))
        nodes (descriptors->nodes db descriptors)]
    (plan-scope nodes (vec variable-order) nil)))
