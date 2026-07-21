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

(defn- bound-target [variable-order bound added]
  (let [target-set (into (set bound) added)]
    (vec (filter target-set variable-order))))

(defn- ordinary-can-propose? [{:keys [kind] :as descriptor} bound variable]
  (and (not= :or kind)
       (some #{variable} (groundable-variables descriptor bound))))

(defn- or-can-propose? [{:keys [kind variables] :as descriptor}
                        bound]
  (when (= :or kind)
    (let [missing (vec (remove (set bound) variables))
          groundable (set (groundable-variables descriptor bound))]
      (when (and (seq missing) (set/subset? (set missing) groundable))
        missing))))

(defn- fully-validatable? [{:keys [variables]} bound]
  (set/subset? (set variables) (set bound)))

(defn- pending-validation-descriptors [descriptors completed bound]
  (->> descriptors
       (remove (fn [{:keys [idx]}]
                 (contains? completed idx)))
       (filter #(fully-validatable? % bound))
       vec))


(defn- logical-proposing-stage-for
  [descriptors completed bound variable variable-order]
  (let [added [variable]
        target (bound-target variable-order bound added)
        eligible-descriptors (remove (fn [{:keys [idx]}]
                                       (contains? completed idx))
                                     descriptors)
        proposers (filterv #(ordinary-can-propose? % bound variable) eligible-descriptors)]
    (cond
      (seq proposers)
      (let [participants (->> eligible-descriptors
                              (filter (fn [descriptor]
                                        (or (ordinary-can-propose? descriptor bound variable)
                                            (fully-validatable? descriptor target))))
                              (mapv (fn [{:keys [idx]}]
                                      idx)))
            participant-ids (set participants)]
        {:stage {:added added
                 :participants participants
                 :target-variables target}
         :completed (->> eligible-descriptors
                         (filter (fn [{:keys [idx] :as descriptor}]
                                   (and (contains? participant-ids idx)
                                        (fully-validatable? descriptor target))))
                         (map (fn [{:keys [idx]}]
                                idx))
                         set)})

      :else
      (when-let [{:keys [idx] :as selected-or}
                 (first (filter #(let [missing (or-can-propose? % bound)]
                                   (and missing (some #{variable} missing)))
                                eligible-descriptors))]
        (let [or-added (or-can-propose? selected-or bound)]
          {:stage {:added or-added
                   :participants [idx]
                   :target-variables (bound-target variable-order bound or-added)}
           :completed #{idx}})))))

(defn- add-logical-validation-stage [descriptors completed bound stages]
  (let [validation-descriptors (pending-validation-descriptors descriptors completed bound)]
    (if (empty? validation-descriptors)
      [completed stages]
      [(into completed (map (fn [{:keys [idx]}]
                              idx)
                            validation-descriptors))
       (conj stages
             {:added []
              :participants (mapv (fn [{:keys [idx]}]
                                    idx)
                                  validation-descriptors)
              :target-variables (vec bound)})])))

(defn- incoming-descriptor [variables]
  {:idx -1
   :kind :relation
   :variables variables
   :groundable (partial relation-groundable variables)})

(defn plan-scope
  "Plans descriptors into logical stages whose participants are descriptor indexes.
  Participant -1 represents relevant `incoming` variables and is planning-only."
  [descriptors variable-order incoming]
  (let [variable-order (vec variable-order)
        incoming-set (set incoming)
        in-scope (filterv incoming-set variable-order)
        new-variable-order (into in-scope (remove incoming-set variable-order))
        descriptors (vec descriptors)
        descriptors (if (seq in-scope)
                      (into [(incoming-descriptor in-scope)] descriptors)
                      descriptors)]
    (loop [bound []
           completed #{}
           stages []]
      (let [[completed stages] (add-logical-validation-stage descriptors completed bound stages)
            remaining-vars (vec (remove (set bound) new-variable-order))]
        (if (empty? remaining-vars)
          (do
            (when-not (= (set completed)
                         (set (map (fn [{:keys [idx]}]
                                     idx)
                                   descriptors)))
              (throw (IllegalStateException. "Not every pattern was lowered into a stage")))
            stages)
          (if-let [{:keys [stage] proposal-completed :completed :as _proposal}
                   (some #(logical-proposing-stage-for descriptors
                                                       completed
                                                       bound
                                                       %
                                                       new-variable-order)
                         remaining-vars)]
            (recur (:target-variables stage)
                   (into completed proposal-completed)
                   (conj stages stage))
            (let [unbound (first remaining-vars)]
              (err/incorrect-ex (format "%s not bound" unbound)
                                {:unbound-var unbound :grounded (set bound)}
                                :db.error/insufficient-binding))))))))

(defn- descriptor-execution-incoming
  [{:keys [kind idx]}
   {:keys [added participants target-variables]}
   bound]
  (case kind
    :or (if (and (seq added) (= [idx] participants))
          bound
          target-variables)
    :not target-variables
    []))

(declare assemble-scope)

(defn- descriptor->exec-pattern
  [{:keys [aev ave] :as db}
   {:keys [kind idx variables clause branches children binding-set] :as _descriptor}
   incoming]
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

    :or (OrPattern. idx
                    (mapv (fn [branch]
                            (assemble-scope db branch variables incoming))
                          branches))

    :not (NotPattern. idx (assemble-scope db children variables incoming))))


(defn- descriptor-incoming-by-index [descriptors logical-stages]
  (let [descriptors-by-index (into {} (map (juxt :idx identity)) descriptors)]
    (loop [bound []
           [stage & remaining] logical-stages
           incoming-by-index {}]
      (if stage
        (let [{:keys [participants target-variables]} stage
              incoming-by-index
              (reduce (fn [result participant]
                        (if-let [{:keys [kind idx] :as descriptor}
                                 (get descriptors-by-index participant)]
                          (if (#{:or :not} kind)
                            (assoc result idx (descriptor-execution-incoming descriptor stage bound))
                            result)
                          result))
                      incoming-by-index
                      participants)]
          (recur target-variables remaining incoming-by-index))
        incoming-by-index))))

(defn- exec-patterns-by-index [db descriptors logical-stages]
  (let [incoming-by-index (descriptor-incoming-by-index descriptors logical-stages)]
    (into {}
          (map (fn [{:keys [idx] :as descriptor}]
                 [idx (descriptor->exec-pattern db
                                                descriptor
                                                (get incoming-by-index idx []))]))
          descriptors)))

(defn- participant-patterns [exec-patterns participant-indexes]
  (into []
        (keep (fn [idx]
                (when-not (= -1 idx)
                  (or (get exec-patterns idx)
                      (throw (IllegalStateException.
                              (format "No descriptor found for participant %s" idx)))))))
        participant-indexes))

(defn- logical-stage->stage
  [exec-patterns {:keys [added participants target-variables] :as _logical-stage}]
  (->Stage added
           (participant-patterns exec-patterns participants)
           target-variables))

(defn- assemble-scope [db descriptors variable-order incoming]
  (let [logical-stages (plan-scope descriptors variable-order incoming)
        exec-patterns (exec-patterns-by-index db descriptors logical-stages)]
    (mapv (partial logical-stage->stage exec-patterns) logical-stages)))

;; clauses -> descriptors
;; descriptors -> logical stages (recursively)
;; descriptors + logical stages -> runtime patterns + stages

(defn plan
  "Compiles a validated, conformed query into a vector of executable Stage records."
  [db {:keys [in where] :as _conformed-query} args variable-order]
  (let [descriptors (into (inputs->descriptors in args)
                          (clauses->descriptors where))]
    (assemble-scope db descriptors (vec variable-order) [])))
