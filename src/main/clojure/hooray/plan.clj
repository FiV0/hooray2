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

(defrecord Stage [added participants proposer-positions target-variables]
  IStage
  (getAdded [_] added)
  (getParticipants [_] participants)
  (getProposerPositions [_] proposer-positions)
  (getTargetVariables [_] target-variables))

(defn- ensure-distinct [values message]
  (when-not (= (count values) (count (distinct values)))
    (throw (IllegalStateException. ^String message))))

(defn- ->stage [added participants proposers target-variables]
  (ensure-distinct added "Stage added variables must be distinct")
  (ensure-distinct target-variables "Stage target variables must be distinct")
  (ensure-distinct (mapv #(.getIdx ^ExecPattern %) participants)
                   "Stage participant indexes must be distinct")
  ;; TODO bring this check back but currently we remove intermediate RelationPattern
  #_
  (when-not (= (empty? proposers) (empty? added))
    (throw (IllegalStateException.
            "Stage proposer positions must be empty exactly when added variables are empty")))
  (when-not (every? (set target-variables) added)
    (throw (IllegalStateException. "Stage target variables must contain added variables")))
  (->Stage added participants proposers target-variables))

(defonce ^:private next-pattern-index (atom 0))

(defn- next-index! []
  (swap! next-pattern-index inc))

(defn- variable-names [values]
  (->> values
       (keep (fn [[value-type value]]
               (when (= :variable value-type)
                 value)))
       util/distinctv))

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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; clauses -> descriptors
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

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
        missing (vec (remove bound-set variables))
        missing-set (set missing)]
    (if (and (seq missing)
             (every? #(set/subset? missing-set
                                   (branch-groundable % bound-set))
                     branches))
      missing
      [])))

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
  (util/distinctv (mapcat :variables descriptors)))

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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; descriptors -> logical stages
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- bound-target [variable-order bound added]
  (let [target-set (into (set bound) added)]
    (vec (filter target-set variable-order))))

(defn- can-propose? [{:keys [kind] :as descriptor} bound variable]
  (and (not= :or kind)
       (some #{variable} (groundable-variables descriptor bound))))

(defn- can-validate? [{:keys [kind] :as descriptor} bound new-variables]
  (when (not= :or kind)
    (let [new-groundable (groundable-variables descriptor bound)]
      (and (seq new-groundable)
           (seq (set/intersection (set new-groundable) (set new-variables)))))))

(defn- or-proposal [{:keys [kind variables] :as descriptor}
                    bound]
  (when (= :or kind)
    (let [missing (vec (remove (set bound) variables))
          groundable (set (groundable-variables descriptor bound))]
      (when (and (seq missing) (set/subset? (set missing) groundable))
        missing))))

(defn- fully-validatable? [{:keys [variables]} bound]
  (set/subset? (set variables) (set bound)))

;; Invariant: `:or`/`:not` descriptors participate in exactly one stage — they never
;; propose via `can-propose?` and every path that admits them (validator, OR fallback,
;; validation stage) also marks them completed, keeping their incoming unambiguous.
(defn- proposing-stage-for
  [descriptors completed bound variable variable-order]
  (let [added [variable]
        target (bound-target variable-order bound added)
        eligible-descriptors (remove (comp completed :idx) descriptors)
        proposers (filter #(can-propose? % bound variable) eligible-descriptors)]
    (if (seq proposers)
      (let [participants (->> eligible-descriptors
                              (filter (fn [descriptor]
                                        (or (can-propose? descriptor bound variable)
                                            (fully-validatable? descriptor target))))
                              (mapv :idx))
            participant-ids (set participants)]
        {:stage {:added added
                 :proposers (mapv :idx proposers)
                 :participants participants
                 :target-variables target}
         :completed (->> eligible-descriptors
                         (filter (fn [{:keys [idx] :as descriptor}]
                                   (and (contains? participant-ids idx)
                                        (fully-validatable? descriptor target))))
                         (map :idx)
                         set)})

      ;; This finds the first OR Pattern that introduces the variable and
      ;; can also propose its remaining not yet grounded variables.
      (when-let [{:keys [idx] :as selected-or}
                 (first (filter #(->> (or-proposal % bound)
                                      (some #{variable}))
                                eligible-descriptors))]
        (let [or-added (or-proposal selected-or bound)
              or-validators (->> eligible-descriptors
                                 (filter #(can-validate? % bound or-added)))
              target (bound-target variable-order bound or-added)]
          {:stage {:added or-added
                   :proposers [idx]
                   :participants (into [idx] (map :idx or-validators))
                   :target-variables target}
           :completed (->> or-validators
                           (filter #(fully-validatable? % target))
                           (map :idx)
                           (into #{idx}))})))))

(defn- add-validation-stage [descriptors completed bound stages]
  (if-let [validation-descriptors (->> descriptors
                                       (remove (comp completed :idx))
                                       (filter #(fully-validatable? % bound))
                                       seq)]
    (let [participant-ids (mapv :idx validation-descriptors)]
      [(into completed participant-ids)
       (conj stages
             {:added []
              :proposers []
              :participants participant-ids
              :target-variables (vec bound)})])
    [completed stages]))

(defn- incoming-descriptor [variables]
  {:idx -1
   :kind :relation
   :variables variables
   :groundable (partial relation-groundable variables)})

(defn plan-scope
  "Plans descriptors into logical stages whose participants are descriptor indexes.
  Proposers are the participant indexes that can introduce the stage's added variables.
  Participant -1 represents relevant `incoming` variables and is planning-only."
  [descriptors variable-order incoming]
  (let [incoming-set (set incoming)
        in-scope (filterv incoming-set variable-order)
        new-variable-order (into in-scope (remove incoming-set variable-order))
        descriptors (if (seq in-scope)
                      (into [(incoming-descriptor in-scope)] descriptors)
                      descriptors)]
    (loop [bound []
           completed #{}
           stages []]
      (let [[completed stages] (add-validation-stage descriptors completed bound stages)
            remaining-vars (remove (set bound) new-variable-order)]
        (if (empty? remaining-vars)
          (do
            (when-not (= (set completed) (set (map :idx descriptors)))
              (throw (IllegalStateException. "Not every pattern was lowered into a stage")))
            stages)
          (if-let [{:keys [stage] proposal-completed :completed :as _proposal}
                   ;; This essentially means creates a topological sort of the variables
                   ;; with variable-order as the tie-breaker. If variable-order is already
                   ;; a topo sort then variables will get introduced in that order.
                   (some #(proposing-stage-for descriptors
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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; descriptors + logical stages -> runtime patterns + stages
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

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

(defn- fold-stages [db descriptors logical-stages]
  (let [descriptors-by-index (into {} (map (juxt :idx identity) descriptors))
        get-descriptor (fn [idx] (or (get descriptors-by-index idx)
                                     (throw (IllegalStateException.
                                             (format "No descriptor found for participant %s" idx)))))]
    (loop [patterns-by-index {}
           stages []
           bound []
           [{:keys [added proposers participants target-variables] :as logical-stage} & logical-stages] logical-stages]
      (if-not logical-stage
        stages
        (let [participants (vec (remove #{-1} participants))
              positions-by-index (into {} (map-indexed (fn [position idx]
                                                         [idx position])
                                                       participants))
              proposers-idxs (mapv positions-by-index (remove #{-1} proposers))
              patterns-by-index (reduce (fn [p-by-idx idx]
                                          (update p-by-idx idx #(or %
                                                                    (descriptor->exec-pattern
                                                                     db
                                                                     (get-descriptor idx)
                                                                     ;; TODO: This hints at some better code organization.
                                                                     ;; Some proposers will receive input as target-variables, because
                                                                     ;; they don't win the proposal. The check here only works because
                                                                     ;; patterns where it matters (`or` and `not`) they
                                                                     ;; are not part of the proposers.
                                                                     (if (some #{idx} proposers)
                                                                       bound
                                                                       target-variables)))))
                                        patterns-by-index participants)]
          (recur patterns-by-index
                 (conj stages (->stage added (mapv patterns-by-index participants) proposers-idxs target-variables))
                 target-variables
                 logical-stages))))))

(defn- assemble-scope [db descriptors variable-order incoming]
  (fold-stages db descriptors (plan-scope descriptors variable-order incoming)))

;; The planning pipeline goes through 3 stages.
;; 1. We first translate the query AST (conformed-query) into a recursive list of descriptors.
;; The descriptors are essentially the AST with some additional information. A groundable function
;; that returns a list of variables that can be grounded by the pattern given some set of already
;; bound variables and the list of variables appearing in the sub-plan (the descriptor).
;; 2. We calculate stages as pure data per scope. Scopes are top-level scopes (:in + :where),
;; `not` scopes and `or` scopes. A stage is a set of `added` variables in that stage
;; (most of the time a single variable), proposer and participant descriptor indexes, and finally
;; a list of target-variables describing the result layout after the stage executes.
;; 3. Finally we fold the descriptors + logical stages recursively into runtime patterns
;; (`ExecPattern`) and stages (`IStage`).

(defn plan
  "Compiles a validated, conformed query into a vector of executable Stage records."
  [db {:keys [in where] :as _conformed-query} args variable-order]
  (let [descriptors (into (inputs->descriptors in args)
                          (clauses->descriptors where))]
    (assemble-scope db descriptors (vec variable-order) [])))
