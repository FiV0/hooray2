(ns hooray.plan2
  (:require
   [clojure.core.match :refer [match]]
   [clojure.set :as set]
   [hooray.db :as db]
   [hooray.error :as err]
   [hooray.util :as util])
  (:import
   (org.hooray UniversalComparator)
   (org.hooray.engine BindingSet)
   (org.hooray.iterator
    GenericFnPrefixExtender
    GenericPredicatePrefixExtender
    GenericPrefixExtender
    GenericRelationPrefixExtender
    SealedIndex$MapIndex
    SealedIndex$SetIndex)))

(defn- ensure-distinct [values message]
  (when-not (= (count values) (count (distinct values)))
    (throw (IllegalStateException. ^String message))))

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
  [{:keys [groundable] :as _descriptor} bound]
  (vec (groundable (set bound))))

(defn- fn+args->function [f args var->idx]
  (match args
    [[:constant c1] [:constant c2]] (util/->closure (fn [] (f c1 c2)))
    [[:constant c1] [:variable _]] (util/->function (fn [a] (f c1 a)))
    [[:variable _] [:constant c2]] (util/->function (fn [a] (f a c2)))
    [[:variable v1] [:variable v2]] (if (< (var->idx v1) (var->idx v2))
                                      (util/->bifunction (fn [a b] (f a b)))
                                      (util/->bifunction (fn [a b] (f b a))))
    [[:constant c]] (util/->closure (fn [] (f c)))
    [[:variable _]] (util/->function (fn [a] (f a)))
    :else (throw (ex-info "Unknown function pattern" {:fn f :args args}))))

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
  (->> descriptors
       (mapcat (fn [{:keys [variables]}]
                 variables))
       util/distinctv))

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

(defn- or-proposal [{:keys [kind variables] :as descriptor} bound]
  (when (= :or kind)
    (let [missing (vec (remove (set bound) variables))
          groundable (set (groundable-variables descriptor bound))]
      (when (and (seq missing) (set/subset? (set missing) groundable))
        missing))))

(defn- fully-validatable? [{:keys [variables]} bound]
  (set/subset? (set variables) (set bound)))

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
      (when-let [{:keys [idx] :as selected-or}
                 (first (filter #(->> (or-proposal % bound)
                                      (some #{variable}))
                                eligible-descriptors))]
        (let [or-added (or-proposal selected-or bound)]
          {:stage {:added or-added
                   :proposers [idx]
                   :participants [idx]
                   :target-variables (bound-target variable-order bound or-added)}
           :completed #{idx}})))))

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

(defn- scope-layout [variable-order incoming]
  (let [incoming-set (set incoming)
        input-variables (filterv incoming-set variable-order)]
    {:input-variables input-variables
     :variable-order (into input-variables (remove incoming-set variable-order))}))

(defn plan-scope
  "Plans descriptors into logical stages whose participants are descriptor indexes.
  Proposers are the participant indexes that can introduce the stage's added variables.
  Participant -1 represents relevant `incoming` variables and is planning-only."
  [descriptors variable-order incoming]
  (let [{:keys [input-variables variable-order]} (scope-layout variable-order incoming)
        descriptors (if (seq input-variables)
                      (into [(incoming-descriptor input-variables)] descriptors)
                      descriptors)]
    (loop [bound []
           completed #{}
           stages []]
      (let [[completed stages] (add-validation-stage descriptors completed bound stages)
            remaining-vars (remove (set bound) variable-order)]
        (if (empty? remaining-vars)
          (do
            (when-not (= (set completed) (set (map :idx descriptors)))
              (throw (IllegalStateException. "Not every pattern was lowered into a stage")))
            stages)
          (if-let [{:keys [stage] proposal-completed :completed :as _proposal}
                   (some #(proposing-stage-for descriptors
                                               completed
                                               bound
                                               %
                                               variable-order)
                         remaining-vars)]
            (recur (:target-variables stage)
                   (into completed proposal-completed)
                   (conj stages stage))
            (let [unbound (first remaining-vars)]
              (err/incorrect-ex (format "%s not bound" unbound)
                                {:unbound-var unbound :grounded (set bound)}
                                :db.error/insufficient-binding))))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; logical stages -> checked staged-prefix plan
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- layout-prefix? [prefix layout]
  (= (vec prefix) (vec (take (count prefix) layout))))

(defn- generic-stage [target-variables]
  (ensure-distinct target-variables "Generic stage target variables must be distinct")
  {:kind :generic
   :target-variables (vec target-variables)})

(defn- or-stage
  [variables added input-variables target-variables branches]
  (ensure-distinct variables "OR variables must be distinct")
  (ensure-distinct added "OR added variables must be distinct")
  (ensure-distinct target-variables "OR target variables must be distinct")
  (when-not (= (set target-variables)
               (into (set input-variables) added))
    (throw (IllegalStateException.
            "OR target variables must equal input variables plus added variables")))
  (when-not (every? #(= (set variables) (set (:variable-order %))) branches)
    (throw (IllegalStateException. "Every OR branch must produce the OR variables")))
  {:kind :or
   :variables (vec variables)
   :added (vec added)
   :target-variables (vec target-variables)
   :branches (vec branches)})

(defn- not-stage
  [variables input-variables target-variables body]
  (ensure-distinct variables "NOT variables must be distinct")
  (ensure-distinct target-variables "NOT target variables must be distinct")
  (when-not (= (vec input-variables) (vec target-variables))
    (throw (IllegalStateException. "NOT must preserve its input layout")))
  (when-not (= (set variables) (set (:variable-order body)))
    (throw (IllegalStateException. "NOT body must produce the NOT variables")))
  {:kind :not
   :variables (vec variables)
   :target-variables (vec target-variables)
   :body body})

(defn- validate-stage-transition [variable-order input-variables stage]
  (let [{:keys [kind target-variables added]} stage]
    (ensure-distinct target-variables "Stage target variables must be distinct")
    (when-not (layout-prefix? target-variables variable-order)
      (throw (IllegalStateException. "Stage target variables must be a scope-order prefix")))
    (case kind
      :generic
      (when-not (and (> (count target-variables) (count input-variables))
                     (layout-prefix? input-variables target-variables))
        (throw (IllegalStateException. "Generic stages must extend their input layout")))

      :or
      (when-not (= (set target-variables)
                   (into (set input-variables) added))
        (throw (IllegalStateException.
                "OR target variables must equal input variables plus added variables")))

      :not
      (when-not (= (vec input-variables) (vec target-variables))
        (throw (IllegalStateException. "NOT must preserve its input layout")))

      (throw (IllegalStateException. (str "Unknown stage kind " kind))))
    (vec target-variables)))

(defn- compiled-scope
  [input-variables variable-order extenders stages]
  (let [input-variables (vec input-variables)
        variable-order (vec variable-order)
        stages (vec stages)]
    (ensure-distinct input-variables "Scope input variables must be distinct")
    (ensure-distinct variable-order "Scope variable order must be distinct")
    (when-not (layout-prefix? input-variables variable-order)
      (throw (IllegalStateException. "Scope input variables must prefix its variable order")))
    (let [final-layout (reduce (partial validate-stage-transition variable-order)
                               []
                               stages)]
      (when-not (= final-layout variable-order)
        (throw (IllegalStateException. "Scope stages must reach its complete variable order"))))
    {:input-variables input-variables
     :variable-order variable-order
     :extenders (vec extenders)
     :stages stages}))

(defn- sealed-index [index]
  (if (set? index)
    (SealedIndex$SetIndex. index)
    (SealedIndex$MapIndex. index)))

(defn- triple-extender
  [{:keys [eav aev ave opts] :as _db} variable-order {:keys [e a v] :as clause}]
  (let [empty-set (db/set* (:storage opts))
        empty-map (db/map* (:storage opts))
        var->idx (zipmap variable-order (range))]
    (match [e a v]
      [[:constant _] [:constant _] [:constant _]]
      (err/unsupported-ex)

      [[:constant entity] [:constant attribute] [:variable value-var]]
      (GenericPrefixExtender.
       (sealed-index (get-in eav [entity attribute] empty-set))
       [(int (var->idx value-var))])

      [[:variable entity-var] [:constant attribute] [:constant value]]
      (GenericPrefixExtender.
       (sealed-index (get-in ave [attribute value] empty-set))
       [(int (var->idx entity-var))])

      [[:variable entity-var] [:constant attribute] [:variable value-var]]
      (let [entity-level (int (var->idx entity-var))
            value-level (int (var->idx value-var))
            [index levels] (if (< entity-level value-level)
                             [aev [entity-level value-level]]
                             [ave [value-level entity-level]])]
        (GenericPrefixExtender.
         (sealed-index (get index attribute empty-map))
         levels))

      :else (throw (ex-info "Unknown triple clause" {:triple clause})))))

(defn- relation-extender
  [{:keys [variables binding-set] :as _descriptor} variable-order]
  (let [variable-set (set variables)
        ordered-variables (filterv variable-set variable-order)
        var->idx (zipmap variable-order (range))
        ^BindingSet normalized (.reorder ^BindingSet binding-set ordered-variables)
        levels (mapv (comp int var->idx) ordered-variables)]
    (GenericRelationPrefixExtender. levels (.getRows normalized))))

(defn- predicate-extender
  [{:keys [clause]} variable-order]
  (let [{:keys [predicate args]} clause
        var->idx (zipmap variable-order (range))
        variable-args (->> args
                           (keep (fn [[argument-type value]]
                                   (when (= :variable argument-type)
                                     value))))]
    (GenericPredicatePrefixExtender.
     (sort (mapv (comp int var->idx) variable-args))
     (fn+args->function (util/resolve-fn predicate) args var->idx))))

(defn- function-extender
  [{:keys [clause]} variable-order]
  (let [[{:keys [fun args]} output] clause
        var->idx (zipmap variable-order (range))
        variable-args (->> args
                           (keep (fn [[argument-type value]]
                                   (when (= :variable argument-type)
                                     value))))]
    (GenericFnPrefixExtender.
     (sort (mapv (comp int var->idx) variable-args))
     (int (var->idx output))
     (fn+args->function (util/resolve-fn fun) args var->idx))))

(defn- descriptor->extender [db descriptor variable-order]
  (case (:kind descriptor)
    :triple (triple-extender db variable-order (:clause descriptor))
    :relation (relation-extender descriptor variable-order)
    :predicate (predicate-extender descriptor variable-order)
    :function (function-extender descriptor variable-order)))

(declare compile-scope)

(defn- append-generic-stage [stages target-variables]
  (if (= :generic (:kind (peek stages)))
    (conj (pop stages) (generic-stage target-variables))
    (conj stages (generic-stage target-variables))))

(defn- compile-or-stage
  [db {:keys [variables branches]} input-variables added target-variables]
  (let [compiled-branches (mapv #(compile-scope db % variables input-variables) branches)]
    (or-stage variables added input-variables target-variables compiled-branches)))

(defn- compile-not-stage
  [db {:keys [variables children]} input-variables target-variables]
  (not-stage variables
             input-variables
             target-variables
             (compile-scope db children variables input-variables)))

(defn- append-validation-composites
  [db stages descriptors input-variables target-variables]
  (reduce (fn [compiled descriptor]
            (case (:kind descriptor)
              :or (conj compiled
                        (compile-or-stage db
                                          descriptor
                                          input-variables
                                          []
                                          target-variables))
              :not (conj compiled
                         (compile-not-stage db
                                            descriptor
                                            input-variables
                                            target-variables))))
          stages
          descriptors))

(defn- lower-stages [db descriptors logical-stages]
  (let [descriptors-by-index (into {} (map (juxt :idx identity) descriptors))
        descriptor-for (fn [idx]
                         (or (get descriptors-by-index idx)
                             (throw (IllegalStateException.
                                     (format "No descriptor found for participant %s" idx)))))]
    (loop [bound []
           stages []
           previous-proposing-or? false
           [{:keys [added proposers participants target-variables] :as logical-stage}
            & remaining] logical-stages]
      (if-not logical-stage
        stages
        (let [participant-descriptors (mapv descriptor-for (remove #{-1} participants))
              ordinary (filterv #(not (#{:or :not} (:kind %))) participant-descriptors)
              composites (filterv #(#{:or :not} (:kind %)) participant-descriptors)
              proposer-descriptors (mapv descriptor-for (remove #{-1} proposers))
              proposing-or (first (filter #(= :or (:kind %)) proposer-descriptors))]
          (cond
            proposing-or
            (recur target-variables
                   (conj stages
                         (compile-or-stage db
                                           proposing-or
                                           bound
                                           added
                                           target-variables))
                   true
                   remaining)

            (seq added)
            (let [stages (append-generic-stage stages target-variables)
                  stages (append-validation-composites db
                                                        stages
                                                        composites
                                                        target-variables
                                                        target-variables)]
              (recur target-variables stages false remaining))

            :else
            (do
              (when (and (seq ordinary) (not previous-proposing-or?))
                (throw (IllegalStateException.
                        "Leaf-only validation must immediately follow a proposing OR")))
              (recur target-variables
                     (append-validation-composites db
                                                   stages
                                                   composites
                                                   bound
                                                   target-variables)
                     false
                     remaining))))))))

(defn- compile-scope [db descriptors variable-order incoming]
  (let [{:keys [input-variables variable-order]} (scope-layout variable-order incoming)
        logical-stages (plan-scope descriptors variable-order input-variables)
        stages (lower-stages db descriptors logical-stages)
        extenders (->> descriptors
                       (remove #(#{:or :not} (:kind %)))
                       (mapv #(descriptor->extender db % variable-order)))]
    (compiled-scope input-variables variable-order extenders stages)))

(defn plan
  "Compiles a validated, conformed query into a recursive Clojure staged-prefix plan."
  [db {:keys [in where] :as _conformed-query} args variable-order]
  (let [descriptors (into (inputs->descriptors in args)
                          (clauses->descriptors where))]
    (compile-scope db descriptors (vec variable-order) [])))
