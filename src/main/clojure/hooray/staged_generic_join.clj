(ns hooray.staged-generic-join
  (:import
   (org.hooray.algo GenericJoin)
   (org.hooray.engine BindingSet)
   (org.hooray.iterator GenericRelationPrefixExtender)))

(def ^:private unit-bindings (BindingSet. [] [[]]))

(defn- empty-bindings [variables]
  (BindingSet. variables []))

(defn- execute-generic-stage
  [{:keys [extenders]} ^BindingSet input {:keys [target-variables]}]
  (let [input-variables (.getVariables input)]
    (when-not (and (> (count target-variables) (count input-variables))
                   (= input-variables
                      (vec (take (count input-variables) target-variables))))
      (throw (IllegalStateException. "Generic stage must extend its input layout")))
    (if (zero? (.getRowCount input))
      (empty-bindings target-variables)
      (let [join (GenericJoin. extenders (count target-variables))
            rows (.joinFrom join (.getRows input) (count input-variables))]
        (BindingSet. target-variables rows)))))

(declare execute)

(defn- execute-or-branches
  [^BindingSet input variables branches]
  (let [input-variables (.getVariables input)
        variable-set (set variables)
        correlated-variables (filterv variable-set input-variables)
        correlated-input (-> input
                             (.project correlated-variables)
                             (.distinctRows))]
    (reduce (fn [^BindingSet result
                 {:keys [input-variables] :as branch}]
              (let [branch-input (.reorder correlated-input input-variables)
                    branch-result (execute branch branch-input)
                    normalized-result (.reorder branch-result variables)]
                (.unionDistinct result normalized-result)))
            (BindingSet. variables [])
            branches)))

(defn- execute-or-stage
  [{:keys [variable-order extenders] :as scope}
   ^BindingSet input
   {:keys [variables added target-variables branches] :as stage}]
  (if (zero? (.getRowCount input))
    (empty-bindings target-variables)
    (let [or-relation (execute-or-branches input variables branches)]
      (if (empty? added)
        (.semijoin input or-relation)
        (let [or-variable-set (set variables)
              ordered-or-variables (filterv or-variable-set variable-order)
              normalized-relation (.reorder or-relation ordered-or-variables)
              var->idx (zipmap variable-order (range))
              levels (mapv (comp int var->idx) ordered-or-variables)
              relation-extender (GenericRelationPrefixExtender.
                                 levels
                                 (.getRows normalized-relation))]
          (execute-generic-stage
           (assoc scope :extenders (conj extenders relation-extender))
           input
           stage))))))

(defn- execute-stage [scope input {:keys [kind] :as stage}]
  (case kind
    :generic (execute-generic-stage scope input stage)
    :or (execute-or-stage scope input stage)
    :not (throw (UnsupportedOperationException. "NOT stages are not implemented yet"))
    (throw (IllegalStateException. (str "Unknown stage kind " kind)))))

(defn- execute-stages [{:keys [stages] :as scope} input]
  (reduce (fn [bindings stage]
            (execute-stage scope bindings stage))
          input
          stages))

(defn execute
  "Executes a compiled staged-prefix scope against exactly its declared input layout."
  [{:keys [input-variables variable-order extenders] :as scope} ^BindingSet input]
  (let [actual-input-variables (.getVariables input)]
    (when-not (= input-variables actual-input-variables)
      (throw (IllegalArgumentException.
              (format "Scope input variables %s do not match bindings %s"
                      (pr-str input-variables)
                      (pr-str actual-input-variables)))))
    (if (zero? (.getRowCount input))
      (empty-bindings variable-order)
      (let [dynamic-input (when (seq input-variables)
                            (GenericRelationPrefixExtender.
                             (mapv int (range (count input-variables)))
                             (.getRows input)))
            scope (cond-> scope
                    dynamic-input (assoc :extenders (conj extenders dynamic-input)))
            result (execute-stages scope unit-bindings)]
        (when-not (= variable-order (.getVariables ^BindingSet result))
          (throw (IllegalStateException. "Scope execution returned the wrong final layout")))
        result))))
