(ns hooray.query
  (:require
   [clojure.core.match :refer [match]]
   [clojure.set]
   [clojure.spec.alpha :as s]
   [hooray.db :as db])
  (:import
   (org.hooray.algo
    GenericJoin
    Join
    LeapfrogIndex
    LeapfrogJoin
    PrefixExtender)
   (org.hooray.iterator
    AVLIndex$AVLMapIndex
    AVLIndex$AVLSetIndex
    AVLLeapfrogIndex
    GenericPrefixExtender
    GenericPrefixExtenderAnd
    GenericPrefixExtenderNot
    GenericPrefixExtenderOr
    SealedIndex$MapIndex
    SealedIndex$SetIndex)))

(s/def ::variable symbol?)
(s/def ::constant (complement symbol?))

(s/def ::pattern-element (s/or :variable ::variable
                               :constant ::constant))

(s/def ::triple-pattern (s/and vector?
                               (s/cat :e ::pattern-element
                                      :a ::pattern-element
                                      :v ::pattern-element)))

(s/def ::not-pattern (s/and list?
                            (s/cat :type #{'not}
                                   :patterns (s/+ ::pattern))
                            (s/conformer (fn [{:keys [patterns]}] patterns)
                                         (fn [patterns] {:type 'not :patterns patterns}))))

(s/def ::and-pattern (s/and list?
                            (s/cat :type #{'and}
                                   :patterns (s/+ ::pattern))
                            (s/conformer (fn [{:keys [patterns]}] patterns)
                                         (fn [patterns] {:type 'and :patterns patterns}))))


(s/def ::or-pattern (s/and list?
                           (s/cat :type #{'or}
                                  :patterns (s/+ (s/and (s/or :pattern ::pattern
                                                              :and ::and-pattern)
                                                        (s/conformer (fn [[type value]]
                                                                       (case type
                                                                         :pattern value
                                                                         :and [type value]))
                                                                     (fn [pattern-or-and]
                                                                       (if (= :and (first pattern-or-and))
                                                                         pattern-or-and
                                                                         [:pattern pattern-or-and]))))))
                           (s/conformer (fn [{:keys [patterns]}] patterns)
                                        (fn [patterns] {:type 'or :patterns patterns}))))

(s/def ::pattern (s/or :triple ::triple-pattern
                       :not ::not-pattern
                       :or ::or-pattern))

(s/def ::find (s/and vector?
                     (s/+ ::variable)))

(s/def ::keys (s/and vector? (s/+ symbol?)))
(s/def ::strs (s/and vector? (s/+ symbol?)))
(s/def ::syms (s/and vector? (s/+ symbol?)))

(s/def ::scalar-binding ::variable)
(s/def ::collection-binding (s/and (s/tuple ::scalar-binding #{'...})
                                   (s/conformer first #(vector % '...))))
(s/def ::tuple-binding (s/and vector? (s/+ ::scalar-binding)))
(s/def ::relation-binding (s/and vector? (s/+ ::tuple-binding)))

(s/def ::in (s/and vector? (s/* (s/or
                                 :scale-binding ::scalar-binding
                                 :collection-binding ::collection-binding
                                 :tuple-binding ::tuple-binding
                                 :relation-binding ::relation-binding))))

(s/def ::where (s/and vector? (s/+ ::pattern)))

(s/def ::query (s/keys :req-un [::find ::where]
                       :opt-un [::in ::keys ::strs ::syms]))

;; We don't (yet) support a where clause of the form
;; [1 x "Alice"]
;; This is a very unlikely with Datomic as you will know your attributes.
;; [x y "Alice"] -> vae, in this case attribute variable needs to come before entity
;; [1 x y] -> eav entity needs to come before attribute
;; For simplicity let's just forget about attribute variables for now.

(defn unsupported-ex
  ([] (unsupported-ex "Unsupported operation!"))
  ([msg] (throw (ex-info msg {:cognitect.anomalies/category :cognitect.anomalies/unsupported,
                              :cognitect.anomalies/message msg,
                              :db/error :db.error/unsupported}))))

(declare variable-order*)

(defn variable-order [[type value]]
  (-> (case type
        :triple (let [{:keys [e a v]} value]
                  (match [e a v]
                    [[:constant _] [:constant _] [:constant _]] []
                    [[:constant _] [:constant _] [:variable value-var]] [value-var]
                    [[:variable entity-var] [:constant _] [:constant _]] [entity-var]
                    [[:variable entity-var] [:constant _] [:variable value-var]] [entity-var value-var]
                    [_ [:variable _] _] (unsupported-ex "Currently varialbles in attribute position are not supported")))

        (:or :and :not) (variable-order* value))
      distinct))

(defn variable-order* [patterns]
  (->> (map variable-order patterns)
       flatten
       distinct))

(defn free-variables [pattern]
  (set (variable-order pattern)))

(defn free-variables* [patterns]
  (->> (map free-variables patterns)
       (reduce clojure.set/union)))

(defn validate-patterns [patterns]
  (loop [[[type pattern-value :as pattern] & patterns] patterns positive-vars #{}]
    (when type
      (case type
        (:triple :and :not)
        (recur patterns (into positive-vars (free-variables pattern)))
        :or (let [or-branches (mapv free-variables pattern-value)]
              (when-not (every? #(= (first or-branches) %) (rest or-branches))
                (let [msg "Branches of `or` must have same free variables!"]
                  (throw (ex-info msg {:cognitect.anomalies/category :cognitect.anomalies/incorrect,
                                       :cognitect.anomalies/message msg,
                                       :db/error :db.error/invalid-query}))))
              (recur patterns (into positive-vars (free-variables pattern))))))))

(defn validate-query [{:keys [where] :as conformed-query}]
  (when (> (count (select-keys conformed-query [:keys :strs :syms])) 1)
    (let [msg "Only one of :keys, :strs and :syms must be present!"]
      (throw (ex-info msg {:cognitect.anomalies/category :cognitect.anomalies/incorrect,
                           :cognitect.anomalies/message msg,
                           :db/error :db.error/invalid-query}))))
  (validate-patterns where)
  true)

(defn create-iterator [{:keys [storage algo] :as _opts} index var-order participates-in-level]
  (match [storage algo]
    [:hash  _]
    (GenericPrefixExtender. (if (set? index) (SealedIndex$SetIndex. index) (SealedIndex$MapIndex.  index)) participates-in-level)

    [:avl :generic]
    (GenericPrefixExtender. (if (set? index) (SealedIndex$SetIndex. index) (SealedIndex$MapIndex.  index)) participates-in-level)

    [:btree :generic]
    (GenericPrefixExtender. (if (set? index) (SealedIndex$SetIndex. index) (SealedIndex$MapIndex.  index)) participates-in-level)

    [:avl :leapfrog]
    (AVLLeapfrogIndex. (if (set? index) (AVLIndex$AVLSetIndex. index) (AVLIndex$AVLMapIndex. index))
                       var-order (set (for [level participates-in-level]
                                        (nth var-order level))))

    [:btree :leapfrog]
    (unsupported-ex "BTrees not yet supported!")

    :else (throw (ex-info "not yet supported storage + algo type" {:storage storage :algo algo}))))

(defn- compile-pattern [{:keys [eav ave aev opts] :as db} var-order [type pattern]]
  (let [empty-set (db/set* (:storage opts))
        empty-map (db/map* (:storage opts))
        var-to-index (zipmap var-order (range))]
    (case type
      :triple (let [{:keys [e a v]} pattern]
                (match [e a v]
                  [[:constant _] [:constant _] [:constant _]]
                  (unsupported-ex)

                  [[:constant e-const] [:constant a-const] [:variable v-var]]
                  (create-iterator opts (get-in eav [e-const a-const] empty-set) var-order [(get var-to-index v-var)])

                  [[:variable e-var] [:constant a-const] [:constant v-const]]
                  (create-iterator opts (get-in ave [a-const v-const] empty-set) var-order [(get var-to-index e-var)])

                  [[:variable e-var] [:constant a-const] [:variable v-var]]
                  (let [e-index (var-to-index e-var)
                        v-index (var-to-index v-var)
                        [index participates-in-level] (if (< e-index v-index)  [aev [e-index v-index]] [ave [v-index e-index]])]
                    (create-iterator opts (get index a-const empty-map) var-order participates-in-level))

                  :else (throw (ex-info "Unknown triple clause" {:triple pattern}))))
      :or (GenericPrefixExtenderOr. (mapv (partial compile-pattern db var-order) pattern))
      :and (GenericPrefixExtenderAnd. (mapv (partial compile-pattern db var-order) pattern))
      :not (GenericPrefixExtenderNot. (mapv (partial compile-pattern db var-order) pattern) (dec (count var-order))))))

(defn- transpose [mtx]
  (apply mapv vector mtx))

(defn- in->iterators [in var-to-index args {:keys [algo] :as _opts}]
  (when (not= (count in) (count args))
    (throw (IllegalArgumentException. (format ":in %s and :args %s" (pr-str in) (pr-str args)))))
  (let [create-iterator (case algo
                          :leapfrog (fn [var args] (LeapfrogIndex/createSingleLevel args (var-to-index var)))
                          :generic (fn [var args] (PrefixExtender/createSingleLevel args (var-to-index var))))]
    (letfn [(in->iterator [[[type var] arg]]
              (case type
                :scale-binding [(create-iterator var [arg])]
                :collection-binding [(create-iterator var arg)]
                :tuple-binding (if-not (= (count var) (count arg))
                                 (throw (IllegalArgumentException. (format ":tuple %s and args %s must have same length!"
                                                                           (pr-str var) (pr-str arg))))
                                 (->> (zipmap var arg)
                                      (map (fn [[var arg]] (create-iterator var [arg])))))
                ;; TODO error handling for :relation-binding
                :relation-binding (let [args-by-position (transpose arg)]
                                    (->> (zipmap (first var) args-by-position)
                                         (map (fn [[var args]]
                                                (create-iterator var args)))))))]
      (->> (zipmap in args)
           (mapcat in->iterator)))))

(defn- zipmap-fn [find keys var-to-index key-fn]
  (when (not= (count find) (count keys))
    (throw (IllegalArgumentException. "find and keys must have same size!")))
  (let [keys-in-var-order (->> (zipmap find keys)
                               (sort-by (comp var-to-index key))
                               (map (comp key-fn val))) ]
    (fn [row]
      (zipmap keys-in-var-order row))))

(defn- order-result-fn [find var-to-index]
  (fn [row]
    (mapv (fn [var] (nth row (var-to-index var))) find)))

(defn join [compiled-patterns levels {:keys [algo] :as _opts}]
  (let [^Join join-algo (case algo
                          :generic (GenericJoin. compiled-patterns levels)
                          :leapfrog (LeapfrogJoin. compiled-patterns levels))]
    (.join join-algo)))

(defn query [{:keys [opts] :as db} query args]
  {:pre [(s/valid? ::query query) (validate-query (s/conform ::query query))]}
  (let [{:keys [find keys strs syms in where] :as _conformed-query} (s/conform ::query query)
        var-order (variable-order* where)
        var-to-index (zipmap var-order (range))
        compiled-patterns (concat (in->iterators in var-to-index args opts)
                                  (map (partial compile-pattern db var-order) where))
        order-fn (order-result-fn find var-to-index)]
    (cond->> (join compiled-patterns (count var-order) opts)
      true (map order-fn)
      (seq keys) (map (zipmap-fn find keys var-to-index keyword))
      (seq strs) (map (zipmap-fn find strs var-to-index str))
      (seq syms) (map (zipmap-fn find syms var-to-index symbol))
      true set)))

(comment
  (def q '{:find [x y z]
           :in [[x y]]
           :where [[x :person/name y]
                   [x :person/age z]]})


  (s/valid? ::query q)

  (s/conform ::query q)

  (variable-order (s/conform ::query q)))
