(ns hooray.query
  (:require
   [clojure.core.match :refer [match]]
   [clojure.set]
   [clojure.string]
   [clojure.spec.alpha :as s]
   [hooray.db :as db]
   [hooray.error :as err]
   [hooray.util :as util])
  (:import
   (java.util Map HashMap)
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
    GenericAndPrefixExtender
    GenericNotPrefixExtender
    GenericOrPrefixExtender
    GenericPredicatePrefixExtender
    SealedIndex$MapIndex
    SealedIndex$SetIndex)))

(s/def ::variable symbol?)
(s/def ::constant (complement symbol?))

(s/def ::aggregate (s/and list?
                          (s/cat :func #{'count 'count-distinct 'sum 'min 'max}
                                 :arg ::variable)))

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

(def ^:private predicates #{'= 'not= '< '<= '> '>= 'string? 'number? 're-find})

(s/def ::predicate-pattern (s/and vector?
                                  (s/tuple (s/and list?
                                                  (s/cat :predicate predicates
                                                         :args (s/+ ::pattern-element))))
                                  (s/conformer (fn [vector-wrapped] (first vector-wrapped))
                                               (fn [unwrapped] [unwrapped]))))

(comment
  (s/conform ::predicate-pattern '[(= ?x 10)])
  (s/unform ::predicate-pattern (s/conform ::predicate-pattern '(= ?x 10))))

(def ^:private fns #{'identity 'inc 'dec 'str})

(s/def ::fn-pattern (s/and vector?
                           (s/tuple (s/and list?
                                           (s/cat :fn fns
                                                  :args (s/+ ::pattern-element)))
                                    ::variable)))

(comment
  (s/conform ::fn-pattern  '[(identity ?x) ?y])
  (s/unform ::fn-pattern (s/conform ::fn-pattern '[(identity ?x) ?y])))


(s/def ::pattern (s/or :triple ::triple-pattern
                       :not ::not-pattern
                       :or ::or-pattern
                       :predicate ::predicate-pattern
                       :fn ::fn-pattern))

(s/def ::find (s/and vector?
                     (s/+ (s/or :variable ::variable
                                :aggregate ::aggregate))))

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

(comment
  (s/explain ::query '{:find [name]
                       :in [[name ...]]
                       :where [[(string? name)]]})
  )

;; We don't (yet) support a where clause of the form
;; [1 x "Alice"]
;; This is a very unlikely with Datomic as you will know your attributes.
;; [x y "Alice"] -> vae, in this case attribute variable needs to come before entity
;; [1 x y] -> eav entity needs to come before attribute
;; For simplicity let's just forget about attribute variables for now.

(declare variable-order*)

(defn variable-order [[type value]]
  (-> (case type
        :triple (let [{:keys [e a v]} value]
                  (match [e a v]
                    [[:constant _] [:constant _] [:constant _]] []
                    [[:constant _] [:constant _] [:variable value-var]] [value-var]
                    [[:variable entity-var] [:constant _] [:constant _]] [entity-var]
                    [[:variable entity-var] [:constant _] [:variable value-var]] [entity-var value-var]
                    [_ [:variable _] _] (err/unsupported-ex "Currently varialbles in attribute position are not supported")))

        (:or :and :not) (variable-order* value)
        :predicate (let [{:keys [args]} value]
                     (->> args
                          (filter (fn [[arg-type _arg-value]] (= arg-type :variable)))
                          (map second))))
      distinct))

(defn- in->variables [in]
  (-> (for [[type var] in]
        (case type
          :scale-binding [var]
          :collection-binding [var]
          :tuple-binding var
          :relation-binding var))
      flatten))

(defn variable-order* [patterns]
  (->> (map variable-order patterns)
       flatten
       distinct))

(defn query->variable-order [{:keys [in where] :as _conformed-query}]
  (let [in-vars (in->variables in)
        where-vars (variable-order* where)]
    (-> (concat in-vars where-vars) distinct)))

(defn free-variables [pattern]
  (set (variable-order pattern)))

(defn free-variables* [patterns]
  (->> (map free-variables patterns)
       (reduce clojure.set/union)))

;; TODO this doesn't properly work for nested patterns, let's first pass to not-join and or-join
(defn validate-patterns
  ([patterns] (validate-patterns patterns #{}))
  ([patterns context-pos-vars]
   (let [{triples :triple ors :or ands :and nots :not} (group-by first patterns)
         positive-vars (into context-pos-vars (mapcat free-variables (concat triples ors ands)))]

     (doseq [[_ or-branches] ors]
       (let [or-branches-free-vars (mapv free-variables or-branches)]
         (when-not (every? #(= (first or-branches-free-vars) %) (rest or-branches-free-vars))
           (err/incorrect-ex "Branches of `or` must have same free variables!" :db.error/invalid-query))
         (validate-patterns or-branches positive-vars)))

     (doseq [[_ and-branches] ands]
       (validate-patterns and-branches positive-vars))

     (doseq [[_ not-branches] nots]
       (let [not-free-vars (free-variables* not-branches)
             unbound-vars (clojure.set/difference not-free-vars positive-vars)]
         (when (seq unbound-vars)
           (let [msg (format "%s not bound in `not` clause: %s"
                             (pr-str (vec unbound-vars))
                             (pr-str (s/unform ::not-pattern not-branches)))]
             (err/incorrect-ex msg :db.error/insufficient-binding)))
         (validate-patterns not-branches positive-vars))))))

(defn validate-query [{:keys [where] :as conformed-query}]
  (when (> (count (select-keys conformed-query [:keys :strs :syms])) 1)
    (err/incorrect-ex "Only one of :keys, :strs and :syms must be present!" :db.error/invalid-query))
  (validate-patterns where)
  true)

(defn create-iterator [{:keys [storage algo] :as _opts} index var-in-join-order participates-in-level]
  (match [storage algo]
    [:hash  _]
    (GenericPrefixExtender. (if (set? index) (SealedIndex$SetIndex. index) (SealedIndex$MapIndex.  index)) participates-in-level)

    [:avl :generic]
    (GenericPrefixExtender. (if (set? index) (SealedIndex$SetIndex. index) (SealedIndex$MapIndex.  index)) participates-in-level)

    [:btree :generic]
    (GenericPrefixExtender. (if (set? index) (SealedIndex$SetIndex. index) (SealedIndex$MapIndex.  index)) participates-in-level)

    [:avl :leapfrog]
    (AVLLeapfrogIndex. (if (set? index) (AVLIndex$AVLSetIndex. index) (AVLIndex$AVLMapIndex. index))
                       var-in-join-order (set (for [level participates-in-level]
                                                (nth var-in-join-order level))))

    [:btree :leapfrog]
    (err/unsupported-ex "BTrees not yet supported!")

    :else (throw (ex-info "not yet supported storage + algo type" {:storage storage :algo algo}))))

(defn fn+args->function [f args var->idx]
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

(defn resolve-predicate [f-symbol]
  (if (= f-symbol 're-find)
    (fn [a b] (boolean (re-find a b)))
    (resolve f-symbol)))

(defn- compile-pattern [{:keys [eav ave aev opts] :as db} var-in-join-order [type pattern]]
  (let [empty-set (db/set* (:storage opts))
        empty-map (db/map* (:storage opts))
        var->idx (zipmap var-in-join-order (range))]
    (case type
      :triple (let [{:keys [e a v]} pattern]
                (match [e a v]
                  [[:constant _] [:constant _] [:constant _]]
                  (err/unsupported-ex)

                  [[:constant e-const] [:constant a-const] [:variable v-var]]
                  (create-iterator opts (get-in eav [e-const a-const] empty-set) var-in-join-order [(get var->idx v-var)])

                  [[:variable e-var] [:constant a-const] [:constant v-const]]
                  (create-iterator opts (get-in ave [a-const v-const] empty-set) var-in-join-order [(get var->idx e-var)])

                  [[:variable e-var] [:constant a-const] [:variable v-var]]
                  (let [e-index (var->idx e-var)
                        v-index (var->idx v-var)
                        [index participates-in-level] (if (< e-index v-index)  [aev [e-index v-index]] [ave [v-index e-index]])]
                    (create-iterator opts (get index a-const empty-map) var-in-join-order participates-in-level))

                  :else (throw (ex-info "Unknown triple clause" {:triple pattern}))))
      :or (GenericOrPrefixExtender. (mapv (partial compile-pattern db var-in-join-order) pattern))
      :and (GenericAndPrefixExtender. (mapv (partial compile-pattern db var-in-join-order) pattern))
      :not (GenericNotPrefixExtender. (mapv (partial compile-pattern db var-in-join-order) pattern) (dec (count var-in-join-order)))

      :predicate (let [{:keys [predicate args]} pattern
                       variable-args (->> (filter (fn [[type _value]] (= type :variable)) args)
                                          (map second))]
                   (GenericPredicatePrefixExtender. (sort (mapv var->idx variable-args))
                                                    (fn+args->function (resolve-predicate predicate) args var->idx))))))

(defn- in->iterators [in var->idx args {:keys [algo] :as _opts}]
  (when (not= (count in) (count args))
    (throw (IllegalArgumentException. (format ":in %s and :args %s" (pr-str in) (pr-str args)))))
  (let [create-single-iterator (case algo
                                 :leapfrog (fn [var args] (LeapfrogIndex/createSingleLevel args (var->idx var)))
                                 :generic (fn [var args] (PrefixExtender/createSingleLevel args (var->idx var))))
        create-from-prefix-iterator (case algo
                                      :leapfrog (throw (err/unsupported-ex "Not yet supported!"))
                                      :generic (fn [vars args]
                                                 ;; createFromPrefixExtender wants List<int> for vars
                                                 (PrefixExtender/createFromPrefixExtender (mapv (comp int var->idx) vars) args)))
        create-from-prefixes-iterator (case algo
                                        :leapfrog (throw (err/unsupported-ex "Not yet supported!"))
                                        :generic (fn [vars args]
                                                   ;; createFromPrefixesExtender wants List<int> for vars
                                                   (PrefixExtender/createFromPrefixesExtender (mapv (comp int var->idx) vars) args)))]
    (letfn [(in->iterator [[[type var] arg]]
              (case type
                :scale-binding (create-single-iterator var [arg])
                :collection-binding (create-single-iterator var arg)
                :tuple-binding (if-not (= (count var) (count arg))
                                 (throw (IllegalArgumentException. (format ":tuple %s and args %s must have same length!"
                                                                           (pr-str var) (pr-str arg))))
                                 (create-from-prefix-iterator var arg))
                ;; TODO error handling for :relation-binding
                :relation-binding (create-from-prefixes-iterator (first var) arg)))]
      (->> (zipmap in args)
           (map in->iterator)))))


(defn join [compiled-patterns levels {:keys [algo] :as _opts}]
  (let [^Join join-algo (case algo
                          :generic (GenericJoin. compiled-patterns levels)
                          :leapfrog (LeapfrogJoin. compiled-patterns levels))]
    (.join join-algo)))

(defmulti aggregate (fn [name & _args] name))

(defmethod aggregate 'count [_]
  (fn aggregate-count
    (^long [] 0)
    (^long [^long acc] acc)
    (^long [^long acc _] (inc acc))))

(defmethod aggregate 'count-distinct [_]
  (fn aggregate-count-distinct
    ([] (transient #{}))
    ([acc] (count (persistent! acc)))
    ([acc x] (conj! acc x))))

(defmethod aggregate 'sum [_]
  (fn aggregate-sum
    ([] 0)
    ([acc] acc)
    ([acc x] (+ acc x))))

(defmethod aggregate 'min [_]
  (fn aggregate-min
    ([])
    ([acc] acc)
    ([acc x]
     (if acc
       (if (pos? (compare acc x))
         x
         acc)
       x))))

(defmethod aggregate 'max [_]
  (fn aggregate-max
    ([])
    ([acc] acc)
    ([acc x]
     (if acc
       (if (neg? (compare acc x))
         x
         acc)
       x))))

(defn- emit-projection [[find-type find-form :as _find-arg]]
  (case find-type
    :variable {:logic-vars #{find-form}
               :inputs [find-form]
               :->result first}
    :aggregate (let [{:keys [func arg]} find-form
                     agg-sym (gensym func)]
                 {:aggregate [agg-sym {:aggregate-fn (aggregate func)
                                       :logic-vars #{arg}
                                       :inputs [arg]}]
                  ;; not yet used
                  :inputs [agg-sym]
                  :->result first})))

(defn row->input-fn [inputs symbol->idx]
  (fn [row]
    (mapv (fn [input] (nth row (symbol->idx input))) inputs)))

(defn row->result-value-fn [->result inputs symbol->idx]
  (fn [row]
    (->result (mapv (fn [input] (nth row (symbol->idx input))) inputs))))

(defn compile-find-args [conformed-find]
  (for [find-arg conformed-find]
    (emit-projection find-arg)))

;; terminology
;; var->idx - logic-vars to index in join result
;; row-symbol - either logic var or agg-sym
;; row-symbol->idx - same as var->idx but also includes agg-syms

;; heavily copied from XTDB
(defn agg-find-fn [find-args row-symbol->idx]
  ;; for now a group is defined just by a row before aggregate applications as there is no expression engine or any other logic
  ;; this will likely need to change as more stuff gets added
  (let [[aggregates non-aggregates] ((juxt filter remove) :aggregate find-args)
        value-fns (mapv (fn [{:keys [->result inputs]}] (row->result-value-fn ->result inputs row-symbol->idx)) non-aggregates)
        ;; note this is a nil group if there are no non-aggregate find args
        ->group (fn [row]
                  (mapv #(% row) value-fns))
        agg-fns (->> aggregates
                     (map :aggregate)
                     (mapv (fn [[agg-k {:keys [inputs aggregate-fn] :as _agg}]]
                             (let [->inputs (row->input-fn inputs row-symbol->idx)]
                               [agg-k (fn
                                        ([] (aggregate-fn))
                                        ([acc row] (apply aggregate-fn acc (->inputs row)))
                                        ;; finalize
                                        ([acc] (aggregate-fn acc)))]))))]
    (fn [rows]
      (letfn [(init-aggs []
                (mapv (fn [[agg-k aggregate-fn]]
                        [agg-k aggregate-fn (volatile! (aggregate-fn))])
                      agg-fns))

              (step-aggs [^Map acc row]
                (let [group-accs (.computeIfAbsent acc (->group row)
                                                   (fn [_group]
                                                     (init-aggs)))]
                  (doseq [[_agg-k agg-fn !agg-acc] group-accs]
                    (vswap! !agg-acc agg-fn row))))]

        (let [acc (HashMap.)]
          (doseq [row rows]
            (step-aggs acc row))

          (for [[group-k group-accs] acc]
            ;; TODO get rid of this sort
            (let [group-accs-by-idx (sort-by (comp row-symbol->idx first) group-accs)]
              (into group-k
                    (for [[_agg-k agg-fn !agg-acc] group-accs-by-idx]
                      (agg-fn @!agg-acc))))))))))

(defn compile-find [conformed-find var->idx]
  (let [find-args (compile-find-args conformed-find)]
    (if-not (every? (comp empty? :aggregate) find-args)
      (let [row-symbol->idx (reduce (fn [row-symbol->idx {:keys [aggregate] :as _find-arg}]
                                      (if aggregate
                                        (assoc row-symbol->idx (first aggregate) (count row-symbol->idx))
                                        row-symbol->idx))
                                    var->idx
                                    find-args)]
        (agg-find-fn find-args row-symbol->idx))

      (let [value-fns (mapv (fn [{:keys [->result inputs]}] (row->result-value-fn ->result inputs var->idx)) find-args)]
        (fn [rows]
          (for [row rows]
            (mapv #(% row) value-fns)))))))

(defn- zipmap-fn [find keys var-to-index key-fn]
  (when (not= (count find) (count keys))
    (throw (IllegalArgumentException. "find and keys must have same size!")))
  (let [keys-in-var-order (->> (zipmap find keys)
                               (sort-by (comp var-to-index key))
                               (map (comp key-fn val))) ]
    (fn [row]
      (zipmap keys-in-var-order row))))

(defn query [{:keys [opts] :as db} query args]
  {:pre [(s/valid? ::query query) (validate-query (s/conform ::query query))]}
  (let [{:keys [find keys strs syms in where] :as conformed-query} (s/conform ::query query)
        vars-in-join-order (query->variable-order conformed-query)
        var->idx (zipmap vars-in-join-order (range))
        compiled-patterns (concat (in->iterators in var->idx args opts)
                                  (map (partial compile-pattern db vars-in-join-order) where))
        compiled-find (compile-find find var->idx)]
    (cond->> (join compiled-patterns (count vars-in-join-order) opts)
      true (compiled-find)
      (seq keys) (map (zipmap-fn find keys var->idx keyword))
      (seq strs) (map (zipmap-fn find strs var->idx str))
      (seq syms) (map (zipmap-fn find syms var->idx symbol))
      true set)))

(comment
  (def q '{:find [x y (count z)]
           :in [[x y]]
           :where [[x :person/name y]
                   [x :person/age z]]})


  (s/valid? ::query q)

  (s/conform ::query q)

  (s/unform ::query (s/conform ::query q))

  (variable-order (s/conform ::query q)))
