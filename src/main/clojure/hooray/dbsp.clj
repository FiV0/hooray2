(ns hooray.dbsp
  "Standard DBSP incremental engine.

  Compiles a conjunctive Datalog query of standard triple patterns into a
  circuit of unary/binary operators (see `org.hooray.dbsp`), modelled on the
  Feldera `dbsp` crate. This is the `:standard` engine; the `:wcoj` engine in
  `hooray.incremental` is unaffected."
  (:require [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [hooray.db :as db]
            [hooray.query :as query]
            [hooray.transact :as t])
  (:import (org.hooray.dbsp Circuit FilterOp MapOp IncrementalJoinOp Tuple)
           (org.hooray.incremental IntegerWeight ZSet)))

;; --------------------------------------------------------------------------
;; Phase 1 — pattern descriptors
;; --------------------------------------------------------------------------

(defn- elem
  "Normalises a conformed pattern element into `{:kind :constant :value v}` or
  `{:kind :variable :var s}`."
  [[etype evalue]]
  (case etype
    :constant {:kind :constant :value evalue}
    :variable {:kind :variable :var evalue}))

(defn- elem-var
  "The variable bound by [el], or nil if it is a constant."
  [el]
  (when (= :variable (:kind el)) (:var el)))

(defn compile-pattern
  "Compiles one conformed `where` clause into a triple-pattern descriptor:

    {:index   <position in :where>
     :attr    <constant attribute>
     :e       {:kind :constant :value v} | {:kind :variable :var s}
     :v       {:kind :constant :value v} | {:kind :variable :var s}
     :vars    [vars in [e v] column order]}

  The DBSP-standard engine only supports triple patterns with a constant
  attribute."
  [index [clause-type pattern]]
  (when-not (= :triple clause-type)
    (throw (ex-info "DBSP-standard engine supports only triple patterns"
                    {:clause-type clause-type :pattern pattern})))
  (let [{:keys [e a v]} pattern
        a* (elem a)
        e* (elem e)
        v* (elem v)]
    (when-not (= :constant (:kind a*))
      (throw (ex-info "DBSP-standard engine requires a constant attribute"
                      {:pattern pattern})))
    {:index index
     :attr (:value a*)
     :e e*
     :v v*
     :vars (vec (keep elem-var [e* v*]))}))

(defn compile-patterns
  "Compiles every clause of a conformed `:where` into a vector of descriptors,
  indexed by original position."
  [conformed-where]
  (vec (map-indexed compile-pattern conformed-where)))

(defn- reject-unsupported-query-options
  [{:keys [in keys strs syms]}]
  (when (seq in)
    (throw (ex-info "IN clauses not supported for DBSP-standard queries yet"
                    {:in in})))
  (when (or (seq keys) (seq strs) (seq syms))
    (throw (ex-info "KEYS, STRS, and SYMS not supported for DBSP-standard queries yet"
                    {:keys keys :strs strs :syms syms}))))

;; --------------------------------------------------------------------------
;; Phase 1 — deterministic left-deep join order
;; --------------------------------------------------------------------------

(defn left-deep-order
  "Orders [patterns] (a vector of descriptors) into a left-deep join sequence.

  Starts from the first pattern and repeatedly appends the remaining pattern
  that shares the most variables with the patterns chosen so far; ties — and
  fully disconnected patterns, which become Cartesian joins — are broken by
  lowest original index. Deterministic: the same input always yields the same
  order."
  [patterns]
  (when (seq patterns)
    (loop [chosen [(first patterns)]
           chosen-vars (set (:vars (first patterns)))
           remaining (vec (rest patterns))]
      (if (empty? remaining)
        chosen
        (let [best (->> remaining
                        (sort-by (fn [p]
                                   [(- (count (set/intersection
                                               chosen-vars (set (:vars p)))))
                                    (:index p)]))
                        first)]
          (recur (conj chosen best)
                 (set/union chosen-vars (set (:vars best)))
                 (vec (remove #(= (:index %) (:index best)) remaining))))))))

(defn parse
  "Conforms a raw query and returns `{:find <conformed find> :patterns
  <descriptors>}`. Throws on unsupported clauses."
  [query]
  (let [conformed (s/conform ::query/query query)]
    (when (= ::s/invalid conformed)
      (throw (ex-info "Invalid query" {:query query})))
    (query/validate-query conformed)
    (reject-unsupported-query-options conformed)
    {:find (:find conformed)
     :patterns (compile-patterns (:where conformed))}))

;; --------------------------------------------------------------------------
;; Phase 1 — full join plan
;; --------------------------------------------------------------------------
;;
;; Tuples flow through the circuit as positional value vectors. A base
;; pattern's Source emits 2-column `[e v]` (`:aev`) or `[v e]` (`:ave`) tuples;
;; a Filter drops rows that miss the pattern's constants; a Map projects to the
;; pattern's variable columns. Joins are left-deep: each `IncrementalJoin` keys
;; on the leading columns shared with the accumulated result. The planner picks
;; every pattern's `:order` so its variables already lead correctly (no
;; re-index for base patterns) and inserts a permuting Map before each
;; non-first join for the intermediate result.

(defn- variable? [el] (= :variable (:kind el)))

(defn- ordered-vars
  "The pattern's variables in [order] permutation (`:aev` = e then v)."
  [descriptor order]
  (->> (case order
         :aev [(:e descriptor) (:v descriptor)]
         :ave [(:v descriptor) (:e descriptor)])
       (keep #(when (variable? %) (:var %)))
       vec))

(defn- choose-order
  "Picks `:aev` or `:ave` so the pattern's variables emerge in [target] order."
  [descriptor target]
  (condp = (vec target)
    (ordered-vars descriptor :aev) :aev
    (ordered-vars descriptor :ave) :ave
    (throw (ex-info "pattern variables cannot be arranged to the join target"
                    {:descriptor descriptor :target target}))))

(defn- order-elems [descriptor order]
  (case order
    :aev [(:e descriptor) (:v descriptor)]
    :ave [(:v descriptor) (:e descriptor)]))

(defn- constant-filter
  "Map of source-column -> required constant value, in [order] coordinates."
  [descriptor order]
  (into (sorted-map)
        (keep-indexed (fn [i el] (when-not (variable? el) [i (:value el)]))
                      (order-elems descriptor order))))

(defn- projection
  "Source columns holding variables, in [order] coordinates."
  [descriptor order]
  (vec (keep-indexed (fn [i el] (when (variable? el) i))
                     (order-elems descriptor order))))

(defn- pattern-plan
  "Plan for one base pattern, producing its variables in [target] order."
  [descriptor target]
  (let [order (choose-order descriptor target)]
    {:descriptor descriptor
     :order order
     :filter (constant-filter descriptor order)
     :project (projection descriptor order)
     :out-vars (vec target)}))

(defn- lead-with
  "Reorders [layout] so the members of [key-set] (in layout order) come first."
  [key-set layout]
  (vec (concat (filter key-set layout) (remove key-set layout))))

(defn- indices-of
  "For each variable in [targets], its position in [layout]."
  [layout targets]
  (let [pos (zipmap layout (range))]
    (mapv (fn [v]
            (or (pos v)
                (throw (ex-info "variable not present in layout"
                                {:var v :layout layout}))))
          targets)))

(defn- find-vars [conformed-find]
  (mapv (fn [[t v]]
          (if (= t :variable)
            v
            (throw (ex-info "DBSP-standard engine does not support aggregates yet"
                            {:find-element [t v]}))))
        conformed-find))

(defn plan
  "Builds the full circuit plan for [query]:

    {:find          [find vars]
     :patterns      [{:descriptor :order :filter :project :out-vars} ...]  ; join order
     :joins         [{:key-arity :key-vars :left-permute :out-vars} ...]   ; one per join
     :result-vars   [layout after the last join]
     :final-permute [columns of :result-vars projected to :find]}

  `:joins` has one entry per pattern after the first; join `i` joins the
  accumulated result with `:patterns[i]`. `:left-permute` is nil when the
  accumulated result already leads with the join key (always so for the first
  join), otherwise the column order the intermediate Map must produce."
  [query]
  (let [{:keys [find patterns]} (parse query)
        ordered (left-deep-order patterns)
        fvars (find-vars find)]
    (when (empty? ordered)
      (throw (ex-info "query has no patterns" {:query query})))
    (if (= 1 (count ordered))
      (let [pp (pattern-plan (first ordered) (:vars (first ordered)))]
        {:find fvars
         :patterns [pp]
         :joins []
         :result-vars (:out-vars pp)
         :final-permute (indices-of (:out-vars pp) fvars)})
      (let [var-sets (mapv #(set (:vars %)) ordered)
            ;; keys*[i-1] is the variable set joining the accumulated result
            ;; with ordered[i].
            keys* (loop [i 1, accset (first var-sets), ks []]
                    (if (>= i (count ordered))
                      ks
                      (recur (inc i)
                             (set/union accset (nth var-sets i))
                             (conj ks (set/intersection accset (nth var-sets i))))))
            pp0 (pattern-plan (first ordered)
                              (lead-with (first keys*) (:vars (first ordered))))]
        ;; Single left-to-right pass: each join's key column *order* is fixed by
        ;; the accumulated (left) layout, and the right pattern is arranged to
        ;; that same key order so both sides' leading columns line up.
        (loop [i 1
               acc (:out-vars pp0)
               pattern-plans [pp0]
               joins []]
          (if (>= i (count ordered))
            {:find fvars
             :patterns pattern-plans
             :joins joins
             :result-vars acc
             :final-permute (indices-of acc fvars)}
            (let [ki (nth keys* (dec i))
                  qi (nth ordered i)
                  key-order (vec (filter ki acc))
                  left-needed (into key-order (remove ki acc))
                  permute (indices-of acc left-needed)
                  qi-target (into key-order (remove ki (:vars qi)))
                  out-vars (into left-needed (remove ki (:vars qi)))]
              (recur (inc i)
                     out-vars
                     (conj pattern-plans (pattern-plan qi qi-target))
                     (conj joins {:key-arity (count ki)
                                  :key-vars key-order
                                  :left-permute (when (not= permute
                                                             (vec (range (count acc))))
                                                  permute)
                                  :out-vars out-vars})))))))))

;; --------------------------------------------------------------------------
;; Phase 2 — circuit assembly
;; --------------------------------------------------------------------------

(defn- assemble-pattern
  "Wires one base pattern into [circuit]: Source -> Filter? -> Map(project).
  Returns `{:stream <Stream> :handle <InputHandle>}`."
  [^Circuit circuit pattern]
  (let [pair (.addInput circuit)
        source (.getFirst pair)
        handle (.getSecond pair)
        constants (:filter pattern)
        filtered (if (seq constants)
                   (.addUnary circuit
                              (FilterOp/matchingConstants
                               (int-array (keys constants))
                               (object-array (vals constants)))
                              source)
                   source)
        projected (.addUnary circuit
                             (MapOp/permute (int-array (:project pattern)))
                             filtered)]
    {:stream projected :handle handle}))

(defn plan->circuit
  "Assembles a Kotlin [org.hooray.dbsp.Circuit] from a [plan]. Returns

    {:circuit <Circuit>
     :inputs  [<InputHandle> ...]   ; parallel to (:patterns plan)
     :output  <OutputHandle>}

  The circuit is per-pattern `Source -> Filter? -> Map`, a left-deep chain of
  `IncrementalJoin`s (each non-first join preceded by a permuting `Map` when the
  plan calls for one), and a final `Map` projecting to `:find`."
  [{:keys [patterns joins final-permute]}]
  (let [circuit (Circuit.)
        wired (mapv #(assemble-pattern circuit %) patterns)
        result (loop [i 1
                      acc (:stream (first wired))]
                 (if (>= i (count patterns))
                   acc
                   (let [join (nth joins (dec i))
                         left (if-let [lp (:left-permute join)]
                                (.addUnary circuit (MapOp/permute (int-array lp)) acc)
                                acc)
                         joined (.addBinary circuit
                                            (IncrementalJoinOp. (int (:key-arity join))
                                                                "incremental-join")
                                            left
                                            (:stream (nth wired i)))]
                     (recur (inc i) joined))))
        projected (.addUnary circuit (MapOp/permute (int-array final-permute)) result)]
    {:circuit circuit
     :inputs (mapv :handle wired)
     :output (.output circuit projected)}))

;; --------------------------------------------------------------------------
;; Phase 2 — per-pattern delta construction
;; --------------------------------------------------------------------------

(defn attribute-deltas
  "Given [db-before] and [tx-data], returns `{attr -> {[e v] -> weight}}` — the
  per-attribute change to the set of `(e, v)` facts.

  Retracts count only facts actually present in `db-before`; an `:add` to a
  cardinality-one attribute also retracts that entity's previous value (so an
  update shows up as `-1` for the old value and `+1` for the new)."
  [db-before tx-data]
  (let [{:keys [eav schema]} db-before
        {:keys [add retract]} (db/tx-data->triples db-before tx-data)
        bump (fn [deltas a e v dw]
               (update-in deltas [a [e v]] (fnil + 0) dw))]
    (as-> {} deltas
      (reduce (fn [ds [e a v]]
                (if (contains? (get-in eav [e a]) v)
                  (bump ds a e v -1)
                  ds))
              deltas retract)
      (reduce (fn [ds [e a v]]
                (-> (if (and (= :db.cardinality/one (t/attribute-cardinality schema a))
                             (first (get-in eav [e a])))
                      (bump ds a e (first (get-in eav [e a])) -1)
                      ds)
                    (cond->
                     (not (contains? (get-in eav [e a]) v))
                      (bump a e v 1))))
              deltas add))))

(defn- ->tuple ^Tuple [values]
  (Tuple/of (object-array values)))

(defn pattern-delta-zset
  "Converts an attribute delta `{[e v] -> weight}` into a flat `TupleZSet` in
  the given [order] (`:aev` => `[e v]`, `:ave` => `[v e]`)."
  ^ZSet [attr-delta order]
  (->> attr-delta
       (reduce-kv (fn [m [e v] w]
                    (assoc m
                           (->tuple (case order :aev [e v] :ave [v e]))
                           (IntegerWeight. (int w))))
                  {})
       (ZSet/fromMap)))

(defn- full-db-deltas
  "Treats every fact currently in [db] as a `+1` add — used to prime a freshly
  compiled circuit with the database's existing state."
  [db]
  (reduce (fn [m [e attrs]]
            (reduce (fn [m [a vs]]
                      (reduce (fn [m v] (update-in m [a [e v]] (fnil + 0) 1)) m vs))
                    m attrs))
          {}
          (:eav db)))

;; --------------------------------------------------------------------------
;; Phase 3 — incremental query: compile, step, consume
;; --------------------------------------------------------------------------

(defn- push-deltas!
  "Pushes each pattern's delta (in its planned order) onto the circuit inputs."
  [{:keys [plan inputs]} attr-deltas]
  (doseq [[i pattern] (map-indexed vector (:patterns plan))]
    (let [attr (:attr (:descriptor pattern))]
      (.push ^org.hooray.dbsp.InputHandle (nth inputs i)
             (pattern-delta-zset (get attr-deltas attr {}) (:order pattern))))))

(defn- format-result
  "Renders an output `TupleZSet` as a seq of `[tuple-vector weight]` pairs."
  [^ZSet zset]
  (mapv (fn [entry]
          [(vec (.toList ^Tuple (.getKey entry)))
           (.getValue ^IntegerWeight (.getValue entry))])
        (.entries zset)))

(defrecord DbspQuery [query plan circuit inputs output queue])

(defn dbsp-query?
  "True if [x] is a DBSP-standard incremental query (vs. a WCOJ one)."
  [x]
  (instance? DbspQuery x))

(defn compile-query
  "Compiles [query] into a stepping DBSP circuit, primed with the current state
  of [db]. Returns a [DbspQuery] carrying the circuit and a result queue."
  ^DbspQuery [db query]
  (let [p (plan query)
        {:keys [circuit inputs output]} (plan->circuit p)
        iq (->DbspQuery query p circuit inputs output
                        (atom clojure.lang.PersistentQueue/EMPTY))]
    ;; prime the circuit with the database's existing facts; discard the output
    (push-deltas! iq (full-db-deltas db))
    (.step ^Circuit circuit)
    iq))

(defn compute-delta!
  "Feeds the change described by [tx-data] (against [db-before]) through the
  incremental query [iq], queues the resulting delta, and returns it as a seq
  of `[tuple weight]` pairs."
  [iq db-before tx-data]
  (push-deltas! iq (attribute-deltas db-before tx-data))
  (.step ^Circuit (:circuit iq))
  (let [result (format-result (.get ^org.hooray.dbsp.OutputHandle (:output iq)))]
    (when (seq result)
      (swap! (:queue iq) conj result))
    result))

(defn pop-result!
  "Removes and returns the oldest queued delta, or nil if none is pending."
  [iq]
  (let [q @(:queue iq)]
    (when (seq q)
      (swap! (:queue iq) pop)
      (peek q))))
