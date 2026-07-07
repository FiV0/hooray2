(ns hooray.dbsp
  "Standard DBSP incremental engine.

  Compiles a conjunctive Datalog query of standard triple patterns into a
  circuit of unary/binary operators (see `org.hooray.dbsp`), modelled on the
  Feldera `dbsp` crate. This is the `:standard` engine."
  (:require [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [hooray.db :as db]
            [hooray.error :as err]
            [hooray.query :as query]
            [hooray.transact :as t])
  (:import (org.hooray.dbsp Circuit DistinctOp FilterOp IncrementalJoinOp MapOp MinusOp PlusOp Tuple)
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
  "Compiles one conformed `where` clause into a pattern descriptor.

  Every descriptor has the following keys:
  -  :index      <position in the immediate parent clause>
  -  :kind       the descriptor kind
  -  :vars       the variables this pattern references
  -  :groundable variables this pattern references and can also ground

  (set vars) - (set groundable) must be bound by an outer scope.

  Triple descriptor:

    {:attr       {:kind :constant :value v}
     :entity     {:kind :constant :value v} | {:kind :variable :var s}
     :value      {:kind :constant :value v} | {:kind :variable :var s}}

  Or descriptor (nesting preserved, not flattened):

    {:kind       :or
     :branches   [<descriptor> …]}

  And descriptor:

    {:kind       :and
     :children   [<descriptor> …]}

  Not descriptor:

    {:kind       :not
     :children   [<descriptor> …]}

  Other clause types (`:predicate`, `:fn`) are not yet
  supported and trigger `err/unsupported-ex`."
  [index [clause-type pattern]]
  (case clause-type
    :triple (let [{:keys [e a v]} pattern
                  a* (elem a)
                  e* (elem e)
                  v* (elem v)
                  vars (vec (keep elem-var [e* v*]))]
              {:index index
               :kind :triple
               :attr a*
               :entity e*
               :value v*
               :vars vars
               :groundable vars})

    :or (let [branches (vec (map-indexed compile-pattern pattern))
              groundable (apply set/intersection (map (comp set :groundable) branches))]
          {:index index
           :kind :or
           :branches branches
           :vars (:vars (first branches))
           :groundable (vec groundable)})

    :and (let [children (vec (map-indexed compile-pattern pattern))]
           {:index index
            :kind :and
            :children children
            :vars (vec (distinct (mapcat :vars children)))
            :groundable (vec (distinct (mapcat :groundable children)))})

    :not (let [children (vec (map-indexed compile-pattern pattern))]
           {:index index
            :kind :not
            :children children
            :vars (vec (distinct (mapcat :vars children)))
            :groundable []})

    (err/unsupported-ex (format "DBSP-standard engine does not yet support `%s` clauses" (name clause-type))
                        {:clause-type clause-type :pattern pattern})))

(defn compile-patterns
  "Compiles every clause of a conformed `:where` into a vector of descriptors,
  indexed by original position."
  [conformed-where]
  (vec (map-indexed compile-pattern conformed-where)))

(defn- reject-unsupported-query-options
  [{:keys [in keys strs syms] :as query}]
  (when (contains? query :in)
    (throw (ex-info ":in clauses not supported for DBSP-standard queries yet"
                    {:in in})))
  (when (or (seq keys) (seq strs) (seq syms))
    (throw (ex-info ":keys, :strs, and :syms not supported for DBSP-standard queries yet"
                    {:keys keys :strs strs :syms syms}))))

;; --------------------------------------------------------------------------
;; Phase 1 — deterministic left-deep join order
;; --------------------------------------------------------------------------

(defn left-deep-order
  "Orders [patterns] (a vector of descriptors) into a left-deep join sequence.

  Starts from the first pattern and repeatedly appends the remaining pattern
  that shares the most variables with the patterns chosen so far; ties and
  fully disconnected patterns, which become Cartesian joins, are broken by
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
  "Conforms a raw query and returns

    {:find <conformed find>
     :patterns <descriptors>}.

   Throws on unsupported clauses."
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
;; pattern's Source emits 3-column `[a e v]` (`:aev`) or `[a v e]` (`:ave`)
;; tuples; a Filter drops rows that miss the pattern's constants; a Map
;; projects to the pattern's variable columns. Joins are left-deep: each
;; `IncrementalJoin` keys on the leading columns shared with the accumulated
;; result. The planner picks every pattern's `:order` so its variables already
;; lead correctly (no re-index for base patterns) and inserts a permuting Map
;; before each non-first join for the intermediate result.

(defn- variable? [el] (= :variable (:kind el)))

(defn- order-elems [descriptor order]
  (let [a (:attr descriptor)]
    (case order
      :aev [a (:entity descriptor) (:value descriptor)]
      :ave [a (:value descriptor) (:entity descriptor)])))

(defn- ordered-vars
  "The pattern's variables in [order]'s source tuple coordinates."
  [descriptor order]
  (->> (order-elems descriptor order)
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

(defn- triple-plan
  "Plan for one triple descriptor, producing its variables in [target] order.

    {:kind :triple
     :order :aev/:ave
     :filter …
     :project …
     :out-vars target}"
  [descriptor target]
  (let [order (choose-order descriptor target)]
    {:kind :triple
     ;; the order in which we are processing triples in this triple pattern
     :order order
     ;; the constant part of the triple pattern
     :filter (constant-filter descriptor order)
     ;; the projection after the filter of the constants
     ;; for example [a(constant) e(constant) v] -> [v]
     :project (projection descriptor order)
     ;; the vars of this pattern
     :out-vars (vec target)}))

(declare rel-plan)
(declare plan-inputs)

(defn- union-plan
  "Relation plan for an `or` descriptor. Every branch is planned with the same
  [target] variable order so the branch streams can be unioned directly."
  [descriptor target]
  {:kind :union
   :out-vars (vec target)
   :branches (mapv #(rel-plan % target) (:branches descriptor))})

(defn- and-plan
  "Relation plan for an `and` descriptor. `and` is ordinary conjunction, so it
  lowers directly to the existing join planner over its child relations."
  [descriptor target]
  (plan-inputs (:children descriptor) (vec target)))

(defn- not-plan
  "A bare `not` relation has no finite positive input domain. It can only be
  planned by `plan-inputs` after a positive relation has been built."
  [descriptor _target]
  (err/unsupported-ex "DBSP-standard engine cannot plan `not` without a positive relation"
                      {:descriptor descriptor}))

(defn- rel-plan
  "Plans one descriptor as a relation node that produces [target] variables."
  [descriptor target]
  (case (:kind descriptor)
    :triple (triple-plan descriptor target)
    :or (union-plan descriptor target)
    :and (and-plan descriptor target)
    :not (not-plan descriptor target)))

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
            (err/unsupported-ex "DBSP-standard engine does not support aggregates yet" {:find-element [t v]})))
        conformed-find))

(defn- not-descriptor? [descriptor]
  (= :not (:kind descriptor)))

(defn- anti-join-plan
  "Plans one Datalog `not` clause as A - semijoin(A, distinct(keys(B)))."
  [positive descriptor]
  (let [positive-vars (:out-vars positive)
        negative-vars (:vars descriptor)
        missing-vars (seq (remove (set positive-vars) negative-vars))]
    (when missing-vars
      (throw (ex-info "not variables must be bound by the positive relation"
                      {:not-vars negative-vars
                       :positive-vars positive-vars
                       :missing-vars (vec missing-vars)})))
    (let [key-vars (vec (filter (set negative-vars) positive-vars))
          keyed-vars (lead-with (set key-vars) positive-vars)]
      {:kind :difference
       :positive positive
       :negative (plan-inputs (:children descriptor) key-vars)
       :key-vars key-vars
       :keyed-vars keyed-vars
       :out-vars positive-vars})))

(defn- apply-not-plans [positive nots]
  (reduce anti-join-plan positive nots))

(defn- plan-inputs
  "Plans descriptors as one relation tree. Multiple descriptors become a
  left-deep `:join` node whose inputs are themselves relation nodes."
  ([descriptors] (plan-inputs descriptors nil))
  ([descriptors target]
   (let [target* (some-> target vec)
         positives (vec (remove not-descriptor? descriptors))
         nots (vec (filter not-descriptor? descriptors))
         ordered (left-deep-order positives)]
     (when (and (empty? ordered) (empty? nots))
       (throw (ex-info "query has no patterns" {})))
     (when (empty? ordered)
       (err/unsupported-ex "DBSP-standard engine cannot plan `not` without a positive relation"
                           {:descriptors descriptors}))
     (let [positive-plan
           (if (= 1 (count ordered))
             (rel-plan (first ordered) (or target* (:vars (first ordered))))
             (let [var-sets (mapv #(set (:vars %)) ordered)
                   ;; keys*[i-1] is the variable set joining the accumulated result
                   ;; with ordered[i].
                   keys* (loop [i 1, accset (first var-sets), ks []]
                           (if (>= i (count ordered))
                             ks
                             (recur (inc i)
                                    (set/union accset (nth var-sets i))
                                    (conj ks (set/intersection accset (nth var-sets i))))))
                   first-input (rel-plan (first ordered)
                                         (lead-with (first keys*) (:vars (first ordered))))]
               ;; Single left-to-right pass: each join's key column *order* is fixed by
               ;; the accumulated (left) layout, and the right relation is arranged to
               ;; that same key order so both sides' leading columns line up.
               (loop [i 1
                      acc (:out-vars first-input)
                      inputs [first-input]
                      steps []]
                 (if (>= i (count ordered))
                   (let [out-vars (or target* acc)
                         final-permute (when (and target* (not= acc target*))
                                         (indices-of acc target*))]
                     (cond-> {:kind :join
                              :out-vars out-vars
                              :inputs inputs
                              :steps steps}
                       final-permute (assoc :final-permute final-permute)))
                   (let [ki (nth keys* (dec i))
                         qi (nth ordered i)
                         key-order (vec (filter ki acc))
                         left-needed (into key-order (remove ki acc))
                         permute (indices-of acc left-needed)
                         qi-target (into key-order (remove ki (:vars qi)))
                         right-input (rel-plan qi qi-target)
                         out-vars (into left-needed (remove ki (:vars qi)))]
                     (recur (inc i)
                            out-vars
                            (conj inputs right-input)
                            (conj steps {:right-vars (:out-vars right-input)
                                         :key-arity (count ki)
                                         :key-vars key-order
                                         :left-permute (when (not= permute
                                                                   (vec (range (count acc))))
                                                         permute)
                                         :out-vars out-vars})))))))]
       (apply-not-plans positive-plan nots)))))

(defn plan
  "Builds the full circuit plan for [query]:

    {:find          [find vars]
     :where-plan    <relation plan>
     :result-vars   [layout produced by :where-plan]
     :final-permute [columns of :result-vars projected to :find]}"
  [query]
  (let [{:keys [find patterns]} (parse query)
        fvars (find-vars find)
        where-plan (plan-inputs patterns)
        result-vars (:out-vars where-plan)]
    {:find fvars
     :where-plan where-plan
     :result-vars result-vars
     :final-permute (indices-of result-vars fvars)}))

;; --------------------------------------------------------------------------
;; Phase 2 — circuit assembly
;; --------------------------------------------------------------------------

(defn- assemble-triple
  "Wires one triple pattern into [circuit]: Source -> Filter? -> Map(project).
  Returns

   {:stream <Stream>
    :vars […]
    :handles [<InputHandle>]
    :leaves [{:order …}]}."
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
    {:stream projected
     :vars (:out-vars pattern)
     :handles [handle]
     :leaves [{:order (:order pattern)}]}))

(defn- project-stream
  "Projects or reorders [stream] from [source-vars] to [target-vars]."
  [^Circuit circuit stream source-vars target-vars]
  (if (= (vec source-vars) (vec target-vars))
    stream
    (.addUnary circuit
               (MapOp/permute (int-array (indices-of source-vars target-vars)))
               stream)))

(declare assemble-rel)

(defn- assemble-union
  "Wires a :union relation node into [circuit]: each branch is recursively assembled,
  the branch streams are folded left-to-right with `PlusOp`, and the union is
  fed into a `DistinctOp` to enforce set-union semantics. Returns

   {:stream <Stream>
    :handles […]
    :leaves […]}

  with handles/leaves concatenated across all branches in plan order."
  [^Circuit circuit {:keys [branches out-vars]}]
  (let [wired (mapv #(assemble-rel circuit %) branches)
        ;; union-plan targets each branch at out-vars, so this is normally a no-op.
        ;; Keep the projection here as an assembly boundary guard.
        wired (mapv (fn [{:keys [stream vars] :as branch}]
                      (assoc branch :stream (project-stream circuit stream vars out-vars)
                                    :vars out-vars))
                    wired)
        summed (reduce (fn [acc {:keys [stream]}]
                         (.addBinary circuit (PlusOp.) acc stream))
                       (:stream (first wired))
                       (rest wired))
        distinct-out (.addUnary circuit (DistinctOp.) summed)]
    {:stream distinct-out
     :vars out-vars
     :handles (vec (mapcat :handles wired))
     :leaves (vec (mapcat :leaves wired))}))

(defn- assemble-join
  "Wires a :join relation node into [circuit]. Inputs are assembled recursively,
  then joined left-to-right according to the planned join steps."
  [^Circuit circuit {:keys [inputs steps out-vars final-permute]}]
  (let [wired (mapv #(assemble-rel circuit %) inputs)
        joined (loop [i 1
                      acc (:stream (first wired))]
                 (if (>= i (count inputs))
                   acc
                   (let [join (nth steps (dec i))
                         right (nth wired i)
                         left (if-let [lp (:left-permute join)]
                                (.addUnary circuit (MapOp/permute (int-array lp)) acc)
                                acc)
                         ;; plan-inputs targets each right input at :right-vars, so this is
                         ;; normally a no-op. Keep it as an assembly boundary guard.
                         right-stream (project-stream circuit
                                                      (:stream right)
                                                      (:vars right)
                                                      (:right-vars join))
                         joined (.addBinary circuit
                                            (IncrementalJoinOp. (int (:key-arity join))
                                                                "incremental-join")
                                            left
                                            right-stream)]
                     (recur (inc i) joined))))
        result (if final-permute
                 (.addUnary circuit (MapOp/permute (int-array final-permute)) joined)
                 joined)]
    {:stream result
     :vars out-vars
     :handles (vec (mapcat :handles wired))
     :leaves (vec (mapcat :leaves wired))}))

(defn- assemble-difference
  "Wires a :difference relation node as A - semijoin(A, distinct(keys(B)))."
  [^Circuit circuit {:keys [positive negative key-vars keyed-vars out-vars]}]
  (let [positive-wired (assemble-rel circuit positive)
        negative-wired (assemble-rel circuit negative)
        positive-keyed (project-stream circuit
                                       (:stream positive-wired)
                                       (:vars positive-wired)
                                       keyed-vars)
        negative-keys (project-stream circuit
                                      (:stream negative-wired)
                                      (:vars negative-wired)
                                      key-vars)
        distinct-negative-keys (.addUnary circuit (DistinctOp.) negative-keys)
        matched-left (.addBinary circuit
                                 (IncrementalJoinOp. (int (count key-vars))
                                                     "incremental-join")
                                 positive-keyed
                                 distinct-negative-keys)
        difference (.addBinary circuit (MinusOp. "difference") positive-keyed matched-left)
        result (project-stream circuit difference keyed-vars out-vars)]
    {:stream result
     :vars out-vars
     :handles (vec (concat (:handles positive-wired) (:handles negative-wired)))
     :leaves (vec (concat (:leaves positive-wired) (:leaves negative-wired)))}))

(defn- assemble-rel
  "Dispatches relation assembly by [rel]'s `:kind`.
  Returns

   {:stream <Stream>
    :handles [...]
    :leaves [...]}

  `:handles` and `:leaves` are equal-length flat vectors,
  one entry per leaf input triple."
  [^Circuit circuit rel]
  (case (:kind rel)
    :triple (assemble-triple circuit rel)
    :union  (assemble-union  circuit rel)
    :join   (assemble-join   circuit rel)
    :difference (assemble-difference circuit rel)))

(defn plan->circuit
  "Assembles a Kotlin [org.hooray.dbsp.Circuit] from a [plan]. Returns

    {:circuit <Circuit>
     :inputs  [<InputHandle> ...]   ; flat, one per leaf triple
     :leaves  [{:order …} ...]      ; parallel to :inputs
     :output  <OutputHandle>}

  The circuit is per-leaf `Source -> Filter? -> Map`, recursive relation assembly,
  and a final `Map` projecting to `:find`."
  [{:keys [where-plan final-permute]}]
  (let [circuit (Circuit.)
        wired (assemble-rel circuit where-plan)
        ;; wire up the find clause
        projected (.addUnary circuit (MapOp/permute (int-array final-permute)) (:stream wired))]
    {:circuit circuit
     :inputs (:handles wired)
     :leaves (:leaves wired)
     :output (.output circuit projected)}))

;; --------------------------------------------------------------------------
;; Phase 2 — per-pattern delta construction
;; --------------------------------------------------------------------------

(defn db->index-deltas
  "Given [db-before] and [tx-data], returns DBSP input deltas in index order:

    {:aev {[a e v] weight}
     :ave {[a v e] weight}}

  Retracts count only facts actually present in `db-before`; an `:add` to a
  cardinality-one attribute also retracts that entity's previous value (so an
  update shows up as `-1` for the old value and `+1` for the new)."
  [db-before tx-data]
  (let [{:keys [eav schema]} db-before
        {:keys [add retract]} (db/tx-data->triples db-before tx-data)
        bump-helper (fn [deltas order tuple dw]
                      (update deltas order
                              (fnil (fn [m]
                                      (let [w (+ (get m tuple 0) dw)]
                                        (if (zero? w)
                                          (dissoc m tuple)
                                          (assoc m tuple w))))
                                    {})))
        bump (fn [deltas a e v dw]
               (-> deltas
                   (bump-helper :aev [a e v] dw)
                   (bump-helper :ave [a v e] dw)))]
    (as-> {} deltas
      (reduce (fn [ds [e a v]]
                (if (contains? (get-in eav [e a]) v)
                  (bump ds a e v -1)
                  ds))
              deltas retract)
      (reduce (fn [ds [e a v]]
                (let [current-values (get-in eav [e a])
                      previous-v (first current-values)]
                  (case (t/attribute-cardinality schema a)
                    :db.cardinality/one
                    (cond
                      (= previous-v v) ds
                      (and (not (nil? previous-v))
                           ;; also check that that we are not explicitly retracting
                           (nil? (get-in ds [:aev [a e previous-v]])))
                      (-> ds
                          (bump a e previous-v -1)
                          (bump a e v 1))
                      :else (bump ds a e v 1))

                    :db.cardinality/many
                    (if (contains? current-values v)
                      ds
                      (bump ds a e v 1)))))
              deltas add))))

(defn- ->tuple ^Tuple [values]
  (Tuple/of (object-array values)))

(defn index-delta-zset
  "Converts DBSP input deltas into a flat `TupleZSet` in the given [order]
  (`:aev` => `[a e v]`, `:ave` => `[a v e]`)."
  ^ZSet [index-deltas order]
  (->> (get index-deltas order {})
       (reduce-kv (fn [m tuple w]
                    (assoc m (->tuple tuple) (IntegerWeight. (int w))))
                  {})
       (ZSet/fromMap)))

(defn- full-db-deltas
  "Treats every fact currently in [db] as a `+1` add — used to prime a freshly
  compiled circuit with the database's existing state."
  [db]
  (reduce (fn [m [e attrs]]
            (reduce (fn [m [a vs]]
                      (reduce (fn [m v]
                                (-> m
                                    (update-in [:aev [a e v]] (fnil + 0) 1)
                                    (update-in [:ave [a v e]] (fnil + 0) 1)))
                              m vs))
                    m attrs))
          {}
          (:eav db)))

;; --------------------------------------------------------------------------
;; Phase 3 — incremental query: compile, step, consume
;; --------------------------------------------------------------------------

(defn- push-deltas!
  "Pushes each leaf triple's delta (in its planned order) onto the circuit
  inputs. `:leaves` and `:inputs` are parallel flat vectors, one entry per
  leaf — for triple-only plans that's one entry per pattern; an `:or` block
  contributes one entry per branch."
  [{:keys [inputs leaves]} index-deltas]
  (dotimes [i (count leaves)]
    (.push ^org.hooray.dbsp.InputHandle (nth inputs i)
           (index-delta-zset index-deltas (:order (nth leaves i))))))

(defn- zset->result-set
  "Renders an output `TupleZSet` as a seq of `[tuple-vector weight]` pairs."
  [^ZSet zset]
  (mapv (fn [entry]
          [(vec (.toList ^Tuple (.getKey entry)))
           (.getValue ^IntegerWeight (.getValue entry))])
        (.entries zset)))

(defrecord DbspQuery [id query plan circuit inputs leaves output queue])

(defn dbsp-query?
  "True if [x] is a DBSP-standard incremental query (vs. a WCOJ one)."
  [x] (instance? DbspQuery x))

(defn compile-query
  "Compiles [query] into a stepping DBSP circuit, primed with the current state
  of [db]. Returns a [DbspQuery] carrying the circuit and a result queue."
  ^DbspQuery [db query]
  {:pre [(s/valid? ::query/query query) (query/validate-query (s/conform ::query/query query))]}
  (let [p (plan query)
        {:keys [circuit inputs leaves output]} (plan->circuit p)
        iq (->DbspQuery (random-uuid) query p circuit inputs leaves output
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
  (push-deltas! iq (db->index-deltas db-before tx-data))
  (.step ^Circuit (:circuit iq))
  (let [result (zset->result-set (.get ^org.hooray.dbsp.OutputHandle (:output iq)))]
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
