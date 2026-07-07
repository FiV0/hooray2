(ns hooray.dbsp
  "Standard DBSP incremental engine.

  Compiles a conjunctive Datalog query of standard triple patterns into a
  circuit of unary/binary operators (see `org.hooray.dbsp`), modelled on the
  Feldera `dbsp` crate."
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
           ;; query validation guarantees all branches share the same free
           ;; variable *set*, so the first branch is a correct representative.
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

  Tracks the set of variables grounded so far, seeded with [grounded] (empty
  for a top-level scope, the running relation's variables for a nested
  scope). A descriptor becomes introducible once every variable it cannot
  ground itself — `(set :vars) - (set :groundable)` — is grounded. Among the
  introducible descriptors the one sharing the most variables with the
  grounded set is appended next; ties and fully disconnected patterns, which
  become Cartesian joins, are broken by lowest original index. Throws
  `:db.error/insufficient-binding` when descriptors remain but none is
  introducible. Deterministic: the same input always yields the same order."
  ([patterns] (left-deep-order patterns #{}))
  ([patterns grounded]
   (loop [chosen []
          grounded (set grounded)
          remaining (vec patterns)]
     (if (empty? remaining)
       chosen
       (let [introducible? (fn [p]
                             (set/subset? (set/difference (set (:vars p))
                                                          (set (:groundable p)))
                                          grounded))
             candidates (filter introducible? remaining)]
         (if (empty? candidates)
           (let [p (apply min-key :index remaining)
                 unbound (first (sort (set/difference (set (:vars p))
                                                      (set (:groundable p))
                                                      grounded)))]
             (err/incorrect-ex (format "%s not bound" unbound)
                               {:unbound-var unbound :grounded grounded}
                               :db.error/insufficient-binding))
           (let [best (->> candidates
                           (sort-by (fn [p]
                                      [(- (count (set/intersection
                                                  grounded (set (:vars p)))))
                                       (:index p)]))
                           first)]
             (recur (conj chosen best)
                    (into grounded (:vars best))
                    (vec (remove #(= (:index %) (:index best)) remaining))))))))))

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
;; projects to the pattern's variable columns.
;;
;; A scope (the top-level `:where`, an `and`, an `or` branch, a `not` body)
;; plans as a left-deep chain `{:kind :chain :base <rel> :steps [<step> …]}`:
;; a self-contained base relation followed by steps that each extend the
;; running relation. The running stream is threaded through the steps at
;; assembly time; `(set (:out-vars acc))` is by construction the set of
;; variables grounded so far, which is what `left-deep-order` checks
;; introducibility against. Step ops:
;;
;;   {:op :join …}        joins a triple onto the running relation; the
;;                        `IncrementalJoin` keys on the leading columns shared
;;                        with the running relation, the triple's `:order` is
;;                        picked so its variables already lead correctly, and
;;                        a permuting Map is inserted when the running layout
;;                        does not
;;   {:op :union …}       or-continuation; every branch extends the same
;;                        running stream (so branches can anti-join outer
;;                        bindings) and the results are unioned
;;   {:op :difference …}  anti-joins a `not`'s relation off the running one
;;   {:op :permute …}     reorders to an explicit target layout

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
  "Relation plan for a scope-initial `or` descriptor (no running relation to
  extend, so the `or` must be self-groundable). Every branch is planned
  standalone with the same [target] variable order so the branch streams can
  be unioned directly. A non-initial `or` becomes a `:union` step instead
  (see `union-step`)."
  [descriptor target]
  {:kind :union
   :out-vars (vec target)
   :branches (mapv #(rel-plan % target) (:branches descriptor))})

(defn- and-plan
  "Relation plan for a standalone `and` descriptor. `and` is ordinary
  conjunction, so it lowers directly to a left-deep scope over its children."
  [descriptor target]
  (plan-inputs (:children descriptor) (vec target)))

(defn- not-plan
  "A bare `not` relation has no finite positive input domain. It can only be
  planned as a `:difference` step off a running relation."
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

(defn- join-step
  "A `:join` step: extends the running relation (layout [acc-vars]) with
  [descriptor]'s triple stream. The join's key column *order* is fixed by the
  running (left) layout, and the right relation is arranged to that same key
  order so both sides' leading columns line up."
  [acc-vars descriptor]
  (let [ki (set/intersection (set acc-vars) (set (:vars descriptor)))
        key-order (vec (filter ki acc-vars))
        left-needed (into key-order (remove ki acc-vars))
        permute (indices-of acc-vars left-needed)]
    {:op :join
     :right (rel-plan descriptor (into key-order (remove ki (:vars descriptor))))
     :key-arity (count ki)
     :key-vars key-order
     :left-permute (when (not= permute (vec (range (count acc-vars))))
                     permute)
     :out-vars (into left-needed (remove ki (:vars descriptor)))}))

(defn- difference-step
  "A `:difference` step: anti-joins the running relation (layout [acc-vars])
  with a `not` descriptor's relation as A - semijoin(A, distinct(keys(B))).

  The negative relation B is planned standalone, keyed on the `not`'s
  variables (all grounded by the running relation, per `left-deep-order`).
  A `not` body that is not itself self-groundable — e.g. a bare `not` inside
  an `or` inside this `not` — therefore still fails with an
  insufficient-binding error."
  [acc-vars descriptor]
  (let [negative-vars (:vars descriptor)
        missing-vars (seq (remove (set acc-vars) negative-vars))]
    (when missing-vars
      (throw (ex-info "not variables must be bound by the running relation"
                      {:not-vars negative-vars
                       :acc-vars acc-vars
                       :missing-vars (vec missing-vars)})))
    (let [key-vars (vec (filter (set negative-vars) acc-vars))]
      {:op :difference
       :negative (plan-inputs (:children descriptor) key-vars)
       :key-vars key-vars
       :keyed-vars (lead-with (set key-vars) acc-vars)
       :out-vars (vec acc-vars)})))

(defn- permute-step
  "A `:permute` step: reorders the running relation to [target-vars]."
  [acc-vars target-vars]
  {:op :permute
   :indices (indices-of acc-vars target-vars)
   :out-vars (vec target-vars)})

(declare extend-step)

(defn- union-step
  "A `:union` step: extends the running relation (layout [acc-vars]) with an
  `or` descriptor. Every branch continues the *same* running stream — this is
  what lets a branch's inner `not` key on variables from the outer scope, and
  a bare `not` branch anti-join the running relation itself. Each branch is a
  step vector; branch layouts are normalised to a common `:out-vars` (the
  running variables plus the variables the `or` grounds) so the branch
  streams can be unioned directly."
  [acc-vars descriptor]
  (let [out-vars (into (vec acc-vars) (remove (set acc-vars) (:vars descriptor)))]
    {:op :union
     :branches (mapv (fn [branch]
                       (let [{:keys [vars steps]} (extend-step {:vars (vec acc-vars) :steps []}
                                                               branch)]
                         (cond-> steps
                           (not= vars out-vars) (conj (permute-step vars out-vars)))))
                     (:branches descriptor))
     :out-vars out-vars}))

(defn- extend-step
  "Extends the running left-deep relation [acc] (`{:vars […] :steps […]}`)
  with one [descriptor], dispatching on its kind. `and` is ordinary
  conjunction, so its children are spliced into the current scope's chain."
  [acc descriptor]
  (if (= :and (:kind descriptor))
    (reduce extend-step acc (left-deep-order (:children descriptor) (set (:vars acc))))
    (let [step (case (:kind descriptor)
                 :triple (join-step (:vars acc) descriptor)
                 :or (union-step (:vars acc) descriptor)
                 :not (difference-step (:vars acc) descriptor))]
      (-> acc
          (assoc :vars (:out-vars step))
          (update :steps conj step)))))

(defn- plan-inputs
  "Plans [descriptors] as one left-deep scope: a standalone base relation
  extended step-by-step in `left-deep-order`. Returns the base relation
  directly when no steps are needed, otherwise a `:chain` node. With
  [target], the result is arranged to that variable order."
  ([descriptors] (plan-inputs descriptors nil))
  ([descriptors target]
   (let [target* (some-> target vec)
         ordered (left-deep-order descriptors #{})]
     (when (empty? ordered)
       (throw (ex-info "query has no patterns" {})))
     (when (not-descriptor? (first ordered))
       (err/unsupported-ex "DBSP-standard engine cannot plan `not` without a positive relation"
                           {:descriptors descriptors}))
     (let [d0 (first ordered)
           ;; lead the base with the first step's join key so the first join
           ;; needs no left permute.
           base-target (if-let [d1 (second ordered)]
                         (lead-with (set/intersection (set (:vars d0)) (set (:vars d1)))
                                    (:vars d0))
                         (or target* (:vars d0)))
           base (rel-plan d0 base-target)
           {:keys [vars steps]} (reduce extend-step
                                        {:vars (:out-vars base) :steps []}
                                        (rest ordered))
           steps (cond-> steps
                   (and target* (not= vars target*)) (conj (permute-step vars target*)))]
       (if (seq steps)
         {:kind :chain
          :base base
          :steps steps
          :out-vars (:out-vars (peek steps))}
         base)))))

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
(declare assemble-step)

(defn- assemble-union
  "Wires a standalone :union relation node into [circuit]: each branch is
  recursively assembled, the branch streams are folded left-to-right with
  `PlusOp`, and the union is fed into a `DistinctOp` to enforce set-union
  semantics. Returns

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

(defn- assemble-chain
  "Wires a :chain relation node into [circuit]: the base relation is
  assembled once and each step then extends the running stream."
  [^Circuit circuit {:keys [base steps]}]
  (reduce (fn [acc step] (assemble-step circuit acc step))
          (assemble-rel circuit base)
          steps))

(defn- assemble-join-step
  "Extends the running stream with a `:join` step: permute the running layout
  when the key columns do not already lead, then `IncrementalJoin` with the
  step's right relation."
  [^Circuit circuit
   {:keys [stream handles leaves]}
   {:keys [right key-arity left-permute out-vars]}]
  (let [left (if left-permute
               (.addUnary circuit (MapOp/permute (int-array left-permute)) stream)
               stream)
        right-wired (assemble-rel circuit right)
        joined (.addBinary circuit
                           (IncrementalJoinOp. (int key-arity) "incremental-join")
                           left
                           (:stream right-wired))]
    {:stream joined
     :vars out-vars
     :handles (into handles (:handles right-wired))
     :leaves (into leaves (:leaves right-wired))}))

(defn- assemble-difference-step
  "Extends the running stream A with a `:difference` step as
  A - semijoin(A, distinct(keys(B)))."
  [^Circuit circuit
   {:keys [stream vars handles leaves]}
   {:keys [negative key-vars keyed-vars out-vars]}]
  (let [negative-wired (assemble-rel circuit negative)
        positive-keyed (project-stream circuit stream vars keyed-vars)
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
     :handles (into handles (:handles negative-wired))
     :leaves (into leaves (:leaves negative-wired))}))

(defn- assemble-union-step
  "Extends the running stream with a `:union` step: the running stream fans
  out into every branch's step chain, the branch streams are folded
  left-to-right with `PlusOp`, and the union is fed into a `DistinctOp` to
  enforce set-union semantics."
  [^Circuit circuit
   {:keys [stream vars handles leaves]}
   {:keys [branches out-vars]}]
  (let [wired (mapv (fn [branch-steps]
                      (reduce (fn [acc step] (assemble-step circuit acc step))
                              {:stream stream :vars vars :handles [] :leaves []}
                              branch-steps))
                    branches)
        ;; union-step appends a :permute step when a branch layout differs,
        ;; so this is normally a no-op. Keep it as an assembly boundary guard.
        branch-streams (mapv #(project-stream circuit (:stream %) (:vars %) out-vars) wired)
        summed (reduce (fn [acc s] (.addBinary circuit (PlusOp.) acc s))
                       (first branch-streams)
                       (rest branch-streams))
        distinct-out (.addUnary circuit (DistinctOp.) summed)]
    {:stream distinct-out
     :vars out-vars
     :handles (into handles (mapcat :handles wired))
     :leaves (into leaves (mapcat :leaves wired))}))

(defn- assemble-step
  "Extends the running stream [acc] (`{:stream :vars :handles :leaves}`) with
  one planned step, dispatching on its `:op`."
  [^Circuit circuit acc step]
  (case (:op step)
    :join (assemble-join-step circuit acc step)
    :difference (assemble-difference-step circuit acc step)
    :union (assemble-union-step circuit acc step)
    :permute {:stream (.addUnary circuit
                                 (MapOp/permute (int-array (:indices step)))
                                 (:stream acc))
              :vars (:out-vars step)
              :handles (:handles acc)
              :leaves (:leaves acc)}))

(defn- assemble-rel
  "Dispatches relation assembly by [rel]'s `:kind`.
  Returns

   {:stream <Stream>
    :vars […]
    :handles [...]
    :leaves [...]}

  `:handles` and `:leaves` are equal-length flat vectors,
  one entry per leaf input triple."
  [^Circuit circuit rel]
  (case (:kind rel)
    :triple (assemble-triple circuit rel)
    :union  (assemble-union  circuit rel)
    :chain  (assemble-chain  circuit rel)))

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
