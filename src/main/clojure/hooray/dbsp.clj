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
  scope). A descriptor becomes introducible once every ungroundable variable
  (`(set :vars) - (set :groundable)`) is grounded by previous patterns. Among the
  introducible descriptors the one sharing the most variables with the
  grounded set is appended next; ties and fully disconnected patterns are
  broken by lowest original index. Throws `:db.error/insufficient-binding`
  when descriptors remain but none is introducible."
  ([patterns] (left-deep-order patterns #{}))
  ([patterns grounded]
   (loop [chosen []
          grounded (set grounded)
          remaining (vec patterns)]
     (if (empty? remaining)
       chosen
       (let [ungroundable (fn [p] (set/difference (set (:vars p)) (set (:groundable p))))
             introducible? (fn [p]
                             (set/subset? (ungroundable p) grounded))
             candidates (filter introducible? remaining)]
         (if (empty? candidates)
           (let [p (apply min-key :index remaining)
                 unbound (first (sort (set/difference (ungroundable p) grounded)))]
             (err/incorrect-ex (format "%s not bound" unbound)
                               {:unbound-var unbound :grounded grounded}
                               :db.error/insufficient-binding))
           (let [best (->> candidates
                           (sort-by (fn [p]
                                      [(- (count (set/intersection grounded (set (:vars p))))) (:index p)]))
                           first)]
             (recur (conj chosen best)
                    (into grounded (:vars best))
                    (vec (remove #{best} remaining))))))))))

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
;; tuples. A Filter drops rows that miss the pattern's constants; a DBSP Map
;; operator projects to the pattern's variable columns.
;;
;; The plan is a tree of nodes `{:kind … :out-vars […] …}` with kinds
;; `:triple`, `:chain`, `:union`, `:difference` and `:permute`. A node
;; optionally carries `:incoming`, the column layout of the running relation
;; it extends. Without `:incoming` a node produces its stream standalone,
;; rooted at its own Source(s); with it the node consumes the running stream:
;;
;;   :triple       the standard join; the `IncrementalJoin` keys on the
;;                 leading columns shared with the running relation, the
;;                 triple's `:order` is picked so its variables lead in that
;;                 key order, and a permuting Map operator (`:left-permute`)
;;                 is inserted when the running layout does not already lead
;;                 with the keys
;;   :chain        the running stream is the base of the chain; every child
;;                 extends its predecessor's output (child i's `:incoming`
;;                 is child i-1's `:out-vars`)
;;   :union        every branch extends the *same* running stream (so
;;                 branches can anti-join outer bindings) and the branch
;;                 streams are unioned
;;   :difference   anti-joins a `not`'s relation off the running one
;;   :permute      reorders the running stream to an explicit target layout;
;;                 never carries `:incoming` (it is a unary reordering of
;;                 whatever stream it follows)
;;
;; A scope (the top-level `:where`, an `and`, an `or` branch, a `not` body)
;; plans its descriptors in `left-deep-order`, each node extending its
;; predecessor's output; `(set (:out-vars <previous node>))` is by
;; construction the set of variables grounded so far, which is what
;; `left-deep-order` checks introducibility against. A scope that plans to
;; a single node (e.g. an `or` branch holding one triple) is that node
;; directly; only a multi-node scope is wrapped in a `:chain`.

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
     :filter <constant-filter for the incoming [a e v] tuples>
     :project <projection to the patterns variable columns>
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

(declare plan-node)
(declare plan-scope)

(defn- permute-node
  "A `:permute` node: reorders the running relation (layout [acc-vars]) to
  [target-vars]."
  [acc-vars target-vars]
  {:kind :permute
   :indices (indices-of acc-vars target-vars)
   :out-vars (vec target-vars)})

(defn- ensure-target
  "Arranges [node]'s output to the [target] layout: a no-op when [target] is
  nil or already matches, otherwise a `:permute` node is appended to the
  chain, or wrapped around the node."
  [node target]
  (let [target (some-> target vec)]
    (cond
      (or (nil? target) (= (:out-vars node) target))
      node

      (= :chain (:kind node))
      (-> node
          (update :children conj (permute-node (:out-vars node) target))
          (assoc :out-vars target))

      :else
      (cond-> {:kind :chain
               :children [node (permute-node (:out-vars node) target)]
               :out-vars target}
        (:incoming node) (assoc :incoming (:incoming node))))))

(defn- triple-join-node
  "A joining `:triple` node: extends the running relation (layout [incoming])
  with [descriptor]'s triple stream — the standard join. The join's key
  column *order* is fixed by the running (left) layout, and the triple's own
  source pipeline is arranged to that same key order so both sides' leading
  columns line up; `:left-permute` reorders the running relation when its
  key columns do not already lead."
  [descriptor incoming]
  (let [ki (set/intersection (set incoming) (set (:vars descriptor)))
        key-order (vec (filter ki incoming))
        left-needed (into key-order (remove ki incoming))
        permute (indices-of incoming left-needed)]
    (-> (triple-plan descriptor (into key-order (remove ki (:vars descriptor))))
        (assoc :incoming (vec incoming)
               :key-arity (count ki)
               :key-vars key-order
               :left-permute (when (not= permute (vec (range (count incoming))))
                               permute)
               :out-vars (into left-needed (remove ki (:vars descriptor)))))))

(defn- union-node
  "A `:union` node for an `or` descriptor. Every branch is planned against
  the same [incoming] layout — each branch extends the *same* running stream,
  which is what lets a branch's inner `not` key on variables from the outer
  scope and a bare `not` branch anti-join the running relation itself. All
  branches are arranged to the union's `:out-vars` (with [incoming] the
  running variables plus the variables the `or` grounds, standalone the
  requested [target]) so the branch streams can be unioned directly."
  [descriptor incoming target]
  (let [out-vars (if incoming
                   (into (vec incoming) (remove (set incoming) (:vars descriptor)))
                   (vec (or target (:vars descriptor))))]
    (cond-> {:kind :union
             :branches (mapv #(plan-node % incoming out-vars) (:branches descriptor))
             :out-vars out-vars}
      incoming (assoc :incoming (vec incoming)))))

(defn- difference-node
  "A `:difference` node for a `not` descriptor: anti-joins the running
  relation (layout [incoming]) with the `not`'s relation as
  A - semijoin(A, distinct(keys(B))).

  The negative relation B is planned standalone, keyed on the `not`'s
  variables (all grounded by the running relation, per `left-deep-order`).
  A `not` body that is not itself self-groundable — e.g. a bare `not` inside
  an `or` inside this `not` — therefore still fails with an
  insufficient-binding error. Without a running relation a `not` has no
  finite positive input domain and cannot be planned."
  [descriptor incoming]
  (when-not incoming
    (err/unsupported-ex "DBSP-standard engine cannot plan `not` without a positive relation"
                        {:descriptor descriptor}))
  (let [negative-vars (:vars descriptor)
        missing-vars (remove (set incoming) negative-vars)]
    (when (seq missing-vars)
      (throw (ex-info "not variables must be bound by the running relation"
                      {:not-vars negative-vars
                       :incoming (vec incoming)
                       :missing-vars (vec missing-vars)})))
    (let [key-vars (vec (filter (set negative-vars) incoming))]
      {:kind :difference
       :incoming (vec incoming)
       :negative (plan-scope (:children descriptor) nil key-vars)
       :key-vars key-vars
       :keyed-vars (lead-with (set key-vars) incoming)
       :out-vars (vec incoming)})))

(defn- plan-node
  "Plans one [descriptor] as a plan node. With [incoming] — the running
  relation's column layout — the node extends that relation; without it the
  node produces its stream standalone. With [target], the node's output is
  arranged to that variable order. `and` is ordinary conjunction, so it
  lowers directly to a left-deep scope over its children."
  [descriptor incoming target]
  (case (:kind descriptor)
    :triple (if incoming
              (ensure-target (triple-join-node descriptor incoming) target)
              (triple-plan descriptor (or target (:vars descriptor))))
    :or (ensure-target (union-node descriptor incoming target) target)
    :and (plan-scope (:children descriptor) incoming target)
    :not (ensure-target (difference-node descriptor incoming) target)))

(defn- plan-scope
  "Plans [descriptors] as one left-deep scope in `left-deep-order`. Without
  [incoming] the scope opens with a standalone base relation; with it every
  descriptor (the first included) extends the running relation. Returns the
  sole node directly when the scope plans to a single node, otherwise a
  `:chain`. With [target], the result is arranged to that variable order."
  [descriptors incoming target]
  (let [ordered (left-deep-order descriptors (set incoming))]
    (when (empty? ordered)
      (throw (ex-info "query has no patterns" {})))
    (let [;; a lone standalone descriptor can take the target directly;
          ;; a multi-descriptor scope plans its base in natural order and
          ;; reaches the target through a final :permute instead (the base
          ;; may not even carry all the target's variables).
          base-target (when (and (nil? incoming) (= 1 (count ordered)))
                        target)
          children (reduce (fn [children d]
                             (conj children (plan-node d (:out-vars (peek children)) nil)))
                           [(plan-node (first ordered) incoming base-target)]
                           (rest ordered))
          node (if (= 1 (count children))
                 (first children)
                 (cond-> {:kind :chain
                          :children children
                          :out-vars (:out-vars (peek children))}
                   incoming (assoc :incoming (vec incoming))))]
      (ensure-target node target))))

(defn plan
  "Builds the full circuit plan for [query]:

    {:find          [find vars]
     :where-plan    <relation plan>
     :result-vars   [layout produced by :where-plan]
     :final-permute [columns of :result-vars projected to :find]}"
  [query]
  (let [{:keys [find patterns]} (parse query)
        fvars (find-vars find)
        where-plan (plan-scope patterns nil nil)
        result-vars (:out-vars where-plan)]
    {:find fvars
     :where-plan where-plan
     :result-vars result-vars
     :final-permute (indices-of result-vars fvars)}))

;; --------------------------------------------------------------------------
;; Phase 2 — circuit assembly
;; --------------------------------------------------------------------------

(defn- assemble-triple
  "Wires a `:triple` node into [circuit]. Standalone (no running relation
  [acc]) the node is its own Source -> Filter? -> Map(project) pipeline;
  extending, the running stream (permuted when the key columns do not
  already lead) is `IncrementalJoin`ed with that pipeline."
  [^Circuit circuit node acc]
  (let [left (when acc
               (if-let [left-permute (:left-permute node)]
                 (.addUnary circuit (MapOp/permute (int-array left-permute)) (:stream acc))
                 (:stream acc)))
        pair (.addInput circuit)
        source (.getFirst pair)
        handle (.getSecond pair)
        constants (:filter node)
        filtered (if (seq constants)
                   (.addUnary circuit
                              (FilterOp/matchingConstants
                               (int-array (keys constants))
                               (object-array (vals constants)))
                              source)
                   source)
        projected (.addUnary circuit
                             (MapOp/permute (int-array (:project node)))
                             filtered)]
    (if acc
      {:stream (.addBinary circuit
                           (IncrementalJoinOp. (int (:key-arity node)) "incremental-join")
                           left
                           projected)
       :vars (:out-vars node)
       :handles (conj (:handles acc) handle)
       :leaves (conj (:leaves acc) {:order (:order node)})}
      {:stream projected
       :vars (:out-vars node)
       :handles [handle]
       :leaves [{:order (:order node)}]})))

(defn- project-stream
  "Projects or reorders [stream] from [source-vars] to [target-vars]."
  [^Circuit circuit stream source-vars target-vars]
  (if (= (vec source-vars) (vec target-vars))
    stream
    (.addUnary circuit
               (MapOp/permute (int-array (indices-of source-vars target-vars)))
               stream)))

(declare assemble-node)

(defn- assemble-union
  "Wires a `:union` node into [circuit]: every branch is assembled against
  the same running relation [acc] (or standalone when there is none), the
  branch streams are folded left-to-right with `PlusOp`, and the union is fed
  into a `DistinctOp` to enforce set-union semantics. Handles/leaves
  concatenate [acc]'s with every branch's in plan order."
  [^Circuit circuit {:keys [branches out-vars]} acc]
  (let [branch-acc (some-> acc (assoc :handles [] :leaves []))
        wired (mapv #(assemble-node circuit % branch-acc) branches)
        ;; the planner arranges every branch to out-vars, so this is normally
        ;; a no-op. Keep the projection here as an assembly boundary guard.
        branch-streams (mapv #(project-stream circuit (:stream %) (:vars %) out-vars) wired)
        summed (reduce (fn [acc-stream s] (.addBinary circuit (PlusOp.) acc-stream s))
                       (first branch-streams)
                       (rest branch-streams))
        distinct-out (.addUnary circuit (DistinctOp.) summed)]
    {:stream distinct-out
     :vars out-vars
     :handles (into (vec (:handles acc)) (mapcat :handles wired))
     :leaves (into (vec (:leaves acc)) (mapcat :leaves wired))}))

(defn- assemble-difference
  "Wires a `:difference` node into [circuit]: extends the running relation A
  as A - semijoin(A, distinct(keys(B))), with the negative relation B
  assembled standalone."
  [^Circuit circuit
   {:keys [negative key-vars keyed-vars out-vars]}
   {:keys [stream vars handles leaves]}]
  (let [negative-wired (assemble-node circuit negative nil)
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

(defn- assemble-node
  "Wires one plan node into [circuit] and returns

   {:stream <Stream>
    :vars […]
    :handles [...]
    :leaves [...]}

  [acc] (same shape) is the running relation the node extends, or nil when
  the node produces its stream standalone. `:handles` and `:leaves` are
  equal-length flat vectors, one entry per leaf input triple."
  [^Circuit circuit node acc]
  (when-let [incoming (:incoming node)]
    (assert (= (vec incoming) (vec (:vars acc)))
            (str "running relation layout " (:vars acc)
                 " does not match the node's planned :incoming " incoming)))
  (case (:kind node)
    :triple (assemble-triple circuit node acc)
    :chain (reduce (fn [running child] (assemble-node circuit child running))
                   acc
                   (:children node))
    :union (assemble-union circuit node acc)
    :difference (assemble-difference circuit node acc)
    :permute {:stream (.addUnary circuit
                                 (MapOp/permute (int-array (:indices node)))
                                 (:stream acc))
              :vars (:out-vars node)
              :handles (:handles acc)
              :leaves (:leaves acc)}))

(defn plan->circuit
  "Assembles a Kotlin [org.hooray.dbsp.Circuit] from a [plan]. Returns

    {:circuit <Circuit>
     :inputs  [<InputHandle> ...]   ; flat, one per leaf triple
     :leaves  [{:order …} ...]      ; parallel to :inputs
     :output  <OutputHandle>}

  The circuit is per-leaf `Source -> Filter? -> Map`, recursive node assembly,
  and a final `Map` projecting to `:find`."
  [{:keys [where-plan final-permute]}]
  (let [circuit (Circuit.)
        wired (assemble-node circuit where-plan nil)
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
