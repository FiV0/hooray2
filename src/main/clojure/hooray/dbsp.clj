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
            [hooray.transact :as t]
            [hooray.util :as util :refer [distinctv]])
  (:import (java.util.function Function Predicate)
           (org.hooray.dbsp Circuit DistinctOp FilterOp IncrementalJoinOp MapOp MinusOp PlusOp Tuple)
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

  Predicate descriptor:

    {:kind       :predicate
     :predicate  <symbol>
     :args       [{:kind :constant :value v} | {:kind :variable :var s} …]}

  Fn descriptor:

    {:kind       :fn
     :fn         <symbol>
     :args       [{:kind :constant :value v} | {:kind :variable :var s} …]
     :ret-var    <symbol>}

  Any other clause type triggers `err/unsupported-ex`."
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
            :vars (distinctv (mapcat :vars children))
            :groundable (distinctv (mapcat :groundable children))})

    :not (let [children (vec (map-indexed compile-pattern pattern))]
           {:index index
            :kind :not
            :children children
            :vars (distinctv (mapcat :vars children))
            :groundable []})

    :predicate (let [{:keys [predicate args]} pattern
                     args* (mapv elem args)]
                 {:index index
                  :kind :predicate
                  :predicate predicate
                  :args args*
                  :vars (distinctv (keep elem-var args*))
                  :groundable []})

    :fn (let [[{:keys [fun args]} ret-var] pattern
              args* (mapv elem args)
              arg-vars (vec (keep elem-var args*))]
          (when-not (<= 1 (count args*) 2)
            (err/unsupported-ex "DBSP-standard engine only supports unary and binary functions"
                                {:fn fun :args args :ret-var ret-var}))
          {:index index
           :kind :fn
           :fn fun
           :args args*
           :ret-var ret-var
           :vars (distinctv (conj arg-vars ret-var))
           :groundable (if (some #{ret-var} arg-vars) [] [ret-var])})

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
;; `:triple`, `:chain`, `:union`, `:difference`, `:permute`, `:filter` and
;; `:function`. A node
;; optionally carries `:incoming`, the column layout of the running relation
;; it extends — the stream of the already-built left partial sub-tree of the
;; query. Without `:incoming` a node produces its stream standalone, rooted
;; at its own Source(s); with it the node consumes the running stream:
;;
;;   :triple       without `:incoming` simply produces its values as a
;;                 stream; with it, a standard join with the running
;;                 relation (a cartesian product when there is no variable
;;                 overlap)
;;   :chain        with `:incoming` the running stream is the base of the
;;                 chain, otherwise the first child is; every child extends
;;                 its predecessor's output (child i's `:incoming` is child
;;                 i-1's `:out-vars`)
;;   :union        with `:incoming` every branch extends the *same* running
;;                 stream; without it every branch produces the same column
;;                 set standalone; in both cases the branch streams are
;;                 unioned
;;   :difference   anti-joins a `not`'s relation off the running one
;;                 (`:incoming` is required); the not body is itself planned
;;                 against the running relation's key columns
;;   :permute      reorders the running stream to an explicit target layout;
;;                 never carries `:incoming` (it is a unary reordering of
;;                 whatever stream it follows)
;;   :filter       applies a stateless predicate to the running stream
;;                 (`:incoming` is required); rows that fail are dropped,
;;                 the layout is unchanged (out-vars = incoming)
;;   :function     computes a function clause over the running stream. When
;;                 its result variable is absent from `:incoming`, it appends
;;                 the computed value as a new trailing column. When the result
;;                 variable is already present, it keeps only rows where that
;;                 column equals the computed value (`:incoming` is required)
;;
;; A scope (the top-level `:where`, an `and` and a `not` body)
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

(defmulti ^:private plan-node
  "Plans one [descriptor] as a plan node, dispatching on its `:kind`. With
  [incoming] being the running relation stream column layout. The node extends
  that relation; without it the node produces its stream standalone. With
  [target], each method arranges the node's output to that variable order."
  (fn [descriptor _incoming _target] (:kind descriptor)))

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
     :order order
     :filter (constant-filter descriptor order)
     ;; the projection after the filter of the constants
     ;; for example [a(constant) e(constant) v] -> [v]
     :project (projection descriptor order)
     :out-vars (vec target)}))


;; A `:triple` node either produces values by itself (nil `incoming`) or
;; extends/joins the running relation (layout [incoming]) with the values
;; produced by this triple. The key column *order* needed for a join is fixed
;; via the incoming stream (:aev vs. :ave). The triple's own source pipeline
;; is arranged to that same key order so both sides' leading columns line up.
;; `:left-permute` reorders the running relation when the key columns do not already lead.

;; TODO The :left-permute value of has the same purpose as the :permute node.
;; Maybe extract that logic into separate :permute, :join and :triple nodes.
(defmethod plan-node :triple
  [descriptor incoming target]
  (if-not incoming
    (triple-plan descriptor (or target (:vars descriptor)))
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
                 :out-vars (into left-needed (remove ki (:vars descriptor))))
          (ensure-target target)))))

;; For an `or` descriptor an `:union` node is planned. Every branch is planned against
;; the same [incoming] layout — each branch extends the *same* running stream.
;; This is what lets a branch's inner predicates or `not` patterns use the
;; variables from the outer scope. All branches are arranged to the
;; union's `:out-vars`, so the branch streams can be unioned directly.
(defmethod plan-node :or
  [descriptor incoming target]
  (let [out-vars (if incoming
                   (into (vec incoming) (remove (set incoming) (:vars descriptor)))
                   (vec (or target (:vars descriptor))))]
    (cond-> {:kind :union
             :branches (mapv #(plan-node % incoming out-vars) (:branches descriptor))
             :out-vars out-vars}
      incoming (assoc :incoming (vec incoming))
      target (ensure-target target))))

(declare plan-scope)

;; `and` is ordinary conjunction, so it lowers directly to a left-deep tree
;; over its children; `plan-scope` arranges to [target] itself.
(defmethod plan-node :and
  [descriptor incoming target]
  (plan-scope (:children descriptor) incoming target))

;; A `:difference` node for a `not` descriptor anti-joins the running
;; stream (which is required in this case) with the `not`'s relation as
;;   A - semijoin(A, distinct(keys(B))).

;; The negative relation B is planned against the running relation's key
;; columns, so the `not` body patterns that cannot produce variables themselves
;; (predicates) can still plan against the outer bindings. A `not` body itself
;; has a running relation.
(defmethod plan-node :not
  [descriptor incoming target]
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
      (cond-> {:kind :difference
               :incoming (vec incoming)
               ;; the difference operator needs to assure the incoming relation
               ;; is reshuffled into leading key-vars before being joined with the negative.
               :negative (plan-scope (:children descriptor) key-vars key-vars)
               :key-vars key-vars
               :keyed-vars (lead-with (set key-vars) incoming)
               :out-vars (vec incoming)}
        target (ensure-target target)))))

;; A predicate cannot produce rows on its own. It filters the running relation
;; in place. Its `:groundable` is empty, so `left-deep-order` only schedules it
;; once all its variables are grounded; [incoming] is nil only when there is no
;; positive relation to filter.
(defmethod plan-node :predicate
  [descriptor incoming target]
  (when-not incoming
    (err/unsupported-ex "DBSP-standard engine cannot plan a predicate without a positive relation"
                        {:descriptor descriptor}))
  (cond-> {:kind :filter
           :incoming (vec incoming)
           :predicate descriptor
           :out-vars (vec incoming)}
    target (ensure-target target)))

;; A function clause extends the running relation row-by-row. The single
;; `:function` plan node records whether its result variable is already present
;; in [incoming] through its input and output layouts; circuit construction
;; chooses whether to append the result or join it with the existing binding.
;; [incoming] is nil only when there is no positive relation to extend.
(defmethod plan-node :fn
  [{:keys [ret-var] :as descriptor} incoming target]
  (when-not incoming
    (err/unsupported-ex "DBSP-standard engine cannot plan a function without a positive relation"
                        {:descriptor descriptor}))
  (let [incoming (vec incoming)
        out-vars (cond-> incoming
                   (not (some #{ret-var} incoming)) (conj ret-var))]
    (cond-> {:kind :function
             :incoming incoming
             :function descriptor
             :out-vars out-vars}
      target (ensure-target target))))

(defn- plan-scope
  "Plans [descriptors] as one left-deep tree in `left-deep-order`. Without
  [incoming] the scope opens with a standalone base relation that must produce a stream.
  With it every descriptor (the first included) extends the running relation stream
  (that is going to be available at circuit construction). Returns the
  sole node directly when the scope plans to a single node, otherwise a
  `:chain`. With [target], the result is arranged to that variable order."
  [descriptors incoming target]
  (let [ordered (left-deep-order descriptors (set incoming))]
    (cond
      (empty? ordered)
      (throw (ex-info "query has no patterns" {}))

      (= 1 (count ordered))
      (plan-node (first ordered) incoming target)

      :else (let [children (reduce (fn [children d]
                                     (conj children (plan-node d (:out-vars (peek children)) nil)))
                                   [(plan-node (first ordered) incoming nil)]
                                   (rest ordered))]
              (cond-> {:kind :chain
                       :children children
                       :out-vars (:out-vars (peek children))}
                incoming (assoc :incoming (vec incoming))
                target (ensure-target target))))))

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

(defn- project-stream
  "Projects or reorders [stream] from [source-vars] to [target-vars]."
  [^Circuit circuit stream source-vars target-vars]
  (if (= (vec source-vars) (vec target-vars))
    stream
    (.addUnary circuit
               (MapOp/project (int-array (indices-of source-vars target-vars)))
               stream)))

(defn- check-incoming!
  "Asserts the running relation [acc]'s layout matches the layout the node
  was planned against (`:incoming`)."
  [node acc]
  (when-let [incoming (:incoming node)]
    (assert (= (vec incoming) (vec (:vars acc)))
            (str "running relation layout " (:vars acc)
                 " does not match the node's planned :incoming " incoming))))

(defmulti ^:private assemble-node
  "Wires one plan node into [circuit], dispatching on the node's `:kind`,
  and returns

   {:stream <Stream>
    :vars […]
    :leaves [{:order … :handle <InputHandle>} …]}

  [acc] (same shape) is the running relation the node extends, or nil when
  the node produces its stream standalone. `:leaves` is a flat vector with
  one entry per leaf input triple."
  (fn [_circuit node _acc] (:kind node)))

;; Standalone (no running relation acc) a `:triple` node is its own
;; Source -> Filter? -> Map(project) pipeline. The triple pattern is joined with
;; the running stream (permuted when the key columns do not already lead) when acc is given.
(defmethod assemble-node :triple
  [^Circuit circuit {:keys [out-vars order left-permute filter project] :as node} acc]
  (check-incoming! node acc)
  (let [left (when acc
               (if left-permute
                 (.addUnary circuit (MapOp/project (int-array left-permute)) (:stream acc))
                 (:stream acc)))
        pair (.addInput circuit)
        source (.getFirst pair)
        handle (.getSecond pair)
        constants filter
        filtered (if (seq constants)
                   (.addUnary circuit
                              (FilterOp/matchingConstants
                               (int-array (keys constants))
                               (object-array (vals constants)))
                              source)
                   source)
        projected (.addUnary circuit
                             (MapOp/project (int-array project))
                             filtered)]
    (if acc
      {:stream (.addBinary circuit
                           (IncrementalJoinOp. (int (:key-arity node)) "incremental-join")
                           left
                           projected)
       :vars out-vars
       :leaves (conj (:leaves acc) {:order order :handle handle})}
      {:stream projected
       :vars out-vars
       :leaves [{:order order :handle handle}]})))

;; The running relation threads through a `:chain`'s children: the first
;; child sees the chain's [acc] (nil for a standalone chain), each
;; subsequent child its predecessor's output.
(defmethod assemble-node :chain
  [^Circuit circuit node acc]
  (check-incoming! node acc)
  (reduce (fn [running child] (assemble-node circuit child running))
          acc
          (:children node)))

;; Every branch of a `:union` is assembled against the same running relation
;; [acc] (or standalone when there is none), the branch streams are folded
;; left-to-right with `PlusOp`, and the union is fed into a `DistinctOp` to
;; enforce set-union semantics.
(defmethod assemble-node :union
  [^Circuit circuit {:keys [branches out-vars] :as node} acc]
  (check-incoming! node acc)
  (let [branch-acc (some-> acc (assoc :leaves []))
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
     :leaves (into (vec (:leaves acc)) (mapcat :leaves wired))}))

;; A `:difference` extends the running relation A as
;; A - semijoin(A, distinct(keys(B))), with the negative relation B
;; assembled against A's key projection. The seed is fed raw — A's key
;; multiplicities may flow through B's operators, but the distinct on
;; `keys(B)` normalizes the semijoin selector to weight 1 per key, which is
;; all the subtraction needs to cancel exactly.
(defmethod assemble-node :difference
  [^Circuit circuit {:keys [negative key-vars keyed-vars out-vars] :as node} acc]
  (check-incoming! node acc)
  (let [{:keys [stream vars leaves]} acc
        positive-keyed (project-stream circuit stream vars keyed-vars)
        ;; keyed-vars = key-vars ++ remaining vars. We strip to key-vars
        negative-seed {:stream (project-stream circuit positive-keyed keyed-vars key-vars)
                       :vars key-vars
                       :leaves []}
        negative-wired (assemble-node circuit negative negative-seed)
        ;; This is a pure planner boundary check. Normally negative-wired returns
        ;; tuples in key-vars order. This is currently a no-op.
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
     :leaves (into leaves (:leaves negative-wired))}))

;; A `:permute` is a unary reordering of the running stream to the node's
;; target layout; it never carries `:incoming`.
(defmethod assemble-node :permute
  [^Circuit circuit {:keys [out-vars indices] :as _node} acc]
  {:stream (.addUnary circuit
                      (MapOp/project (int-array indices))
                      (:stream acc))
   :vars out-vars
   :leaves (:leaves acc)})

(defn- arg-reader [arg layout var->idx]
  (case (:kind arg)
    :constant (constantly (:value arg))
    :variable (let [idx (var->idx (:var arg))]
                (when (nil? idx)
                  (throw (ex-info "variable not present in filter input layout"
                                  {:var (:var arg) :layout layout})))
                (let [idx (int idx)]
                  (fn [^Tuple tuple] (.get tuple idx))))))

;; `test` runs once per tuple per delta, so the common arities call [f]
;; directly instead of paying `apply` + a lazy arg seq on every tuple.
(defn- predicate-filter [{:keys [predicate args] :as _descriptor} layout]
  (let [f (util/resolve-fn predicate)
        var->idx (zipmap layout (range))
        arg-readers (mapv #(arg-reader % layout var->idx) args)
        [r0 r1 r2] arg-readers]
    (case (count arg-readers)
      0 (reify Predicate
          (test [_ _tuple] (boolean (f))))
      1 (reify Predicate
          (test [_ tuple] (boolean (f (r0 tuple)))))
      2 (reify Predicate
          (test [_ tuple] (boolean (f (r0 tuple) (r1 tuple)))))
      3 (reify Predicate
          (test [_ tuple] (boolean (f (r0 tuple) (r1 tuple) (r2 tuple)))))
      (reify Predicate
        (test [_ tuple]
          (boolean (apply f (mapv #(% tuple) arg-readers))))))))

;; A `:filter` node lowers its predicate to one stateless FilterOp over the running stream
(defmethod assemble-node :filter
  [^Circuit circuit {:keys [predicate out-vars] :as node} {:keys [stream vars] :as acc}]
  (check-incoming! node acc)
  (let [stream (.addUnary circuit (FilterOp/fromPredicate "filter-predicate" (predicate-filter predicate vars)) stream)]
    (assoc acc :stream stream :vars out-vars)))

;; Like `predicate-filter`, the function lowerings run once per tuple per
;; delta, so the two supported arities call [f] directly.
(defn- function-map-transform
  "A Function<Tuple,Tuple> appending the fn clause's computed value as a new
  trailing column. nil/false results are appended as ordinary values."
  [{f-sym :fn :keys [args] :as _descriptor} layout]
  (let [f (util/resolve-fn f-sym)
        var->idx (zipmap layout (range))
        [r0 r1] (mapv #(arg-reader % layout var->idx) args)
        append (fn ^Tuple [^Tuple tuple v] (.concat tuple (Tuple/of (object-array [v]))))]
    (case (count args)
      1 (reify Function
          (apply [_ tuple] (append tuple (f (r0 tuple)))))
      2 (reify Function
          (apply [_ tuple] (append tuple (f (r0 tuple) (r1 tuple))))))))

(defn- function-filter-predicate
  "A Predicate<Tuple> keeping rows whose bound result column equals the fn
  clause's computed value (`=`, so nil/false results compare as values)."
  [{f-sym :fn :keys [args ret-var] :as _descriptor} layout]
  (let [f (util/resolve-fn f-sym)
        var->idx (zipmap layout (range))
        [r0 r1] (mapv #(arg-reader % layout var->idx) args)
        ret-reader (arg-reader {:kind :variable :var ret-var} layout var->idx)]
    (case (count args)
      1 (reify Predicate
          (test [_ tuple] (= (f (r0 tuple)) (ret-reader tuple))))
      2 (reify Predicate
          (test [_ tuple] (= (f (r0 tuple) (r1 tuple)) (ret-reader tuple)))))))

;; A `:function` either appends a newly computed result or keeps rows whose
;; existing result binding equals the computed value. The planned `:incoming`
;; layout determines which circuit operator is required.
(defmethod assemble-node :function
  [^Circuit circuit {:keys [function incoming out-vars] :as node} {:keys [stream vars] :as acc}]
  (check-incoming! node acc)
  (let [{:keys [ret-var]} function
        stream (if (some #{ret-var} incoming)
                 (.addUnary circuit
                            (FilterOp/fromPredicate "filter-function"
                                                    (function-filter-predicate function vars))
                            stream)
                 (.addUnary circuit
                            (MapOp/fromFunction "map-function"
                                                (function-map-transform function vars))
                            stream))]
    (assoc acc :stream stream :vars out-vars)))

(defn plan->circuit
  "Assembles a Kotlin [org.hooray.dbsp.Circuit] from a [plan]. Returns

    {:circuit <Circuit>
     :leaves  [{:order … :handle <InputHandle>} ...]   ; flat, one per leaf triple
     :output  <OutputHandle>}

  The circuit is per-leaf `Source -> Filter? -> Map`, recursive node assembly,
  and a final `Map` projecting to `:find`."
  [{:keys [where-plan final-permute]}]
  (let [circuit (Circuit.)
        wired (assemble-node circuit where-plan nil)
        ;; wire up the find clause
        projected (.addUnary circuit (MapOp/project (int-array final-permute)) (:stream wired))]
    {:circuit circuit
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
  "Pushes each leaf triple's delta (in its planned order) onto its input
  handle. `:leaves` holds one `{:order … :handle …}` entry per leaf."
  [{:keys [leaves]} index-deltas]
  (doseq [{:keys [handle order]} leaves]
    (.push ^org.hooray.dbsp.InputHandle handle
           (index-delta-zset index-deltas order))))

(defn- zset->result-set
  "Renders an output `TupleZSet` as a seq of `[tuple-vector weight]` pairs."
  [^ZSet zset]
  (mapv (fn [entry]
          [(vec (.toList ^Tuple (.getKey entry)))
           (.getValue ^IntegerWeight (.getValue entry))])
        (.entries zset)))

(defrecord DbspQuery [id query plan circuit leaves output queue])

(defn dbsp-query?
  "True if [x] is a DBSP-standard incremental query (vs. a WCOJ one)."
  [x] (instance? DbspQuery x))

(defn compile-query
  "Compiles [query] into a stepping DBSP circuit, primed with the current state
  of [db]. Returns a [DbspQuery] carrying the circuit and a result queue."
  ^DbspQuery [db query]
  {:pre [(s/valid? ::query/query query) (query/validate-query (s/conform ::query/query query))]}
  (let [p (plan query)
        {:keys [circuit leaves output]} (plan->circuit p)
        iq (->DbspQuery (random-uuid) query p circuit leaves output
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
