# Spec: DBSP-Or — `or` branches in the standard DBSP circuit

Status: **Historical** — documents the original PR #12 implementation.

Note: the current `:standard` DBSP planner now uses an explicit relation tree
with `:pattern`, `:join`, and `:union` nodes. Sections below that describe `or`
as a pseudo-pattern inside top-level `:patterns` / `:joins` are retained as
history for the original OR change, not as the current planner shape.
Builds on: `specs/dbsp-standard.md` (PR #6, merged as 71b8964).

## Objective

Extend the DBSP-standard incremental engine (`hooray.dbsp` + `org.hooray.dbsp.*`)
with support for **`or` branches** in `:where` clauses. After this change a
query such as

```clojure
{:find  [name]
 :where [[?e :name name]
         (or [?e :sex :male]
             [?e :sex :female])]}
```

can be registered with `*dbsp-version*` set to `:standard` and incrementally
maintained. Reference behaviour for tests comes from the static query engine
(`hooray.query/query`) and from hand-computed deltas; the `:wcoj`
incremental engine does **not** yet support `or` and is therefore not a
valid comparison baseline for this work.

**Why now.** PR #6 landed the standard DBSP engine for conjunctive triple-only
queries. `or` is the next item on the README roadmap for the incremental side
and the smallest non-trivial relational extension — it does not require new
operator primitives (the existing `PlusOp` + `DistinctOp` suffice), only new
planning and assembly machinery.

**Scope.**

- **In:** `(or B₁ B₂ …)` clauses where every leaf branch is a single triple
  pattern. Branches may themselves be `or` clauses — nesting is handled
  recursively (see design decision #7). Branches may share constants and
  variables freely; the set of free variables must be identical across
  branches at every level (already enforced recursively by
  `hooray.query/validate-patterns`). `or`-only queries and `or` blocks mixed
  with regular triple patterns in the same `:where`.
- **Out (rejected at plan time with `unsupported-ex`):** branches whose
  clause-type is `:and`, `:not`, `:predicate`, or `:fn`. Top-level
  `and`/`not`/predicate/`fn` clauses outside `or` remain unsupported too —
  matching the current `hooray.dbsp/compile-pattern` behaviour. The
  query-parser grammar (`hooray.query`) is **not changed**: branch shapes that
  the spec already accepts continue to conform, the standard engine simply
  refuses to plan anything it cannot yet compile, exactly as it does today for
  predicates/functions.

## Design decisions

1. **Semantics: set-union via `DistinctOp`.** An `or` block evaluates to
   `distinct(plus(branch₁, …, branchₖ))`. Overlap between branches collapses to
   weight 1, matching standard Datalog `or` semantics and the existing static
   (`hooray.query`) engine. Plain `PlusOp` (bag-union) would double-count tuples
   that satisfy multiple branches and is explicitly rejected.
2. **No new operator primitives.** `PlusOp` and `DistinctOp` already exist in
   `org.hooray.dbsp` (see PR #6, used by the existing operator-test oracles).
   This change only wires them.
3. **`or` block as a pseudo-pattern.** From the planner's point of view an `or`
   block looks like a triple pattern: it has an `:index`, a set of free
   `:vars`, and produces a relation over those variables. It participates in
   `left-deep-order` unchanged. Each top-level entry of `:where` becomes one
   pattern-plan entry (either `:kind :triple` or `:kind :or`).
4. **Canonical branch layout.** The planner picks the `or` block's output
   variable order (its `target`) the same way it does for a triple — by the
   outer left-deep join's needs. Each branch's `pattern-plan` is then computed
   with that same target, so all branches emit tuples in the same column order
   before union.
5. **Determinism preserved.** Branch order follows query order. When the outer
   chain leaves the `or` block's `:vars` order ambiguous (e.g. a 2-var `or` with
   no outer dependencies on column order), the existing
   `lead-with`/`indices-of` machinery picks deterministically — no new tie-break
   rules are needed.
6. **Branch validation at compile time, not parse time.** The `hooray.query`
   spec keeps accepting any `::pattern`/`::and-pattern` inside an `::or-pattern`
   (`hooray.query` is shared with the WCOJ engine, which supports the broader
   grammar). The DBSP planner rejects unsupported branch contents with a clear
   exception — the same pattern PR #6 used for predicates.
7. **Nested `or` handled recursively, not flattened.** An `or` branch can
   itself be an `:or` descriptor. `compile-pattern`, `pattern-plan`, and
   `assemble-pattern` all dispatch on `:kind`, so nesting falls out naturally:
   a nested `(or A (or B C))` compiles to `Distinct(Plus(A, Distinct(Plus(B,
   C))))`. The outer `Distinct` is correct (idempotent on set inputs) and the
   inner `Distinct` is a no-op in delta terms once `B`/`C` outputs are sets,
   so the extra operator costs accumulator state but not correctness. Keeping
   the recursive structure avoids a separate normalization pass and keeps the
   descriptor/plan/circuit tree a faithful image of the source query.
8. **Branch fold order.** `PlusOp` is associative and commutative, but to keep
   the circuit deterministic and reviewable, branches are folded **left to
   right in plan order** (which is query order). No selectivity-based
   reordering in v1.
9. **`Distinct` is unconditional.** Every `:or` block, including a
   single-branch `(or B)` and every nested `:or`, emits a `DistinctOp` on its
   union output. Uniformity over micro-optimization: a single-branch `or` is
   semantically just `B` and not worth special-casing in code; the redundant
   inner `Distinct` in a nested `or` costs only accumulator state. Both can
   be revisited later if a profile demands it.
10. **No effect on `:wcoj`.** `hooray.incremental` is not touched. The dispatch
    in `hooray.core/transact` already routes per registered query.

## Architecture

### Pattern descriptors (`hooray.dbsp/compile-pattern`)

Today the descriptor for a clause is

```clojure
{:index N :attr … :entity … :value … :vars [...]}
```

After this change every descriptor carries a `:kind` tag (`:triple` or `:or`):

```clojure
;; triple
{:index N :kind :triple
 :attr {…} :entity {…} :value {…}
 :vars [<encounter-order vars>]}

;; or block
{:index N :kind :or
 :branches [<descriptor> …]           ; each branch is :kind :triple or :kind :or
 :vars     [<canonical free-vars>]}   ; encounter order in the first branch
```

`compile-pattern` is extended:

- For `[:or branches]`, recursively compile every branch using
  `compile-pattern`. The resulting branch descriptor's `:kind` is either
  `:triple` or `:or` (the latter for nested `or` clauses).
- Any compiled branch whose `:kind` is neither `:triple` nor `:or` triggers
  `err/unsupported-ex` — covers `:and`, `:not`, `:predicate`, `:fn`, and any
  future non-triple clause.
- Compute `:vars` from the first branch's `:vars` (encounter order). The
  validation that all branches share the same *set* of free variables already
  runs recursively in `hooray.query/validate-patterns`, so the planner can
  trust the invariant rather than re-checking it.
- The branch's `:index` is set to its position in the `or` clause (0, 1, …);
  used only for diagnostics.

### Join order (`hooray.dbsp/left-deep-order`)

**Unchanged.** It only reads `:vars` and `:index` from descriptors.

### Plan (`hooray.dbsp/pattern-plan`, `hooray.dbsp/plan`)

Pattern-plan returns a tagged plan node:

```clojure
;; triple plan (today's shape, plus :kind)
{:kind :triple
 :descriptor … :order :aev/:ave :filter … :project … :out-vars target}

;; or plan
{:kind :or
 :descriptor   …
 :branch-plans [<plan with target=out-vars> …]   ; each branch plan is :kind :triple or :kind :or
 :out-vars     target}
```

For an `or` descriptor with target `T`, `pattern-plan` recursively calls
`pattern-plan` on each branch descriptor with the same target `T`. Because all
branches share the same free variables (transitively, by recursive validation),
each branch — whether a triple or a nested `or` — can produce its variables in
target order.

`plan`'s overall structure is unchanged. The vector returned in `:patterns`
contains tagged plan nodes; `:joins`/`:final-permute` stay the same.

### Circuit assembly (`hooray.dbsp/assemble-pattern`, `plan->circuit`)

`assemble-pattern` dispatches on `:kind`:

- **`:triple`** — same as today: `Source → Filter? → Map(project)`. Returns
  `{:stream s :handles [h]}` (handles now a vector for uniformity).
- **`:or`** — for each branch:
  1. Recursively call `assemble-pattern` on the branch plan. The branch is
     either a triple (yielding one handle) or a nested `or` (yielding many
     handles).
  2. Collect branch streams `[s₁ … sₖ]` and concatenate their handle lists.

  Then:
  3. If `k = 1`: feed `s₁` straight into `DistinctOp` (no Plus needed).
  4. If `k ≥ 2`: fold left with `PlusOp` (`addBinary`), feed the sum into
     `DistinctOp`.

  Return `{:stream <distinct-out> :handles [handles from all branches, in order]}`.

`plan->circuit` collects `:handles` from every pattern in order — recursive
descent through nested `or`s concatenates them — and returns a flat
`:inputs` vector parallel to a new flat `:leaves` structure that records each
leaf's `:order` (`:aev`/`:ave`). This is needed for `push-deltas!`.

### Delta dispatch (`hooray.dbsp/push-deltas!`, `compute-delta!`)

Today `push-deltas!` iterates `(:patterns plan)` and reads `:order` per
pattern. After this change it iterates a flat `:leaves` list of
`{:handle … :order …}` records — one record per leaf triple (triples have one
leaf, `or` blocks have *k* leaves, one per branch, all in plan order). The
delta z-set per leaf is built the same way it is today
(`index-delta-zset deltas (:order leaf)`).

`compute-delta!` is otherwise unchanged.

### No changes to `hooray.core` or `org.hooray.dbsp.*`

- `hooray.core` dispatch is already engine-agnostic.
- `Circuit`, `PlusOp`, `DistinctOp`, `MapOp`, `FilterOp`, `IncrementalJoinOp`,
  `Tuple`, `TupleZSet` already do everything we need.

## Worked example

Query:

```clojure
{:find  [name]
 :where [[?e :name name]
         (or [?e :sex :male]
             [?e :sex :female])]}
```

Descriptors after `compile-patterns`:

```
0: {:index 0 :kind :triple :attr :name :entity ?e :value name :vars [?e name]}
1: {:index 1 :kind :or
    :vars [?e]
    :branches
      [{:kind :triple :attr :sex :entity ?e :value :male   :vars [?e]}
       {:kind :triple :attr :sex :entity ?e :value :female :vars [?e]}]}
```

`left-deep-order` keeps `[0 1]` (shares `?e`). Plan (abridged):

```
:patterns [{:kind :triple :order :aev :filter {0 :name} :project [1 2]
            :out-vars [?e name]}
           {:kind :or
            :out-vars [?e]
            :branch-plans
              [{:kind :triple :order :aev :filter {0 :sex, 2 :male}   :project [1] :out-vars [?e]}
               {:kind :triple :order :aev :filter {0 :sex, 2 :female} :project [1] :out-vars [?e]}]}]
:joins [{:key-arity 1 :key-vars [?e] :left-permute nil :out-vars [?e name]}]
:result-vars   [?e name]
:final-permute [1]
```

Circuit (operator names in build order):

```
input  filter-constants  permute                    ; triple 0
input  filter-constants  permute                    ; or branch 0
input  filter-constants  permute                    ; or branch 1
plus                                                ; union of branches
distinct                                            ; set-semantics
incremental-join                                    ; triple 0 ⋈ or-block
permute                                             ; final :find projection
```

Three input handles; deltas are pushed per branch in plan order.

## Commands

```
Build:                  ./gradlew build
All tests:              ./gradlew test
DBSP standard tests:    ./gradlew test --tests "*dbsp*" --tests "*Dbsp*"
Clojure REPL:           ./gradlew clojureRepl
```

## Project Structure

```
src/main/clojure/hooray/dbsp.clj             ← extended: compile-pattern, pattern-plan,
                                                assemble-pattern, plan->circuit, push-deltas!
src/test/clojure/hooray/dbsp_test.clj        ← extended: parse/plan/assemble/e2e tests for :or
specs/dbsp-or.md                              ← this spec
```

No Kotlin changes. `hooray.query`, `hooray.incremental`, `hooray.core`, and
every file under `org.hooray.*` are left untouched.

## Code Style

Match `hooray.dbsp` as it stands after PR #6 — small focused functions,
descriptors as plain maps, multi-arity dispatch by `:kind`, exceptions via
`hooray.error`. Example shape of the new dispatch:

```clojure
(defn- assemble-pattern
  [^Circuit circuit plan-node]
  (case (:kind plan-node)
    :triple (assemble-triple circuit plan-node)
    :or     (assemble-or     circuit plan-node)))

(defn- assemble-or
  [^Circuit circuit {:keys [branch-plans]}]
  (let [wired   (mapv #(assemble-triple circuit %) branch-plans)
        summed  (reduce (fn [acc {:keys [stream]}]
                          (.addBinary circuit (PlusOp.) acc stream))
                        (:stream (first wired))
                        (rest wired))
        distinct-out (.addUnary circuit (DistinctOp.) summed)]
    {:stream  distinct-out
     :handles (mapv :handle wired)}))
```

## Testing Strategy

JUnit-via-Gradle. New tests live in `src/test/clojure/hooray/dbsp_test.clj`
beside the existing ones, in four groups:

1. **Pattern descriptors** (`compile-patterns`)
   - `or` clause yields a `:kind :or` descriptor with the expected `:vars` and
     per-branch descriptors.
   - Nested `or` yields a nested `:kind :or` branch descriptor (preserved, not
     flattened); deeply nested `or` (three or more levels) compiles to a tree
     of matching depth.
   - Branch with `:and`, `:not`, `:predicate`, or `:fn` throws
     `unsupported-ex`.

2. **Plan** (`plan`)
   - Single `or`-only query (one or-block, no outer triples).
   - `or` joined with one outer triple — target/order/filter/project on each
     branch match the outer key.
   - 2-var `or` (e.g. `(or [?a :r ?b] [?a :s ?b])`) joined into a chain that
     forces `:ave` on the branches.
   - Plan determinism: planning the same query twice returns equal data.

3. **Circuit assembly** (`plan->circuit`)
   - Operator-name sequence matches the worked example (per-branch
     `input/filter/permute`, one `plus` per extra branch, one `distinct`, then
     the outer join and final projection).
   - Single-branch `or` produces `input/filter/permute/distinct` with no
     `plus`.
   - `:inputs` length equals the total leaf count.

4. **End-to-end** (`compute-delta!` + `q-inc` via `*dbsp-version* :standard`)
   - Single-branch `or` produces the same deltas as the equivalent bare triple.
   - Two-branch `or` over disjoint conditions (e.g. male/female) returns the
     union.
   - Two-branch `or` where both branches yield the same tuple — DistinctOp
     keeps weight 1; retracting one of the two matching facts emits no delta;
     retracting both emits `-1`.
   - `or` + outer triple join, both add and retract paths.
   - 2-var `or` joined into a chain.
   - `or`-only query.
   - Nested `or` end-to-end: e.g.
     `(or [?e :sex :male] (or [?e :name "Ada"] [?e :name "Bob"]))` returns the
     same delta multiset as the flat three-branch equivalent.
   - Rejection: each unsupported branch shape throws when registered.

   Note: the existing `cross-engine=` harness compares `:wcoj` vs `:standard`
   per-transaction deltas. It is **not extended** for `or` — `:wcoj` does not
   yet support `or` and cannot serve as an oracle. Where deltas need an
   external reference, hand-compute the expected delta multiset in the test
   or compare against the static (`hooray.query/query`) engine's
   set-of-results after replaying the transaction sequence.

No coverage threshold; every new plan branch and every new operator wiring path
has ≥1 test.

## Boundaries

- **Always:** preserve existing `:standard` behaviour (every dbsp_test.clj test
  that exists today must still pass); reject unsupported branch shapes with a
  clear `unsupported-ex` carrying the offending clause; use `PlusOp` +
  `DistinctOp` rather than introducing new operators; keep planning
  deterministic.
- **Ask first:** any change to `hooray.query` (parser, validation, or the
  `::or-pattern` spec); adding a new Kotlin operator in `org.hooray.dbsp`;
  changing the shape returned by `compile-query` (touches `hooray.core`
  consumers).
- **Never:** weaken or remove existing incremental tests; change `:wcoj`
  behaviour; implement `and`/`not`/predicates/functions as part of this PR;
  introduce bag-union `or` (i.e. skip `DistinctOp`); silently re-shape the
  `:patterns` vector in a way that breaks the existing
  `parse`/`plan`/`plan->circuit` boundaries.

## Success Criteria

1. With `*dbsp-version*` at its default `:wcoj`, every existing test passes
   unchanged; `hooray.incremental` and `hooray.query` are byte-identical.
2. Under `:standard`, `(or B₁ … Bₖ)` with triple-only branches compiles,
   plans, and runs.
3. A two-branch `or` where both branches emit the same tuple at the same step
   produces that tuple with weight 1 (set semantics); retracting one of the two
   underlying facts emits no delta.
4. A single-branch `(or B)` produces the same delta stream as the bare triple
   `B`.
5. An `or` block joined with one or more outer triples returns the expected
   delta multiset for the same query and the same transaction sequence
   (oracle: the static `hooray.query/query` engine or a hand-computed
   expected set).
6. A 2-variable `or` (e.g. `(or [?p :name n] [?p :age n])`) works regardless
   of the outer chain's variable layout.
7. Every unsupported branch shape (`and`, `not`, predicate, function) throws
   `unsupported-ex` at plan time, identifying the offending clause. Nested
   `or` is *not* an error — it compiles to the same descriptor as the flat
   equivalent.
8. Planning the same `or` query twice returns `=`-equal plan data.

## Open Questions

None — review-round questions resolved:

1. **Fold order:** left-to-right in plan order (design decision #8).
2. **`Distinct` for single-branch `or`:** always emitted (design decision #9);
   a single-branch `or` is degenerate (equivalent to the bare branch) and not
   worth special-casing.
3. **Redundant `Distinct` in nested `or`:** accepted in v1 for uniformity
   (design decision #9); revisitable later via assembly-time elision.
