# Spec: DBSP-Standard — incremental Datalog as a circuit of unary/binary operators

Status: **Phase 1 (Specify)** — revised after review round 1; awaiting approval before Plan.
Branch: `worktree-dbsp-standard`.

## Objective

Build a *standard* DBSP implementation for hooray2: evaluate incremental Datalog
queries as a static **circuit** of **unary and binary operators**, constructed
once via a **stream API**, modelled directly on the Feldera `dbsp` crate.

This is a deliberate alternative to the existing `hooray.incremental` /
`org.hooray.incremental.IncrementalGenericJoin` path (PR #4), which combines all
triple patterns in a single n-ary incremental WCOJ delta computation. The new
path instead decomposes a query into a tree of **binary joins** — exactly the
DBSP model — at the cost of not being worst-case optimal for cyclic queries.

**Who/why:** This is an exploratory research codebase. The goal is a faithful,
minimal DBSP-style engine to compare against the WCOJ approach and to serve as
the base for later operators (`or`, `and`, `not`, predicates, functions).

**Scope (v1):** Standard triple patterns only, conjunctive queries, bag
semantics — the same surface the current incremental path supports.

**Out of scope (v1):** `or` / `and` / `not` / `or-join` / `not-join`,
predicates, functions, aggregates, pull, rules/recursion, nested circuits and
feedback loops, cost-based planning, disk-backed traces.

## Design decisions (confirmed)

1. **Placement** — Circuit/Stream/operator framework in a new Kotlin package
   `org.hooray.dbsp`. Query analysis and circuit assembly stay in Clojure
   (`hooray.dbsp`), driving the Kotlin framework via interop.
2. **Input model** — Each triple pattern is its **own input** (own source
   stream), even when the same attribute appears in several patterns (self-joins
   are independent inputs). A transaction is supplied as shared **flat index
   `ZSet`s**: unlike the current `ZSetIndices` (nested `IndexedZSet`s), each
   "order" is a flat `ZSet` over full index tuples. v1 maintains two orders —
   entity-first (`AEV`, `[a e v]`) and value-first (`AVE`, `[a v e]`). Each
   pattern receives the order chosen by the planner, filters its own attribute
   and constants, then projects variable columns. This keeps base patterns from
   needing a `map_index` / re-index operator.
3. **Semantics** — Bag semantics, **no implicit `distinct`**. `distinct` is a
   defined operator but is *not* auto-applied; matches current incremental tests.
4. **Join planning** — Deterministic **left-deep binary join tree**, ordered by
   variable connectivity. No statistics.
5. **Engine dispatch** — A dynamic var `*dbsp-version*` ∈ `{:wcoj, :standard}`
   (default `:wcoj`) selects the engine. `q-inc` records the *current* value of
   `*dbsp-version*` on the registered `!inc-qs` entry; `transact` dispatches each
   registered query to its engine. The two engines coexist; the existing
   `hooray.incremental` path is unchanged.
6. **Tuples** are positional **indexed arrays** (`Array<Any?>`); join keys are
   leading-position slices.
7. **`IncrementalJoin`** is a single fused stateful bilinear operator (matching
   Feldera's trace-based `join`). The primitives (`Integrate`, `Differentiate`,
   `Z1`, `Plus`/`Minus`, `StreamJoin`, `Distinct`) are *also* implemented; a
   join composed from them is the differential-test oracle for the fused one.

## Tech Stack

- **Kotlin 2.1.20** (JVM 17) — circuit framework + operators (new package).
- **Clojure 1.12.3** — query parsing, join-plan analysis, circuit assembly, dispatch.
- Build: Gradle + clojurephant 0.8.0-beta.7; tests on JUnit 5 (Jupiter).
- **No new dependencies.** Reuse the existing `org.hooray.incremental` z-set
  algebra (`ZSet`, `IZSet`, `Weight`, `IntegerWeight`).

## Architecture

### Circuit & Stream (Kotlin, `org.hooray.dbsp`)

A `Circuit` is an immutable DAG of operator nodes, built once, then `step()`ed.
Each `step` consumes one delta per input and produces one delta per output.

- `Stream<D>` — handle to one operator's output value for the current step.
- `Operator` (base): `name`. `SourceOperator<O>: eval(): O`,
  `UnaryOperator<I,O>: eval(I): O`, `BinaryOperator<I1,I2,O>: eval(I1,I2): O`.
- `Circuit.addInput()` returns a `(Stream<D>, InputHandle<D>)` pair (mirrors
  Feldera `add_input_zset`); `addUnary / addBinary` return `Stream`s.
- `InputHandle<D>.push(zset)` sets a source's value for the next step;
  `Stream.output()` yields an `OutputHandle<D>` read after `step`.
- `Circuit.step` evaluates nodes in topological order.
- **No feedback loops in v1.** `Integrate` is a stateful accumulator primitive,
  not a `plus`+`z1` cycle — so the circuit is a pure DAG and the scheduler is a
  plain topological pass; no strict-operator handling needed.

### Operators

The **v1 standard circuit wires only**: `Source`, `Filter`, `Map`,
`IncrementalJoin`.

| Operator | Kind | Semantics |
|---|---|---|
| `Source` | source | Emits the per-step index delta `ZSet`, fed by an `InputHandle` in the pattern's chosen order |
| `Filter` | unary, linear | Keep tuples matching a predicate (constant `e`/`v`) — incremental form is itself on the delta |
| `Map` | unary, linear | Project columns, and **re-permute intermediate results** so the next join key is leading — incremental form is itself on the delta |
| `IncrementalJoin` | binary, bilinear | Fused stateful incremental equi-join (below) |

Also **implemented and unit-tested** but not wired by the v1 bag-semantics
circuit — present for completeness, the test oracle, and future operators:
`Integrate` (I), `Differentiate` (D), `Z1` (unit delay), `Plus`/`Minus`,
`StreamJoin` (non-incremental bilinear join), `Distinct` (positive weights → 1).

### IncrementalJoin (fused bilinear operator)

Holds two running integrals of its inputs, keyed by join key. On `eval(Δa, Δb)`:

```
ΔO = Δa ⋈ Iₙₑw(b)  +  Iₒₗd(a) ⋈ Δb
```

where `Iₙₑw(b)` is `b`'s integral *including* the current `Δb`, and `Iₒₗd(a)` is
`a`'s integral *before* this step. (This folds in the `Δa ⋈ Δb` cross term.)
Matching-key tuples are concatenated, duplicate join columns dropped, **weights
multiplied** (bag-semantics multiset join, via existing `ZSet` algebra). An
`IncrementalJoin` requires both its inputs keyed on the join key (leading
columns). Base patterns get this from their planner-chosen `AEV`/`AVE`
permutation; **intermediate results do not** — a join's output tuple carries all
bound variables in some order, so each `IncrementalJoin` after the first is
preceded by a permuting `Map` on its intermediate (left) input that moves the
next join key into leading position. Without it the join cannot group
efficiently (e.g. joining on a variable sitting in tuple position 3).

### Triple-pattern inputs (flat permuted ZSets)

Per transaction, the dispatcher (`hooray.dbsp/compute-delta!`) builds, per
registered standard query, shared index delta facts as flat `ZSet`s of indexed
arrays in two permutations:

- `AEV` order — entity-leading, tuples `[a e v]`.
- `AVE` order — value-leading, tuples `[a v e]`.

The attribute stays in the source tuple. Each pattern has one source stream with
one `InputHandle`; the dispatcher `push`es the index order the planner assigned
that pattern. The pattern's `Filter` matches the constant attribute and any
constant `e`/`v`, then its `Map` projects the remaining variable columns so the
join key is leading. Two orders suffice in v1: a pattern has at most two
variables, so its join key is `e`, `v`, or `{e,v}`.

### Analysis phase (Clojure, `hooray.dbsp`)

1. Parse the query via the existing `::query` spec (`hooray.query`).
2. Compile each `where` triple pattern → a descriptor: constant attribute,
   constant/variable `e` and `v`, variable names.
3. Build a join graph (patterns = nodes, shared variables = edges); produce a
   deterministic **left-deep order** — start from the first pattern, repeatedly
   append the connected pattern sharing the most variables (ties → query order).
4. For each join step compute the join key = variables shared between the
   accumulated tuple and the next pattern (empty key ⇒ Cartesian product).
5. Assign each base pattern's input permutation (`AEV`/`AVE`) from its join key,
   and the column order each intermediate permuting `Map` must produce.
6. Emit the circuit: per-pattern `Source → Filter → Map`; a left-deep chain of
   `IncrementalJoin`s, each non-first join preceded by a permuting `Map` on its
   intermediate input; a final `Map` for the `:find` projection; one output.

Cyclic queries (e.g. triangle) work — each join keys on *all* shared variables —
they are simply not worst-case optimal (that is the WCOJ path's job).

### Engine dispatch (`*dbsp-version*`)

- `^:dynamic *dbsp-version*` defined in `hooray.core`, default `:wcoj`.
- `hooray.core/q-inc` reads `*dbsp-version*` at registration and stores it on
  the `!inc-qs` entry (extra `:version` field on the registered value/record).
- `hooray.core/transact` dispatches per registered query:
  `:wcoj → incremental/compute-delta!`, `:standard → dbsp/compute-delta!`.
- `dbsp/compute-delta!` builds shared `AEV`/`AVE` index delta `ZSet`s,
  `push`es each pattern's planner-chosen index order through its `InputHandle`,
  calls `circuit.step`, and reads the result from the `OutputHandle`.
- The public API is otherwise unchanged: `q-inc`, `consume-delta!`,
  `unregister-inc-q`. Selecting the new engine:
  `(binding [h/*dbsp-version* :standard] (h/q-inc node query))`.
- `:wcoj` behaviour must be **byte-for-byte unchanged**.

## Commands

```
Build:            ./gradlew build
All tests:        ./gradlew test
DBSP tests only:  ./gradlew test --tests "*Dbsp*" --tests "*dbsp*"
Clojure REPL:     ./gradlew clojureRepl
```

## Project Structure

```
src/main/kotlin/org/hooray/dbsp/
  Circuit.kt              → Circuit builder + topological scheduler, Stream
  Operator.kt             → Operator / Source / Unary / Binary interfaces
  Stream.kt               → Stream / input / output handles
  Tuple.kt                → positional tuple value type
  Types.kt                → shared z-set aliases
  FilterOp.kt             → linear filter
  MapOp.kt                → linear projection / column reorder
  IncrementalJoinOp.kt    → fused bilinear incremental join
  StreamJoinOp.kt         → non-incremental bilinear join (oracle / future)
  IntegrateOp.kt          → I (stateful accumulator)
  DifferentiateOp.kt      → D
  Z1Op.kt                 → unit delay
  PlusOp.kt               → Plus / Minus
  DistinctOp.kt           → positive weights → 1
src/main/clojure/hooray/dbsp.clj   → analysis (join plan), circuit assembly, compute-delta!
src/main/clojure/hooray/core.clj   → +*dbsp-version*, q-inc records version, transact dispatch
src/test/kotlin/org/hooray/dbsp/   → circuit + operator unit tests, fused-vs-composed join oracle
src/test/clojure/hooray/dbsp_test.clj → end-to-end query tests under :standard
specs/dbsp-standard.md             → this spec (kept in sync as decisions change)
```

`org.hooray.incremental.*` and `hooray.incremental` are reused (z-set algebra)
but **not modified**.

## Code Style

Match the existing `org.hooray.incremental` package — small interfaces, explicit
generics, `ZSet`/`IntegerWeight` typing.

```kotlin
/** A unary circuit operator: one input stream, one output stream. */
interface UnaryOperator<I, O> : Operator {
    fun eval(input: I): O
}

/** Keeps only ZSet entries whose key satisfies [predicate]. Linear, so its
 *  incremental form is itself applied to the delta. */
class FilterOp<K>(
    private val predicate: (K) -> Boolean,
) : UnaryOperator<ZSet<K, IntegerWeight>, ZSet<K, IntegerWeight>> {
    override val name = "filter"
    override fun eval(input: ZSet<K, IntegerWeight>): ZSet<K, IntegerWeight> =
        input.filterKeys(predicate)
}
```

Clojure: namespaced keywords for conformed query parts, kebab-case, follow
`hooray.incremental` for naming.

## Testing Strategy

JUnit 5 via Gradle. Four levels:

- **Kotlin unit (`org.hooray.dbsp`)** — `Circuit` builds an immutable DAG and
  `step`s in topological order; `Filter`/`Map`/`Source` correctness;
  `Integrate`/`Differentiate`/`Z1`/`Plus`/`Distinct` correctness.
- **Kotlin — join oracle** — `IncrementalJoin` (fused) is checked against a join
  composed from `Integrate`/`Z1`/`StreamJoin`/`Plus` on randomized delta
  sequences; explicitly assert the `Δa ⋈ Δb` cross term is included.
- **Clojure analysis** — the join plan for a given `where` is deterministic and
  left-deep; per-step join keys and per-pattern input permutations are correct;
  single-pattern and Cartesian (no shared var) cases.
- **Clojure end-to-end (`hooray/dbsp_test.clj`)** — run under
  `(binding [*dbsp-version* :standard] …)`; port the bag-semantics cases from
  `query_inc_test.clj`. Include a cross-engine test: the same query set run
  under `:wcoj` and `:standard` must yield **identical delta multisets**.

No coverage threshold; every operator and every plan branch has ≥1 test.

## Boundaries

- **Always:** run `./gradlew test` before committing; put new code in
  `org.hooray.dbsp` / `hooray.dbsp`; reuse the existing z-set algebra; preserve
  bag semantics (no implicit `distinct`); keep the circuit an immutable DAG.
- **In scope, but must not change `:wcoj` behaviour:** the `*dbsp-version*` var,
  `q-inc` recording the version, and `transact` dispatch — these are required
  edits to `hooray.core`.
- **Ask first:** changing `build.gradle.kts` / `deps.edn`; adding any
  dependency; touching anything else under `org.hooray.incremental` or
  `hooray.incremental`.
- **Never:** delete or weaken existing incremental tests; change existing
  incremental/WCOJ test outputs; commit without tests; add a final implicit
  `distinct` that would turn bag results into set results.

## Success Criteria

1. With `*dbsp-version*` at its default `:wcoj`, **all existing incremental and
   WCOJ tests pass unchanged**, and no `*.incremental` file is modified.
2. `(binding [*dbsp-version* :standard] (q-inc …))` builds and steps a circuit;
   `transact` dispatches it to the standard engine.
3. The circuit is an immutable DAG; building twice from the same query yields an
   identical operator graph (deterministic plan).
4. Base triple patterns compile with **no re-index / `map_index` operator** —
   the planner-chosen input permutation supplies the join key directly.
5. For **every** bag-semantics case, `:standard` and `:wcoj` produce identical
   delta multisets.
6. These query shapes pass end-to-end under `:standard`: single pattern; update
   (retract+add); 2-pattern join; 3+-pattern chain; self-join; triangle
   (cyclic); Cartesian product.
7. The fused `IncrementalJoin` matches the composed-from-primitives reference
   join on randomized inputs, cross term included.

## Open Questions

All review-round questions are resolved:

1. **Permutation set** — `AEV` + `AVE` suffice for v1; the source tuple keeps the
   attribute column and a pattern has ≤2 variables.
2. **Intermediate ordering** — intermediate results *do* need re-permutation; a
   permuting `Map` precedes every non-first `IncrementalJoin` (a join cannot
   group efficiently on a variable buried in a non-leading tuple position).
3. **Delta delivery** — per-query shared `AEV`/`AVE` index deltas are computed in
   `compute-delta!` and pushed via per-pattern `InputHandle`s.

None blocking. Cross-query delta-bundle *sharing* remains a possible later
optimization, explicitly out of scope for v1.
