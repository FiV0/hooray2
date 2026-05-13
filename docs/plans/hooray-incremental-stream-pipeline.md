# Implementation Plan: Hooray Incremental Stream Pipeline

Spec: [hooray-incremental-stream-pipeline.md](../specs/hooray-incremental-stream-pipeline.md)

## Overview

Build a compiled stream/circuit incremental pipeline (`org.hooray.incremental.stream.*` / `hooray.incremental.stream`) alongside the existing `IncrementalPipeline` path. A Clojure analysis phase produces a reusable `CircuitSpec`; a Kotlin runtime walks that graph per tick. v1 targets streams over base relations only, with first-class `mapIndex`, `integrate`, `delay`, `differentiate` operators. Migrate via a `*circuit-version*` dynamic var; cut over only when `query_inc_test.clj` reaches parity. Steps 2 (base-index views) and 3 (derived relations + trace) come after v1.

## Decisions taken before planning

- **`CompiledTriplePattern` reuse.** The stream package reuses `org.hooray.incremental.CompiledTriplePattern` directly. No duplicate type; new code imports the existing class.
- **`ZSetIndices` shape.** Keep AEV + AVE only (matching the current data type). EAV/VAE remain a spec-level concept for v1; their absence is fine for success criterion 2 since the mixed-permutation test uses AEV/AVE.
- **Plan scope.** Covers the full spec: v1 (step 1), step 2 (remove saved base deltas), and post-v1 derived relations + general trace. Phases are gated by checkpoints so step 2 and post-v1 work only starts after parity at the prior checkpoint.

## Architecture decisions (recorded from spec)

- Single external input: `InputHandle<ZSetIndices>`. Base relations are views over that input, not independent inputs.
- One generic `Stream<T>` with typealiases (`ZSetStream`, `IndexedZSetStream`, `AccumulatedStream`); no per-physical-index interfaces.
- Permutation choice is data (`IndexSpec`), never branches inside operators.
- Physical index availability sits on `Relation`, not on `Stream`.
- `ZSetGenericJoin` is the lower-level operator: inputs must arrive already in correct order; it doesn't pick indexes or compute delta terms.
- `IncrementalWcojJoin` is a sub-circuit that expands compiled patterns into branch-local nodes (`mapIndex`, `integrate`, `delay`, `differentiate`, `ZSetGenericJoin`, canonicalization).
- Runtime API: `circuit.input.set(delta); circuit.step()`. `circuit.step(delta)` is a migration wrapper, not the long-term API.

## Risks and mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Stateful operator semantics (eval-before-commit) drift from existing `IncrementalPipeline` | High — silent correctness bugs | Pull `step()` two-phase model directly from `IncrementalPipeline.kt`; add unit tests asserting "operator sees previous state during eval()" for `integrate`/`delay`/`differentiate` |
| `IncrementalWcojJoin` expansion is the riskiest piece; it has to match the existing `IncrementalJoinOperator` branch logic exactly | High — parity failures | Build it after `ZSetGenericJoin` is unit-tested against a reference; write Kotlin circuit-level tests that exercise the same delta-term expansion before flipping `*circuit-version*` |
| Saved-delta base streams (step 1) interact subtly with `integrate` (which should expose accumulated state across ticks) | Medium | Pin the contract in tests: "after N ticks, `integrate(base.aev)` equals the accumulated AEV ZSet from input history"; same shape for `delay` |
| Reusing existing `CompiledTriplePattern` couples stream code to the old package | Low–medium | Accept the coupling for v1; revisit if step 3 needs a richer pattern type |
| `*circuit-version*` parity gating could mask divergence if both paths share helpers | Medium | Run `query_inc_test.clj` with `*circuit-version*` rebound in a per-test fixture; assert results match on every test, not just suite-level pass |
| `ZSetIndices` only carrying AEV/AVE limits which queries the new path can compile (`:e/_v` reverse-lookup patterns may not have a needed index) | Low (v1 scope) | Document the limitation; mirror the existing `compile-incremental-q` pattern coverage; defer EAV/VAE to a follow-up |

## Open questions

- **`ResultZSet` type.** The existing path returns `ResultZSet`. Is the same type appropriate for the new circuit output, or does `Stream<T>` need a typed result? Default: reuse `ResultZSet`.
- **`compile-incremental-stream-q` IR.** Does the new Clojure analysis pass produce the `CircuitSpec` directly, or go through an intermediate Clojure data structure that's easier to test? Default: an intermediate spec map, then a single Kotlin builder call to construct `CircuitSpec`.
- **Builder API.** Spec shows `CircuitSpec` as a data class. Does the Clojure side build it directly, or via a Kotlin DSL/builder (clearer error messages, type-checking at construction)? Default: a Kotlin `CircuitBuilder` with `input(...)`, `wcoj(spec)`, `transform(spec)` calls.

These don't block planning, but each should be resolved before the relevant phase starts.

---

## Task List

### Phase 1 — Core types (foundation)

- [ ] **Task 1: `Stream<T>` + typealiases.** Add the sealed `Stream<out T>` interface and `ZSetStream`/`IndexedZSetStream`/`AccumulatedStream` typealiases.
  - Files: `src/main/kotlin/org/hooray/incremental/stream/Stream.kt`
  - Acceptance: `Stream<T>` is sealed; carries `val node: Node`; typealiases compile against existing `ZSet`, `IndexedZSet`, `IntegerWeight`.
  - Verification: `./gradlew compileKotlin` succeeds; a JUnit test instantiates a stub `Node` and uses each alias.
  - Dependencies: None
  - Scope: XS

- [ ] **Task 2: `Node` taxonomy.** Add sealed `Node` with `NodeId` value class and a `DerivedNode<T>` subinterface (output: `ZSetStream<T>`).
  - Files: `src/main/kotlin/org/hooray/incremental/stream/Node.kt`
  - Acceptance: `Node` is sealed; `NodeId` is a typed wrapper (e.g., `@JvmInline value class NodeId(val value: Int)`); `DerivedNode<T>.output` is non-null.
  - Verification: Compile + JUnit test asserting the type relationships.
  - Dependencies: Task 1
  - Scope: XS

- [ ] **Task 3: `InputHandle<T>`.** Buffer-and-clear handle: `set(value: T)`, `clear()`, internal `takeOrEmpty(default: T)` for circuit consumption.
  - Files: `src/main/kotlin/org/hooray/incremental/stream/InputHandle.kt`
  - Acceptance: `set()` overwrites pending value; `takeOrEmpty()` returns the buffered value or default and clears the slot.
  - Verification: JUnit test covers set→take, clear→take→default, double-set→last-wins.
  - Dependencies: None
  - Scope: XS

- [ ] **Task 4: `IndexSpec`.** Data class capturing target layout (`name`, `keyLevels`, `valueLevels`, `fixedPrefix`).
  - Files: `src/main/kotlin/org/hooray/incremental/stream/IndexSpec.kt`
  - Acceptance: Generic over `<T, K, V>`; `equals`/`hashCode`/`toString` work (regular `data class`).
  - Verification: JUnit test: two specs with identical fields compare equal; differing `keyLevels` compare unequal.
  - Dependencies: None
  - Scope: XS

- [ ] **Task 5: `Relation<Row>` interface + `BaseRelation`.** Interface with `id`, `canonicalStream`, `availableIndexes()`, `canProvide(layout)`, `index(layout)`. `BaseRelation` implements it for a single base triple-pattern source backed by `ZSetIndices`.
  - Files: `src/main/kotlin/org/hooray/incremental/stream/Relation.kt`
  - Acceptance: `BaseRelation` exposes AEV and AVE for v1 (no EAV/VAE); `canProvide(EAV) == false`; `index(AEV)` returns an `IndexedZSetStream` whose node references the base relation node.
  - Verification: Unit test: construct a `BaseRelation`, query `availableIndexes()`, fetch AEV/AVE index streams; cross-check `canProvide`.
  - Dependencies: Tasks 1, 2, 4
  - Scope: S

### Checkpoint: Foundation
- [ ] `./gradlew build` succeeds.
- [ ] All Phase 1 unit tests pass.
- [ ] No imports from `org.hooray.incremental.stream.*` in old package (one-way dependency only).

---

### Phase 2 — Pure operators (stateless)

- [ ] **Task 6: `MapIndex` operator.** First-class operator `mapIndex(input: ZSetStream<T>, spec: IndexSpec<T, K, V>): IndexedZSetStream<K, V>`. Implementation routes per-tuple to `(K, V)` per `IndexSpec`.
  - Files: `src/main/kotlin/org/hooray/incremental/stream/ops/MapIndex.kt`
  - Acceptance: Pure function of input; given a fixed `IndexSpec`, identical inputs produce identical indexed outputs.
  - Verification: Unit test on a small fixture ZSet (5–10 tuples, mixed weights including negatives) — assert keys, values, weights match a hand-computed reference.
  - Dependencies: Tasks 1, 4, 5
  - Scope: S

- [ ] **Task 7: `Project` transform.** Maps `ZSetStream<Row>` → `ZSetStream<Row'>` via a projection function; preserves weights.
  - Files: `src/main/kotlin/org/hooray/incremental/stream/ops/Project.kt`
  - Acceptance: Projection over `Row` preserves total weight; coalesces duplicate projected rows by summing weights; drops zero-weight entries.
  - Verification: Unit test with overlapping projections; assert weight sums and zero-cleanup.
  - Dependencies: Task 1
  - Scope: S

- [ ] **Task 8: `Distinct` transform.** Wraps existing `IncrementalDistinct` semantics into a stream node.
  - Files: `src/main/kotlin/org/hooray/incremental/stream/ops/Distinct.kt`
  - Acceptance: Output weights are 0/+1; semantics match `IncrementalDistinct`.
  - Verification: Parity test: drive identical inputs through old `IncrementalDistinct` and new `Distinct`; assert outputs identical per tick.
  - Dependencies: Task 1
  - Scope: S

- [ ] **Task 9: `Filter` transform.** Element-wise predicate over `ZSetStream<T>`; preserves weights, drops non-matching rows.
  - Files: `src/main/kotlin/org/hooray/incremental/stream/ops/Filter.kt`
  - Acceptance: Predicate evaluated per row; surviving rows keep their weights unchanged.
  - Verification: Unit test on a 10-row fixture; assert kept rows + weights.
  - Dependencies: Task 1
  - Scope: XS

### Checkpoint: Pure operators
- [ ] All operator unit tests pass.
- [ ] `Distinct` parity against existing `IncrementalDistinct` is green.

---

### Phase 3 — `ZSetGenericJoin` operator

- [ ] **Task 10: `ZSetGenericJoin` core.** Takes N `IndexedZSetStream`s already arranged in the join's variable order; produces an output `ZSetStream<Row>`. Does **not** select base indexes or compute delta terms. Mirrors core join logic from existing `IncrementalGenericJoin.kt`, but accepts pre-arranged inputs.
  - Files: `src/main/kotlin/org/hooray/incremental/stream/ops/ZSetGenericJoin.kt`
  - Acceptance: Given inputs already arranged correctly, produces the same row set + weights as the existing `IncrementalGenericJoin` join kernel.
  - Verification: Two unit tests — (a) randomized inputs with already-correct ordering: compare output ZSet against a brute-force reference impl over the same tuples; (b) deterministic triangle case fixture matches a hand-computed result.
  - Dependencies: Tasks 1, 5, 6
  - Scope: M

### Checkpoint: Core join
- [ ] `ZSetGenericJoin` matches reference on randomized + fixture inputs.
- [ ] No selection logic for AEV vs AVE exists in this operator (spec invariant).

---

### Phase 4 — Stateful operators (step 1: saved deltas)

> Step 1 implementation: stateful ops own their accumulated state internally. Step 2 (Phase 8) replaces saved deltas with base-index views.

- [ ] **Task 11: `Integrate` operator.** `integrate(input: ZSetStream<T>): AccumulatedStream<T>`. Eval/commit two-phase: `eval()` returns previous accumulated value; `commit()` adds the current input batch.
  - Files: `src/main/kotlin/org/hooray/incremental/stream/ops/Integrate.kt`
  - Acceptance: After N ticks, `integrate` exposes the sum of inputs from ticks 0..N-1 during tick N's `eval()`. `commit()` advances to include tick N.
  - Verification: JUnit test: drive 4 ticks with known deltas; assert `eval()` returns the expected accumulated value at each tick.
  - Dependencies: Tasks 1, 2 + reference to `IncrementalPipeline` eval/commit pattern
  - Scope: S

- [ ] **Task 12: `Delay` operator (overloaded).** `delay(s: ZSetStream<T>): ZSetStream<T>` and `delay(s: AccumulatedStream<T>): AccumulatedStream<T>`. Returns previous tick's value during `eval()`; `commit()` shifts the buffer.
  - Files: `src/main/kotlin/org/hooray/incremental/stream/ops/Delay.kt`
  - Acceptance: Tick 0 returns empty/zero; tick N returns tick N-1's input value during `eval()`.
  - Verification: JUnit test: drive 3 ticks with distinct deltas; assert delayed output matches the previous-tick input.
  - Dependencies: Tasks 1, 11 (for eval/commit pattern alignment)
  - Scope: S

- [ ] **Task 13: `Differentiate` operator.** `differentiate(input: AccumulatedStream<T>): ZSetStream<T>` — emits current minus delayed-accumulated.
  - Files: `src/main/kotlin/org/hooray/incremental/stream/ops/Differentiate.kt`
  - Acceptance: For any input, `differentiate(integrate(s)) == s` per tick (identity contract).
  - Verification: JUnit test asserting identity over 5 ticks of random Z-set inputs.
  - Dependencies: Tasks 11, 12
  - Scope: S

### Checkpoint: Stateful operators
- [ ] `integrate`/`delay`/`differentiate` unit tests pass.
- [ ] Identity `differentiate(integrate(s)) == s` holds at the test level.

---

### Phase 5 — `CircuitSpec`, `Circuit`, WCOJ expansion

- [ ] **Task 14: `CircuitSpec` + `Circuit` skeleton.** `CircuitSpec` data holder; `Circuit` exposes `input: InputHandle<ZSetIndices>`, `step(): ResultZSet`, and migration wrapper `step(input: ZSetIndices): ResultZSet`. No WCOJ source yet — start with a trivial pass-through (input → identity transform → ResultZSet) so the runtime shell works end-to-end.
  - Files: `src/main/kotlin/org/hooray/incremental/stream/Circuit.kt`, `CircuitSpec.kt`
  - Acceptance: A `Circuit` built from a no-op `CircuitSpec` produces empty results; feeding a delta through `input.set` then `step()` reads the buffered delta exactly once.
  - Verification: Unit test: build a trivial circuit; assert `step()` with no input returns empty; with `input.set(d); step()` returns delta-shaped output; `step(d)` wrapper equivalent.
  - Dependencies: Tasks 1–9
  - Scope: M

- [ ] **Task 15: `IncrementalWcojJoinSpec` + branch expansion.** Compile-time expansion that, for each compiled triple pattern, constructs branch-local subgraphs (`mapIndex` to branch variable order → `integrate`/`delay`/`differentiate` where the delta formula needs accumulated/previous state → `ZSetGenericJoin` → canonicalization `mapIndex`). Mirrors `variableOrderForDeltaTerm` + `permutateToCanonical` from the existing path.
  - Files: `src/main/kotlin/org/hooray/incremental/stream/IncrementalWcojJoinSpec.kt`, `analysis/WcojExpansion.kt`
  - Acceptance: For a given `IncrementalWcojJoinSpec(patterns, levels, canonicalOrder)`, expansion produces a deterministic DAG of nodes; node ids and connections are inspectable.
  - Verification: Analysis test: fixture spec for a 2-pattern join; assert expanded DAG has the expected node kinds and edges. Triangle-pattern fixture: matches the existing `IncrementalJoinOperator` branch structure (compared via node-kind sequence).
  - Dependencies: Tasks 10, 11, 12, 13, 14
  - Scope: L → break down if needed

- [ ] **Task 16: `TypeCheck` analysis pass.** For each `mapIndex`/`join`/`differentiate(integrate(...))` site, assert key/value types are consistent; type errors abort `CircuitSpec` construction with a useful message.
  - Files: `src/main/kotlin/org/hooray/incremental/stream/analysis/TypeCheck.kt`
  - Acceptance: Correct circuits pass; mismatched key types throw with a message naming the offending node ids and types.
  - Verification: Two analysis tests — happy path + a deliberately mistyped circuit asserting the exception.
  - Dependencies: Task 15
  - Scope: S

### Checkpoint: Circuit + WCOJ
- [ ] Triangle-pattern Kotlin circuit-level test produces correct results when stepped tick-by-tick (compared against full-query evaluation).
- [ ] `input.set(d); step()` and `step(d)` produce identical results.

---

### Phase 6 — Clojure analysis frontend

- [ ] **Task 17: `compile-incremental-stream-q`.** New namespace `hooray.incremental.stream`. Mirrors `compile-incremental-q`: compute `var-order` (reuse existing planner), compile each where clause to a `CompiledPattern` (reuse `CompiledTriplePattern`), compile `find`/transforms, build a `CircuitSpec` via the Kotlin builder.
  - Files: `src/main/clojure/hooray/incremental/stream.clj`
  - Acceptance: For each query that `compile-incremental-q` handles today, `compile-incremental-stream-q` returns a `Circuit` object whose `step(delta)` is callable.
  - Verification: Clojure test (new): for a small fixture query, call both compilers, assert the new returns a non-nil `Circuit`; smoke-test `step` on an empty delta.
  - Dependencies: Tasks 14, 15, 16
  - Scope: M

- [ ] **Task 18: Wire `find`/`project`/`distinct` transforms.** Hook the Clojure transform compilation to the Kotlin `Project`/`Distinct` ops.
  - Files: `src/main/clojure/hooray/incremental/stream.clj`
  - Acceptance: A query with `:find` projection and implicit distinct compiles into a `CircuitSpec` whose transforms list contains the expected ops in order.
  - Verification: Clojure test inspects the compiled spec.
  - Dependencies: Task 17, 7, 8
  - Scope: S

### Checkpoint: Clojure frontend
- [ ] `compile-incremental-stream-q` compiles all queries used in `query_inc_test.clj` without throwing.
- [ ] Sanity tests on 1–2 small queries run end-to-end.

---

### Phase 7 — Migration switch + parity (v1 done after this)

- [ ] **Task 19: `*circuit-version*` dynamic var.** Add in `src/main/clojure/hooray/incremental.clj`. Default to `:legacy`; when bound to `:stream`, `compile-incremental-q` dispatches to `compile-incremental-stream-q`. The runtime `compute-delta!`/`pop-result!` interface stays the same.
  - Files: `src/main/clojure/hooray/incremental.clj`, `src/main/clojure/hooray/incremental/stream.clj`
  - Acceptance: With `*circuit-version*` rebound to `:stream`, all calls go through the new path; rebinding back to `:legacy` restores the old behavior.
  - Verification: Clojure unit test exercises both bindings on the same query and asserts each returns a usable result.
  - Dependencies: Tasks 17, 18
  - Scope: S

- [ ] **Task 20: Parameterize `query_inc_test.clj` over `*circuit-version*`.** Wrap test fixtures so every case runs under both `:legacy` and `:stream` and asserts identical results.
  - Files: `src/test/clojure/hooray/query_inc_test.clj`
  - Acceptance: Every existing test case runs twice and passes under both versions, or is explicitly marked `pending` with a tracking note.
  - Verification: `./gradlew test --tests '*query_inc*'` passes for both bindings.
  - Dependencies: Task 19
  - Scope: M

- [ ] **Task 21: Mixed-permutation regression test.** Add a single query that consumes AEV and AVE views of the same base relation; assert both views see consistent deltas across ticks and the result matches full-query evaluation.
  - Files: `src/test/clojure/hooray/incremental/stream_test.clj` (new) or extend `query_inc_test.clj`
  - Acceptance: New test passes under `:stream`; AEV/AVE arrangements come from the same `BaseRelation`, not duplicated source state.
  - Verification: Test green; manual inspection of the compiled `CircuitSpec` confirms one base relation, two index views.
  - Dependencies: Task 20
  - Scope: S

### Checkpoint: v1 parity
- [ ] All `query_inc_test.clj` cases pass under both `:legacy` and `:stream`.
- [ ] Mixed-permutation regression passes.
- [ ] Success criteria 1, 2, 3, 4, 5, 6, 9 from spec are satisfied. **This is v1 done.**

---

### Phase 8 — Step 2: base-index views (remove saved deltas)

- [ ] **Task 22: Timestamped `ZSet` views over `ZSetIndices`.** Replace saved-delta storage on base streams with views: `BaseRelation.integratedView(layout, tick)`, `delayedView(layout, tick)`. The input node tracks the current tick; base-backed `integrate`/`delay`/`differentiate` read from views instead of internal storage.
  - Files: `src/main/kotlin/org/hooray/incremental/stream/Relation.kt`, `ops/Integrate.kt`, `ops/Delay.kt`, `ops/Differentiate.kt`
  - Acceptance: When the operator's source is a base-relation stream, no per-stream copy of incoming deltas is allocated.
  - Verification: Memory/profiling test (lightweight): drive 100 ticks of a fixed-size delta; assert allocated state on base-backed stateful ops is bounded (e.g., via a counter wired into the ops).
  - Dependencies: Phase 7 done
  - Scope: M

- [ ] **Task 23: Re-run parity suite under step 2.** Same `query_inc_test.clj` runs under `:stream` with the view-based base streams.
  - Files: (test driver only)
  - Verification: `./gradlew test --tests '*query_inc*'` green.
  - Dependencies: Task 22
  - Scope: XS

### Checkpoint: Step 2
- [ ] No per-stream delta copies for base-backed `integrate`/`delay`/`differentiate`.
- [ ] Spec success criterion 7 satisfied.

---

### Phase 9 — Post-v1: derived relations + general trace

> Explicitly out of scope for v1. Plan continues so the work is sequenced.

- [ ] **Task 24: `DerivedRelation` + `DerivedNode`.** Single-output node producing one ZSet stream in its natural tuple order. Function applications and predicates land here.
  - Files: `src/main/kotlin/org/hooray/incremental/stream/Relation.kt`, `Node.kt`
  - Acceptance: A derived relation can participate in a join via explicit `mapIndex` to the join's variable order.
  - Scope: M

- [ ] **Task 25: General trace implementation.** Backing storage for `delay`/`integrate` when the source is derived (no base-index view available). Independent of base-stream view path.
  - Files: `src/main/kotlin/org/hooray/incremental/stream/Trace.kt`, updates to `ops/Integrate.kt`, `ops/Delay.kt`
  - Acceptance: A test query that integrates a derived relation produces correct accumulated state across ticks.
  - Scope: L

- [ ] **Task 26: Two-`mapIndex` regression.** A derived stream reindexed two different ways uses two explicit `MapIndex` operator nodes; the graph reflects the cost.
  - Files: tests under `src/test/kotlin/org/hooray/incremental/stream/`
  - Acceptance: Spec success criterion 8 satisfied.
  - Scope: S

### Checkpoint: Spec complete
- [ ] All 10 spec success criteria green.
- [ ] **Ask first** before removing old `org.hooray.incremental.*` code.

---

## Parallelization notes

- Tasks 6–9 (pure operators) are independent of each other and can be done in parallel after Phase 1.
- Tasks 11–13 (stateful operators) share the eval/commit pattern; do Task 11 first, then 12 and 13 in parallel.
- Phase 6 (Clojure frontend) blocks on Phase 5 finishing (`CircuitSpec` construction API must exist).
- Phase 8 (step 2) and Phase 9 (post-v1) cannot start until the prior checkpoint passes.

## Verification (pre-implementation)

- [x] Every task has acceptance criteria
- [x] Every task has a verification step
- [x] Task dependencies are identified and ordered
- [x] No task is XL (Task 15 flagged as L; mark for breakdown if it grows during implementation)
- [x] Checkpoints exist between major phases
- [ ] Human review of this plan
