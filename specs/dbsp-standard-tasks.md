# Task Breakdown: DBSP-Standard

Status: **Phase 3 (Tasks)** — awaiting review before Implement.
Companion to `specs/dbsp-standard.md` and `specs/dbsp-standard-plan.md`.

Tasks are dependency-ordered. Each is one focused session, touches ≤5 files, and
ends by running its Verify step and committing. Paths are relative to repo root;
`K/` = `src/main/kotlin/org/hooray/dbsp/`, `KT/` = `src/test/kotlin/org/hooray/dbsp/`.

---

## M1 — Circuit framework

- [ ] **T1 — `Tuple` value type**
  - Acceptance: `Tuple` wraps `Array<Any?>` with structural `equals`/`hashCode`
    (`contentEquals`/`contentHashCode`); positional accessor; `permute(order)`
    and `concat`/projection helpers. Safe as a `ZSet` key.
  - Verify: `./gradlew test --tests "*TupleTest"` — equal tuples hash equal,
    distinct don't, `permute` correct.
  - Files: `K/Tuple.kt`, `KT/TupleTest.kt`.

- [ ] **T2 — Operator / Stream / handle interfaces**
  - Acceptance: `Operator` (`name`), `SourceOperator<O>`, `UnaryOperator<I,O>`,
    `BinaryOperator<I1,I2,O>`; `Stream<D>`; `InputHandle<D>` (`push`);
    `OutputHandle<D>` (read). Type definitions only, no scheduling.
  - Verify: `./gradlew compileKotlin compileTestKotlin` succeeds.
  - Files: `K/Operator.kt`, `K/Stream.kt`.

- [ ] **T3 — `Circuit` builder + topological scheduler** *(CP1)*
  - Acceptance: `Circuit` with `addInput()→(Stream,InputHandle)`, `addUnary`,
    `addBinary`, immutable after `build()`, `step()`. Kahn-style scheduling;
    every node evaluated exactly once per step in dependency order.
  - Verify: `./gradlew test --tests "*CircuitTest"` — linear chain
    `source→map→output`; diamond DAG (source → 2×unary → binary → output)
    evaluates in correct order, each node once.
  - Files: `K/Circuit.kt`, `KT/CircuitTest.kt`.

## M2 — Operators

- [ ] **T4 — `Source` / `Filter` / `Map` operators**
  - Acceptance: `SourceOp` (InputHandle-backed, emits pushed `ZSet`, empty if
    none); `FilterOp` (keep entries by key predicate); `MapOp` (key-transform,
    summing weights of colliding keys — bag).
  - Verify: `./gradlew test --tests "*LinearOpsTest"` — filter/map on
    `ZSet<Tuple>`, incl. weight collision in map.
  - Files: `K/SourceOp.kt`, `K/FilterOp.kt`, `K/MapOp.kt`, `KT/LinearOpsTest.kt`.

- [ ] **T5 — `Integrate` / `Differentiate` / `Z1`** *(CP2 part)*
  - Acceptance: `IntegrateOp` (running accumulator); `DifferentiateOp`
    (`x − prev`); `Z1Op` (unit delay, first step emits empty).
  - Verify: `./gradlew test --tests "*PrimitiveOpsTest"` — `D∘I = id`,
    `I∘D = id` over a delta sequence; `Z1` shifts by one.
  - Files: `K/IntegrateOp.kt`, `K/DifferentiateOp.kt`, `K/Z1Op.kt`,
    `KT/PrimitiveOpsTest.kt`.

- [ ] **T6 — `Plus` / `Distinct`** *(CP2 part)*
  - Acceptance: `PlusOp` (binary z-set add + minus/negate variant); `DistinctOp`
    (positive weights → 1, incremental threshold-crossing over accumulated
    state, matching `IncrementalDistinct` semantics).
  - Verify: `./gradlew test --tests "*GroupOpsTest"` — group laws for plus/minus;
    distinct crossings (`0→+ ⇒ +1`, `+→0 ⇒ −1`).
  - Files: `K/PlusOp.kt`, `K/DistinctOp.kt`, `KT/GroupOpsTest.kt`.

## M3 — Joins

- [ ] **T7 — `StreamJoin` (non-incremental bilinear join)**
  - Acceptance: joins two `ZSet<Tuple>` on leading-k columns; concatenated
    tuples (duplicate join columns dropped), weights multiplied; handles empty
    key (Cartesian).
  - Verify: `./gradlew test --tests "*StreamJoinTest"` — small joins, Cartesian,
    no-match.
  - Files: `K/StreamJoinOp.kt`, `KT/StreamJoinTest.kt`.

- [ ] **T8 — `IncrementalJoin` (fused bilinear)**
  - Acceptance: holds running integrals of both inputs; `eval(da,db) =
    da⋈Ibₙₑw + Iaₒₗd⋈db`; weights multiplied; formula derived in comments.
  - Verify: `./gradlew compileKotlin`; two hand-traced steps assert correct
    deltas (full proof in T9).
  - Files: `K/IncrementalJoinOp.kt`.

- [ ] **T9 — Fused-vs-composed join oracle** *(CP3)*
  - Acceptance: a composed incremental join built in test code from
    `Integrate`/`Z1`/`StreamJoin`/`Plus`; randomized differential test asserts
    fused `IncrementalJoin` == composed over random delta sequences, incl. the
    `Δa⋈Δb` cross term and the empty-key case.
  - Verify: `./gradlew test --tests "*IncrementalJoinTest"` green.
  - Files: `KT/IncrementalJoinTest.kt`, `KT/ComposedJoin.kt` (test helper).

## M4 — Query analysis *(Clojure; parallelizable with M1–M3)*

- [ ] **T10 — Pattern descriptors + left-deep order**
  - Acceptance: in `hooray/dbsp.clj` — parse via `::query`; compile each triple
    pattern to a descriptor (const attr, const/var `e`&`v`, var names); build
    join graph; deterministic left-deep order.
  - Verify: `./gradlew test --tests "*dbsp*"` analysis tests — stable order;
    single-pattern; disconnected (Cartesian) patterns.
  - Files: `src/main/clojure/hooray/dbsp.clj`,
    `src/test/clojure/hooray/dbsp_test.clj`.

- [ ] **T11 — Join plan (keys, permutations, intermediate Maps)** *(CP4)*
  - Acceptance: per-step join keys; per-pattern `AEV`/`AVE` assignment;
    intermediate `Map` column orders; emit a full plan as EDN data.
  - Verify: analysis tests — correct keys/permutations/intermediate orders for
    chain, triangle, self-join, Cartesian; plan deterministic (build twice ⇒
    equal EDN).
  - Files: `src/main/clojure/hooray/dbsp.clj`,
    `src/test/clojure/hooray/dbsp_test.clj`.

## M5 — Assembly + delta path

- [ ] **T12 — Plan → Kotlin `Circuit` assembly**
  - Acceptance: interop spike confirmed; `plan->circuit` builds the Kotlin
    `Circuit` — per-pattern `Source→Filter→Map`, left-deep `IncrementalJoin`
    chain with permuting `Map`s, final projection `Map`, one `OutputHandle`;
    returns the circuit + per-pattern `InputHandle`s.
  - Verify: a 3-pattern query assembles; assert operator count/names match the
    plan.
  - Files: `src/main/clojure/hooray/dbsp.clj`,
    `src/test/clojure/hooray/dbsp_test.clj`.

- [ ] **T13 — Per-pattern `AEV`/`AVE` delta builder**
  - Acceptance: from `tx-data` + db-before, build per-pattern `AEV`/`AVE` delta
    `ZSet<Tuple>`s, reusing `hooray.incremental` retract/cardinality logic.
  - Verify: test — add / retract / update produce expected flat permuted deltas
    with correct weights.
  - Files: `src/main/clojure/hooray/dbsp.clj`,
    `src/test/clojure/hooray/dbsp_test.clj`.

- [ ] **T14 — `compile` + `compute-delta!`** *(CP5)*
  - Acceptance: `dbsp/query` compiles a query (plan → circuit); `compute-delta!`
    pushes each pattern's chosen permutation via `InputHandle`, `step`s, reads
    the `OutputHandle`, formats `[[tuple weight]…]`; result queue + `pop-result!`
    mirroring `hooray.incremental`.
  - Verify: hand-traced queries (single pattern, 2-join, update) fed directly to
    the circuit; assert deltas.
  - Files: `src/main/clojure/hooray/dbsp.clj`,
    `src/test/clojure/hooray/dbsp_test.clj`.

## M6 — Core integration

- [ ] **T15 — `*dbsp-version*` + `transact` dispatch**
  - Acceptance: `^:dynamic *dbsp-version*` (default `:wcoj`) in `hooray.core`;
    `q-inc` records the version on the `!inc-qs` entry; `transact` dispatches
    `:wcoj → incremental/compute-delta!`, `:standard → dbsp/compute-delta!`.
    `:wcoj` behaviour unchanged.
  - Verify: `./gradlew test` — full existing suite green; a `:standard`-bound
    `q-inc` registers and is dispatched to the dbsp path.
  - Files: `src/main/clojure/hooray/core.clj`.

- [ ] **T16 — End-to-end + cross-engine tests** *(CP6)*
  - Acceptance: end-to-end tests under `(binding [*dbsp-version* :standard])` —
    single, update, 2-join, 3+-chain, self-join, triangle, Cartesian; a
    cross-engine test runs each query under both `:wcoj` and `:standard` and
    asserts identical delta multisets.
  - Verify: `./gradlew test` fully green; Success Criteria 1–7 in
    `specs/dbsp-standard.md` satisfied.
  - Files: `src/test/clojure/hooray/dbsp_test.clj`.

---

## Summary

16 tasks, 6 milestones. Critical path `T1→T2→T3→T5→T8→T9→T12→T13→T14→T15→T16`.
M4 (T10–T11) is pure Clojure and may run in parallel with M1–M3. Checkpoints:
CP1=T3, CP2=T5+T6, CP3=T9, CP4=T11, CP5=T14, CP6=T16.
