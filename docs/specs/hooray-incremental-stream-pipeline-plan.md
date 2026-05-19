# Implementation Plan: Hooray Incremental Stream Pipeline

## Overview

Build the stream/circuit pipeline alongside the current
`org.hooray.incremental.*` implementation, then route Clojure incremental
queries through it behind `*circuit-version*`. The first version targets only
base triple relations using the currently supported AEV/AVE delta indexes. It
keeps `mapIndex`, `integrate`, `delay`, and `differentiate` as explicit stream
operators in the graph model, but does not implement derived relations or a
general trace store.

## Architecture Decisions

- Keep the current public Clojure API (`query`, `compute-delta!`,
  `pop-result!`) stable; switch implementation with dynamic var
  `*circuit-version*`.
- Put new Kotlin code under `org.hooray.incremental.stream.*`; leave the old
  `IncrementalPipeline`, `IncrementalJoinOperator`, and
  `IncrementalWcojJoinEngine` untouched until parity is proven.
- Preserve the existing Clojure compiler shape: `compile-inc-pattern` produces
  compiled triple patterns; the stream compiler turns those into a
  `CircuitSpec`.
- Use one external `InputHandle<ZSetIndices>` and one output `ResultZSet`.
- Treat AEV/AVE selection for base triple patterns as compiled-pattern
  knowledge. `IndexSpec` is only for explicit `mapIndex` nodes, not for choosing
  physical base arrangements.

## Dependency Graph

```text
Stream model + InputHandle + CircuitSpec
    │
    ├── Base-backed operators: integrate / delay / differentiate / mapIndex
    │       │
    │       └── IncrementalWcojJoinSpec execution facade
    │               │
    │               └── Clojure compile-incremental-stream-q
    │                       │
    │                       └── *circuit-version* dispatch + parity tests
    │
    └── Analysis/spec tests
```

## Task List

### Phase 1: Kotlin Stream Foundations

## Task 1: Add stream model and circuit shell

**Description:** Create the new `org.hooray.incremental.stream` package with
the minimal graph types needed to represent a compiled circuit: `Stream`,
`Node`, `InputHandle`, `CircuitSpec`, `IncrementalWcojJoinSpec`, and `Circuit`.
`Circuit.step(input)` should be a compatibility wrapper around
`input.set(delta); step()`.

**Acceptance criteria:**
- [ ] `InputHandle<ZSetIndices>` can buffer, clear, and expose one pending input.
- [ ] `Circuit.step()` returns an empty `ResultZSet` when no input is set.
- [ ] `Circuit.step(input)` and `input.set(input); step()` are equivalent.

**Verification:**
- [ ] Tests pass: `./gradlew test --tests 'org.hooray.incremental.stream.*'`
- [ ] Build succeeds for the new package.

**Dependencies:** None

**Files likely touched:**
- `src/main/kotlin/org/hooray/incremental/stream/Circuit.kt`
- `src/main/kotlin/org/hooray/incremental/stream/CircuitSpec.kt`
- `src/main/kotlin/org/hooray/incremental/stream/InputHandle.kt`
- `src/main/kotlin/org/hooray/incremental/stream/Stream.kt`
- `src/test/kotlin/org/hooray/incremental/stream/CircuitTest.kt`

**Estimated scope:** Medium: 4-5 files

## Task 2: Add compiled pattern/spec bridge types

**Description:** Add stream-package spec types that mirror the current compiled
join path without introducing a fluent builder API. The bridge should accept
the existing `CompiledTriplePattern` values or wrap them without changing their
current behavior.

**Acceptance criteria:**
- [ ] `IncrementalWcojJoinSpec` stores compiled patterns, level count, and
  canonical variable order.
- [ ] Tests can construct a `CircuitSpec` from existing
  `CompiledTriplePattern` instances.
- [ ] No code under `org.hooray.incremental.*` is deleted or behavior-changed.

**Verification:**
- [ ] Tests pass: `./gradlew test --tests 'org.hooray.incremental.stream.*'`
- [ ] Existing join tests still pass:
  `./gradlew test --tests 'org.hooray.incremental.IncrementalWcojJoinTest'`

**Dependencies:** Task 1

**Files likely touched:**
- `src/main/kotlin/org/hooray/incremental/stream/Pattern.kt`
- `src/main/kotlin/org/hooray/incremental/stream/CircuitSpec.kt`
- `src/test/kotlin/org/hooray/incremental/stream/PatternSpecTest.kt`

**Estimated scope:** Small: 2-3 files

## Task 3: Add first-class stream operator nodes

**Description:** Add explicit node/spec representations for `mapIndex`,
`integrate`, `delay`, and `differentiate`. For this task, these can be graph
objects with simple base-backed semantics where needed; the goal is to make the
operator vocabulary real before wiring WCOJ through it.

**Acceptance criteria:**
- [ ] `mapIndex` accepts an `IndexSpec` that records key/value level demand.
- [ ] `integrate`, `delay`, and `differentiate` are distinct node kinds.
- [ ] Unit tests assert operator identity/typing contracts such as
  `differentiate(integrate(x))` at the graph/spec level.

**Verification:**
- [ ] Tests pass: `./gradlew test --tests 'org.hooray.incremental.stream.ops.*'`

**Dependencies:** Tasks 1-2

**Files likely touched:**
- `src/main/kotlin/org/hooray/incremental/stream/IndexSpec.kt`
- `src/main/kotlin/org/hooray/incremental/stream/ops/MapIndex.kt`
- `src/main/kotlin/org/hooray/incremental/stream/ops/StateOperators.kt`
- `src/test/kotlin/org/hooray/incremental/stream/ops/StreamOperatorTest.kt`

**Estimated scope:** Medium: 4 files

### Checkpoint: Foundations

- [ ] `./gradlew test --tests 'org.hooray.incremental.stream.*'`
- [ ] `./gradlew test --tests 'org.hooray.incremental.IncrementalWcojJoinTest'`
- [ ] Confirm no old incremental classes were deleted or routed through the new
  path yet.

### Phase 2: Base-Only Circuit Execution

## Task 4: Wrap current WCOJ engine behind Circuit.step

**Description:** Implement the first executable `Circuit` source by delegating
to the current `IncrementalWcojJoinEngine` through an
`IncrementalWcojJoinSpec`. This gives the new runtime API parity before
decomposing the join into smaller stream nodes.

**Acceptance criteria:**
- [ ] A `CircuitSpec` with an `IncrementalWcojJoinSpec` produces the same
  result as `IncrementalJoinOperator` for the same `ZSetIndices`.
- [ ] `Circuit.step()` uses the input handle and clears/consumes the pending
  input exactly once per step.
- [ ] Initialization from a full DB snapshot can be represented by one
  `step(zsetIndices)` call, matching the current compiler behavior.

**Verification:**
- [ ] Tests pass: `./gradlew test --tests 'org.hooray.incremental.stream.*'`
- [ ] Existing Kotlin WCOJ tests still pass.

**Dependencies:** Tasks 1-2

**Files likely touched:**
- `src/main/kotlin/org/hooray/incremental/stream/Circuit.kt`
- `src/main/kotlin/org/hooray/incremental/stream/IncrementalWcojSource.kt`
- `src/test/kotlin/org/hooray/incremental/stream/IncrementalWcojCircuitTest.kt`

**Estimated scope:** Medium: 3-4 files

## Task 5: Add transform execution for find/project

**Description:** Add transform specs and execution for the current find/project
step so the stream circuit can return query-shaped rows, not only canonical
WCOJ tuples.

**Acceptance criteria:**
- [ ] A `ProjectSpec` can reorder canonical WCOJ tuples into the requested
  `:find` order.
- [ ] The output matches the current `compile-find` transform for representative
  one-variable and multi-variable queries.
- [ ] Distinct remains disabled unless the old path enables it for the same
  query shape.

**Verification:**
- [ ] Tests pass: `./gradlew test --tests 'org.hooray.incremental.stream.*'`
- [ ] Existing Clojure incremental tests still pass on the old default path.

**Dependencies:** Task 4

**Files likely touched:**
- `src/main/kotlin/org/hooray/incremental/stream/TransformSpec.kt`
- `src/main/kotlin/org/hooray/incremental/stream/ops/Project.kt`
- `src/main/kotlin/org/hooray/incremental/stream/Circuit.kt`
- `src/test/kotlin/org/hooray/incremental/stream/ProjectCircuitTest.kt`

**Estimated scope:** Medium: 3-4 files

## Task 6: Make base stream state explicit

**Description:** Replace the opaque engine delegation with a base-stream
execution path that makes current/delta state visible as first-class base
stream views. This can still save incoming deltas internally, matching
implementation step 1 from the spec.

**Acceptance criteria:**
- [ ] Base relation stream state stores current and delta arrangements for the
  current step.
- [ ] WCOJ branch execution chooses delta for the active pattern and delayed
  current state for other patterns.
- [ ] Tests cover mixed AEV/AVE variable-order demand in one query.

**Verification:**
- [ ] Tests pass: `./gradlew test --tests 'org.hooray.incremental.stream.*'`
- [ ] Tests pass:
  `./gradlew test --tests 'org.hooray.incremental.IncrementalWcojJoinTest'`

**Dependencies:** Tasks 3-5

**Files likely touched:**
- `src/main/kotlin/org/hooray/incremental/stream/BaseRelationStream.kt`
- `src/main/kotlin/org/hooray/incremental/stream/IncrementalWcojSource.kt`
- `src/main/kotlin/org/hooray/incremental/stream/ops/ZSetGenericJoin.kt`
- `src/test/kotlin/org/hooray/incremental/stream/BaseRelationStreamTest.kt`
- `src/test/kotlin/org/hooray/incremental/stream/IncrementalWcojCircuitTest.kt`

**Estimated scope:** Medium: 4-5 files

### Checkpoint: Base Circuit Parity

- [ ] New Kotlin stream tests pass.
- [ ] Existing Kotlin incremental tests pass.
- [ ] A direct circuit test proves `input.set(delta); step()` and `step(delta)`
  produce identical deltas across multiple ticks.

### Phase 3: Clojure Compiler and Migration Switch

## Task 7: Add Clojure stream compiler entry

**Description:** Add `hooray.incremental.stream/compile-incremental-stream-q`
that mirrors `compile-incremental-q`: compute `var-order`, compile triple
patterns, compile find/project transforms, initialize with the DB snapshot, and
return a circuit object.

**Acceptance criteria:**
- [ ] The stream compiler rejects the same unsupported query features as the
  old incremental compiler.
- [ ] Initial DB state is loaded with one circuit step before live deltas.
- [ ] The compiler does not introduce a fluent `.baseRelation(...)` style API.

**Verification:**
- [ ] Clojure tests for compiler construction pass.
- [ ] Existing old-path Clojure tests still pass.

**Dependencies:** Tasks 4-5, preferably Task 6

**Files likely touched:**
- `src/main/clojure/hooray/incremental/stream.clj`
- `src/test/clojure/hooray/incremental_stream_test.clj`

**Estimated scope:** Small: 2 files

## Task 8: Add `*circuit-version*` dispatch

**Description:** Add the dynamic var in `hooray.incremental` and route
`query`/`compute-delta!` through either the current `IncrementalPipeline` or
the new stream circuit. Keep the default on the old path until parity is
proven.

**Acceptance criteria:**
- [ ] Default behavior uses the old pipeline.
- [ ] Binding `*circuit-version*` to the stream version uses the new compiler
  and circuit runtime.
- [ ] `compute-delta!` and `pop-result!` keep their public behavior unchanged.

**Verification:**
- [ ] Tests pass: `./gradlew test --tests '*query_inc*'`
- [ ] Add a focused Clojure test that binds `*circuit-version*` and consumes one
  delta.

**Dependencies:** Task 7

**Files likely touched:**
- `src/main/clojure/hooray/incremental.clj`
- `src/main/clojure/hooray/incremental/stream.clj`
- `src/test/clojure/hooray/query_inc_test.clj`
- `src/test/clojure/hooray/incremental_stream_test.clj`

**Estimated scope:** Medium: 3-4 files

## Task 9: Run Clojure parity suite under both versions

**Description:** Parameterize the existing incremental query tests so each case
can run against the old implementation and the stream implementation. Keep
failures isolated by circuit version.

**Acceptance criteria:**
- [ ] Existing `query_inc_test.clj` cases run against both circuit versions.
- [ ] Failures report which version failed.
- [ ] The stream version matches old-path deltas for sanity, retraction,
  distinct-semantics, prefix-extension, triangle, and mixed-permutation cases.

**Verification:**
- [ ] Tests pass: `./gradlew test --tests '*query_inc*'`
- [ ] Full build succeeds: `./gradlew build`

**Dependencies:** Task 8

**Files likely touched:**
- `src/test/clojure/hooray/query_inc_test.clj`
- `src/test/clojure/hooray/incremental_stream_test.clj`

**Estimated scope:** Small: 1-2 files

### Checkpoint: Clojure Parity

- [ ] `./gradlew test --tests 'org.hooray.incremental.stream.*'`
- [ ] `./gradlew test --tests '*query_inc*'`
- [ ] `./gradlew build`
- [ ] Review any parity differences before making stream the default.

### Phase 4: Base Stream Storage Optimization

## Task 10: Replace saved base deltas with timestamped base-index views

**Description:** Implement the second base-only version from the spec: base
streams should expose views over `ZSetIndices` and accumulated base state
instead of saving every incoming delta batch as stream-local data.

**Acceptance criteria:**
- [ ] Base stream delta views read the current input batch for the active step.
- [ ] Base stream current/accumulated views reflect all committed prior steps.
- [ ] `integrate`, `delay`, and `delay().integrate()` behavior remains covered
  by tests.

**Verification:**
- [ ] New stream tests pass.
- [ ] Clojure parity suite passes under both versions.
- [ ] Full build succeeds.

**Dependencies:** Tasks 6 and 9

**Files likely touched:**
- `src/main/kotlin/org/hooray/incremental/stream/BaseRelationStream.kt`
- `src/main/kotlin/org/hooray/incremental/stream/ops/Integrate.kt`
- `src/main/kotlin/org/hooray/incremental/stream/ops/Delay.kt`
- `src/test/kotlin/org/hooray/incremental/stream/BaseRelationStreamTest.kt`

**Estimated scope:** Medium: 3-4 files

## Task 11: Make stream circuit the default

**Description:** Once parity is green, flip the default `*circuit-version*` to
the stream implementation while preserving a way to bind back to the old path
during transition.

**Acceptance criteria:**
- [ ] Unbound `*circuit-version*` uses the stream circuit.
- [ ] Binding to the old version still works.
- [ ] Documentation/spec comments identify old-path removal as a separate
  ask-first task.

**Verification:**
- [ ] `./gradlew build`
- [ ] Clojure parity suite passes with default stream behavior and explicit old
  behavior.

**Dependencies:** Task 10

**Files likely touched:**
- `src/main/clojure/hooray/incremental.clj`
- `src/test/clojure/hooray/query_inc_test.clj`
- `docs/specs/hooray-incremental-stream-pipeline.md`

**Estimated scope:** Small: 2-3 files

### Checkpoint: Ready for Review

- [ ] Full build passes.
- [ ] Old and new paths are both testable.
- [ ] No old incremental implementation has been deleted.
- [ ] The spec and plan still match the implemented API.

## Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Current `ZSetIndices` only contains AEV/AVE, while the spec discusses EAV/VAE as physical base streams. | Medium | Keep v1 scoped to the existing AEV/AVE compiler support; add EAV/VAE only when query compilation needs them. |
| Reusing `IncrementalWcojJoinEngine` too long could hide the stream operator shape. | Medium | Use engine delegation only as Task 4 parity scaffolding; Task 6 makes base stream state and join branches explicit. |
| Clojure/Kotlin interop can obscure type errors until runtime. | Medium | Add focused Clojure compiler tests before parameterizing the full query suite. |
| `integrate`/`delay` semantics can regress if state advances too early. | High | Add multi-tick tests that verify previous-state visibility before Clojure parity work. |
| `IndexSpec` could drift into base-index selection. | Medium | Keep tests and docs asserting that base AEV/AVE choice remains on compiled triple patterns. |

## Open Questions

- Should v1 introduce EAV/VAE fields on `ZSetIndices`, or should that wait
  until the compiler supports query shapes that need them?
- Should the stream circuit initially delegate to `IncrementalWcojJoinEngine`
  for a faster parity checkpoint, or should Task 4 and Task 6 be collapsed to
  avoid temporary scaffolding?
