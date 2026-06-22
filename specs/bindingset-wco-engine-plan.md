# Implementation Plan: BindingSet WCO Query Engine

Companion plan for `specs/bindingset-wco-engine.md`.
Status: **Phase 2 (Plan)** - task breakdown drafted in
`specs/bindingset-wco-engine-tasks.md`; awaiting approval before
implementation.

## Overview

Build a new row-shaped query engine around `BindingSet` and staged WCO-style
execution. The goal is to fix branch-local `or` semantics without extending the
old scalar prefix abstraction. Patterns may introduce one or more variables in a
stage, and every validation step works on complete binding rows rather than
standalone scalar proposals.

This plan keeps the existing scalar `:generic` join implementation in the tree
while the new engine is built and tested. The new planner and executor should be
introduced side by side first; routing public `:generic` queries through the new
engine is a later cutover task after parity coverage is in place.

## Architecture Decisions

- **`BindingSet` is the engine-local result representation.** It carries a
  variable layout and correlated rows. There is no projection or liveness
  pruning in v1 beyond final `:find` extraction.
- **Planning lives in an explicit namespace.** Add `hooray.query.plan` for
  structured stage planning instead of hiding the new planner in
  `hooray.query`.
- **Execution lives outside iterator/prefix machinery.** Add a new Kotlin
  runtime package such as `org.hooray.engine`.
- **The old scalar generic join remains for now.** Do not delete `GenericJoin`,
  `PrefixExtender`, or their existing tests during the first implementation.
- **`or` is conservative in v1.** In mixed stages it validates rows proposed by
  ordinary patterns. It only proposes in a dedicated OR boundary when no
  non-OR pattern can introduce the needed variables.
- **Partial validators participate early.** A pattern whose variables overlap
  newly introduced variables should validate as an existential semijoin even if
  not all of its variables are bound yet.
- **Debug and introspection stay lightweight.** Prefer plain planner data that
  tests and callers can inspect. Do not add a separate tracing or debug
  subsystem unless it stays small and does not obscure the engine code.

## Dependency Graph

```
T1 BindingSet core
    |
    v
T2 executable pattern contracts
    |
    v
T3 stage executor
    |
    +----> T4 triple/input/predicate/function executors
    |          |
    |          v
    +----> T5 structured planner
               |
               v
T6 internal query wiring and final result shaping
               |
               v
T7 OR validator mode
               |
               v
T8 OR dedicated proposal boundary
               |
               v
T9 parity, cutover preparation, and cleanup boundaries
```

T1 and the pure planner shape in T5 can be sketched in parallel once the public
data shapes are agreed. T3 depends on the runtime contracts from T2. T7 and T8
depend on both planner and executor behavior.

## Task List

### Phase 1: Runtime Foundation

#### Task 1: Add `BindingSet` core runtime

**Description.** Add the list-backed runtime representation used by the new
engine. It should track a stable variable layout, store correlated rows, and
provide small helpers for row extension, row alignment, full-row distinct, and
target-layout reordering.

**Acceptance criteria:**
- [ ] `BindingSet` stores variables and rows with a single unambiguous layout.
- [ ] Helpers can extend rows with new variables without breaking existing
      correlations.
- [ ] Full-row distinct deduplicates complete rows, not scalar values.
- [ ] Reordering to a target variable layout is explicit and tested.

**Verification:**
- [ ] Focused Kotlin tests cover construction, extension, distinct, and
      reordering.
- [ ] `./gradlew test` passes.

**Dependencies:** None.

**Files likely touched:**
- `src/main/kotlin/org/hooray/engine/*`
- `src/test/kotlin/org/hooray/engine/*`

**Estimated scope:** M (new runtime type plus focused tests).

---

#### Task 2: Define executable pattern and stage contracts

**Description.** Introduce the small set of runtime contracts the executor will
use: stages, executable patterns, proposer eligibility, counts, proposal, and
validation. The contracts must distinguish ordinary proposer participants from
validator-only participants such as predicates and mixed-stage `or`.

**Acceptance criteria:**
- [ ] A stage records introduced variables, participants, and target variables.
- [ ] Executable patterns expose `count`, `propose`, and `validate` operations.
- [ ] Proposer eligibility is explicit and not inferred from method presence.
- [ ] A stage can represent zero-introduce validation, ordinary proposal, and
      dedicated OR proposal boundaries.

**Verification:**
- [ ] Contract-level tests or fixtures can express a one-pattern proposing
      stage, a multi-participant stage, and a validator-only stage.
- [ ] `./gradlew test` passes.

**Dependencies:** T1.

**Files likely touched:**
- `src/main/kotlin/org/hooray/engine/*`
- `src/test/kotlin/org/hooray/engine/*`

**Estimated scope:** S to M (interfaces/data classes and tests).

### Checkpoint: Runtime Shape

- [ ] `BindingSet` and stage contracts are stable enough for planner and
      executor work.
- [ ] Existing scalar generic join code still compiles unchanged.
- [ ] `./gradlew test` passes.

### Phase 2: Stage Execution

#### Task 3: Implement the staged WCO executor loop

**Description.** Implement ordinary stage execution over a `BindingSet`.
Multi-participant stages build a per-row count table for proposer-eligible
participants, partition input rows by cheapest positive proposer, expand each
shard, validate with the remaining participants, then merge and distinct full
rows.

**Acceptance criteria:**
- [ ] Zero-introduce stages run validation only.
- [ ] Single-proposer stages expand the full input set.
- [ ] Multi-proposer stages choose a proposer per input row from counts.
- [ ] Rows whose minimum proposer count is zero are dropped before proposal.
- [ ] Non-proposer participants validate expanded rows as semijoins.
- [ ] Output rows are reordered to the stage target layout.

**Verification:**
- [ ] Kotlin tests cover zero-introduce validation, single proposal,
      multi-proposer count grouping, zero-count pruning, validation, merge, and
      full-row distinct.
- [ ] `./gradlew test` passes.

**Dependencies:** T1, T2.

**Files likely touched:**
- `src/main/kotlin/org/hooray/engine/*`
- `src/test/kotlin/org/hooray/engine/*`

**Estimated scope:** M (core executor plus test doubles).

---

#### Task 4: Implement non-OR executable patterns

**Description.** Add executable patterns for triples, input bindings,
predicates, and functions. Triple patterns use the existing indexes for counts,
proposal, full validation, and partial existential validation. Predicates never
propose. Functions propose their return variable when inputs are bound and
validate when the return is already bound.

**Acceptance criteria:**
- [ ] Triple patterns can propose one or more missing variables.
- [ ] Triple patterns can validate fully bound rows.
- [ ] Triple patterns can partially validate rows with an existential indexed
      completion check.
- [ ] `:in` scalar, tuple, and relation bindings preserve input correlation.
- [ ] Predicates validate only when all argument variables are bound.
- [ ] Functions propose or validate according to existing function semantics.

**Verification:**
- [ ] Focused engine tests cover triple proposal, triple full validation,
      triple partial semijoin validation, `:in` relation correlation,
      predicates, and functions.
- [ ] `./gradlew test` passes.

**Dependencies:** T3.

**Files likely touched:**
- `src/main/kotlin/org/hooray/engine/*`
- Existing query/index interop files as needed
- `src/test/kotlin/org/hooray/engine/*`

**Estimated scope:** M (several pattern executors, mostly additive).

### Checkpoint: Non-OR Engine Slice

- [ ] A small all-triple query can run through the new runtime internally.
- [ ] Predicate/function behavior is represented in the new runtime.
- [ ] Existing public query tests still pass through the old path.
- [ ] `./gradlew test` passes.

### Phase 3: Planning and Query Wiring

#### Task 5: Add structured planner in `hooray.query.plan`

**Description.** Convert the conformed query shape into explicit stage data.
The planner flattens `and`, treats constants as constraints, tracks bound
variables from `:in`, chooses variable groups that patterns can ground, and
adds every newly relevant full or partial validator to the stage.

**Acceptance criteria:**
- [ ] Planner output is plain structured data suitable for tests and light
      introspection.
- [ ] `and` clauses are flattened.
- [ ] Input bindings seed the initial bound set.
- [ ] Triple and binding patterns can introduce multiple variables when that is
      their natural output shape.
- [ ] Predicates are planned as validators, never proposers.
- [ ] Partially covered patterns are included as existential semijoin
      validators when they overlap newly introduced variables.
- [ ] `or` has explicit planned roles: mixed-stage validator or dedicated OR
      proposal boundary.

**Verification:**
- [ ] Clojure planner tests cover single-variable stages, multi-variable
      stages, predicates, functions, flattened `and`, partial validators, input
      relation bindings, and OR role selection.
- [ ] `./gradlew test` passes.

**Dependencies:** T2 for final runtime shape. The planner tests can start
earlier against plain data.

**Files likely touched:**
- `src/main/clojure/hooray/query/plan.clj`
- `src/test/clojure/hooray/query/plan_test.clj`
- Existing query namespace only for narrow integration hooks

**Estimated scope:** M (new planner namespace plus focused tests).

---

#### Task 6: Wire the new engine through an internal query path

**Description.** Add a narrow internal path from conformed query to planner,
executor, and final result shaping. Keep the public `h/q` surface unchanged and
keep the old scalar generic implementation available while parity is built.
Only after enough tests pass should the `:generic` public path be routed to the
new engine.

**Acceptance criteria:**
- [ ] There is an internal entry point that executes a conformed query with the
      new planner and `BindingSet` executor.
- [ ] Final `:find`, aggregate, `:keys`, `:strs`, and `:syms` shaping remains
      centralized and matches existing public behavior.
- [ ] Existing public `:generic` behavior can still run through the old scalar
      path during development.
- [ ] The eventual cutover point is small and explicit.

**Verification:**
- [ ] Query-level parity tests can run selected existing generic cases through
      the new internal path.
- [ ] Existing public generic query tests still pass.
- [ ] `./gradlew test` passes.

**Dependencies:** T3, T4, T5.

**Files likely touched:**
- `src/main/clojure/hooray/query.clj`
- `src/main/clojure/hooray/query/plan.clj`
- Kotlin interop files as needed
- Existing query tests plus new parity tests

**Estimated scope:** M (interop and result shaping).

### Checkpoint: Planner to Executor Slice

- [ ] A representative non-OR query executes end to end through the new
      internal path.
- [ ] Planner output is inspectable without a custom debug subsystem.
- [ ] Existing public query behavior remains green.
- [ ] `./gradlew test` passes.

### Phase 4: OR Semantics

#### Task 7: Implement OR validator mode

**Description.** Implement `or` as a branch-complete existential validator for
mixed stages. Each branch receives the same seeded input rows and succeeds for
a row only when that branch can complete consistently with the row's currently
bound OR variables. No branch-local scalar proposal is exposed to sibling
branches or to the outer executor.

**Acceptance criteria:**
- [ ] Mixed-stage `or` is not proposer-eligible and does not appear in ordinary
      count tables.
- [ ] Each branch runs from the same seeded `BindingSet`.
- [ ] A row is kept if at least one branch has a completion for the current row
      shape.
- [ ] Branch predicates and functions validate only rows produced inside that
      branch.
- [ ] Revalidating at later stages asks the complete branch-existence question
      again for the newer row shape.

**Verification:**
- [ ] Query-level tests cover the issue #14 motivating query.
- [ ] Tests cover `or` validating level by level when ordinary outer patterns
      introduce the OR variables.
- [ ] Tests cover predicates and functions inside branches.
- [ ] `./gradlew test` passes.

**Dependencies:** T5, T6.

**Files likely touched:**
- `src/main/clojure/hooray/query/plan.clj`
- `src/main/kotlin/org/hooray/engine/*`
- Query-level tests

**Estimated scope:** M (recursive branch execution plus tests).

---

#### Task 8: Implement dedicated OR proposal boundaries

**Description.** Handle the case where no non-OR pattern can introduce the
missing OR variables. The planner chooses one OR as the proposal boundary, runs
each branch to completion for all required OR output variables, unions distinct
full rows, and validates the result with any remaining OR clauses.

**Acceptance criteria:**
- [ ] OR proposal boundaries introduce all still-unbound variables owned by the
      chosen OR boundary in one outer stage.
- [ ] Branches may still use normal staged WCO internally to prune level by
      level before returning complete branch rows.
- [ ] Multiple top-level OR-only clauses choose one proposal boundary and use
      remaining OR clauses as validators.
- [ ] Later OR output variables cannot be satisfied by a sibling branch after
      an earlier output variable was produced by another branch.
- [ ] No branch id is exposed in public results or outer `BindingSet` layouts.

**Verification:**
- [ ] Planner tests cover OR-only proposal boundaries and multiple OR-only
      clauses.
- [ ] Query tests cover OR-only intersections and the sibling-continuation
      failure shape.
- [ ] `./gradlew test` passes.

**Dependencies:** T7.

**Files likely touched:**
- `src/main/clojure/hooray/query/plan.clj`
- `src/main/kotlin/org/hooray/engine/*`
- Query-level tests

**Estimated scope:** M (planner role selection plus recursive branch executor).

### Checkpoint: OR Complete

- [ ] Issue #14 is fixed in the new engine path.
- [ ] Mixed `or`, OR-only proposal, and multiple OR clause cases are covered.
- [ ] The old `GenericOrPrefixExtender` has not received new focused leakage
      tests or branch-state patches.
- [ ] `./gradlew test` passes.

### Phase 5: Cutover Preparation and Cleanup Boundaries

#### Task 9: Prepare the public `:generic` cutover

**Description.** Once parity and OR coverage are strong enough, route the
public `:generic` query path through the new planner/executor behind a small
switch point. Keep the old scalar implementation available in source for a
follow-up removal or comparison task, rather than deleting it as part of the
cutover.

**Acceptance criteria:**
- [ ] The public `:generic` query path can call the new engine from one narrow
      location.
- [ ] Existing generic query tests pass through the new path.
- [ ] Old scalar generic code remains available but no longer defines the
      active generic semantics.
- [ ] Unsupported or not-yet-ported behavior fails explicitly rather than
      silently falling back to branch-leaking behavior.
- [ ] Debug/introspection output, if present, remains simple planner/executor
      data and does not add a separate subsystem.

**Verification:**
- [ ] Full generic query suite passes.
- [ ] OR regression suite passes.
- [ ] `./gradlew test` passes.
- [ ] Optional: `./gradlew clean test` before review.

**Dependencies:** T6, T7, T8.

**Files likely touched:**
- `src/main/clojure/hooray/query.clj`
- Existing generic query tests
- New BindingSet engine tests

**Estimated scope:** M (small wiring change, broad verification).

## Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Multi-variable proposal materializes large intermediate rows | Medium | Keep v1 simple but explicit. Prefer ordinary non-OR proposers when available. Only let OR propose at a dedicated boundary when no non-OR proposer exists. |
| Partial semijoin validators over-prune or under-prune | High | Add focused planner and engine tests for partially covered validators before OR work depends on them. |
| OR validator mode accidentally recreates scalar branch leakage | High | Validate by running complete branch-existence checks over seeded rows. Cover issue #14 and sibling-branch predicate/function cases. |
| Result shaping diverges from existing `:generic` behavior | High | Keep final `:find` shaping centralized and run existing query-level parity tests before cutover. |
| Clojure/Kotlin interop obscures planner or executor data | Medium | Keep planner output plain and inspectable. Keep runtime contracts small and test with fixtures before full query wiring. |
| Old and new generic paths drift during development | Medium | Keep the old path only as a development fallback/comparison. Make the final public cutover narrow and covered by parity tests. |

## Parallelization Opportunities

- T1 and the initial plain-data planner shape in T5 can proceed in parallel
  after agreeing on variable and stage identifiers.
- T4 pattern executors can be split by pattern kind once T2 and T3 are stable.
- OR query tests for T7 and T8 can be drafted from the spec while the executor
  is being implemented.
- T9 must remain last because it changes the public `:generic` execution path.

## Out of Scope

- Replacing the old scalar generic source code during the first implementation.
- Adding `FactLSM`, trie-backed relations, or a persistent relation store.
- Adding projection/liveness pruning before there is a measured need.
- Adding a full debug tracing subsystem.
- Reworking DBSP incremental query evaluation.
