# Task Breakdown: BindingSet WCO Query Engine

Status: **Phase 3 (Tasks)** - awaiting approval before Implement.
Companion to `specs/bindingset-wco-engine.md` and
`specs/bindingset-wco-engine-plan.md`.

Tasks are dependency-ordered. Each task is intended to fit in one focused
implementation session, end with its Verify step, and keep file churn narrow.
Commit only when explicitly asked.

Path abbreviations:

- `KC/` = `src/main/kotlin/org/hooray/engine/`
- `KT/` = `src/test/kotlin/org/hooray/engine/`
- `CQ/` = `src/main/clojure/hooray/query/`
- `CT/` = `src/test/clojure/hooray/query/`

---

## M1 - Runtime Foundation

- [x] **T1 - `BindingSet` value type**
  - Acceptance: `BindingSet` stores an ordered variable layout and correlated
    rows; rejects or clearly fails on row/layout arity mismatch; exposes lookup
    by variable.
  - Verify: `./gradlew test --tests "*BindingSetTest"` covers construction,
    lookup, and invalid arity.
  - Files: `KC/BindingSet.kt`, `KT/BindingSetTest.kt`.

- [x] **T2 - `BindingSet` row operations**
  - Acceptance: helpers extend rows with introduced variables, reorder to a
    target layout, and deduplicate complete rows. Deduplication is full-row
    based, not scalar based.
  - Verify: `./gradlew test --tests "*BindingSetTest"` covers extension,
    reordering, and full-row distinct.
  - Files: `KC/BindingSet.kt`, `KT/BindingSetTest.kt`.

- [x] **T3 - Stage and executable pattern contracts**
  - Acceptance: define `Stage`, `ExecPattern`, proposer eligibility, count,
    propose, and validate contracts. Validator-only participants are explicit.
  - Verify: `./gradlew compileKotlin compileTestKotlin` succeeds.
  - Files: `KC/Stage.kt`, `KC/ExecPattern.kt`.

## M2 - Stage Executor

- [x] **T4 - Zero-introduce and single-proposer execution**
  - Acceptance: the executor can validate a stage that introduces no variables
    and can run a single proposer over every input row when variables are
    introduced.
  - Verify: `./gradlew test --tests "*StageExecutorTest"` covers validation-only
    and single-proposer stages with test doubles.
  - Files: `KC/StageExecutor.kt`, `KT/StageExecutorTest.kt`.

- [x] **T5 - Multi-proposer count grouping**
  - Acceptance: for each input row, count proposer-eligible participants,
    choose the smallest positive count, drop rows whose best count is zero, and
    partition rows by chosen proposer.
  - Verify: `./gradlew test --tests "*StageExecutorTest"` covers per-row
    proposer selection, zero-count pruning, and deterministic tie handling.
  - Files: `KC/StageExecutor.kt`, `KT/StageExecutorTest.kt`.

- [x] **T6 - Proposal validation and shard merge**
  - Acceptance: each proposer expands only its shard; every non-proposer
    validates expanded rows; shards are concatenated, full-row distincted, and
    reordered to the stage target layout.
  - Verify: `./gradlew test --tests "*StageExecutorTest"` covers validation
    after proposal, shard merge, dedupe, and target layout.
  - Files: `KC/StageExecutor.kt`, `KT/StageExecutorTest.kt`.

## M3 - Non-OR Pattern Executors

- [x] **T7 - Triple executable pattern**
  - Acceptance: triple patterns use existing indexes to count, propose one or
    more variables, fully validate bound rows, and partially validate rows with
    an existential indexed completion check.
  - Verify: focused Kotlin tests cover proposal, full validation, and partial
    semijoin validation.
  - Files: `KC/TriplePattern.kt`, `KT/TriplePatternTest.kt`, existing index
    interop only if needed.

- [x] **T8 - Input binding executable pattern**
  - Acceptance: scalar, tuple, and relation `:in` bindings seed or extend
    `BindingSet` rows while preserving tuple/relation correlation.
  - Verify: focused tests cover scalar input, tuple input, relation input, and
    correlation preservation.
  - Files: `KC/InputPattern.kt`, `KT/InputPatternTest.kt`.

- [x] **T9 - Predicate and function executable patterns**
  - Acceptance: predicates validate only after all arguments are bound;
    functions propose return variables when inputs are bound and validate when
    the return is already bound.
  - Verify: focused tests cover predicate filtering, function proposal, and
    function validation.
  - Files: `KC/PredicatePattern.kt`, `KC/FunctionPattern.kt`,
    `KT/PredicateFunctionPatternTest.kt`.

## M4 - Structured Planner

- [x] **T10 - Planner namespace and plain plan data**
  - Acceptance: add `hooray.query.plan`; convert conformed query clauses into
    plain planner pattern data; flatten `and`; seed bound variables from `:in`.
  - Verify: `./gradlew test --tests "*plan*"` covers namespace entry, flattened
    `and`, and input-bound variables.
  - Files: `CQ/plan.clj`, `CT/plan_test.clj`.

- [x] **T11 - Stage selection for ordinary patterns**
  - Acceptance: planner chooses variable groups that ordinary patterns can
    ground; allows multi-variable stages; predicates are validators; partial
    overlap patterns become existential semijoin validators in the stage that
    introduces overlapping variables.
  - Verify: planner tests cover one-variable triple stages, multi-variable
    triple stages, predicates, functions, and partial validators.
  - Files: `CQ/plan.clj`, `CT/plan_test.clj`.

- [x] **T12 - OR role planning**
  - Acceptance: planner marks `or` as a mixed-stage validator when a non-OR
    pattern can propose the relevant variables, and emits a dedicated OR
    proposal boundary when no non-OR proposer exists. Multiple OR-only clauses
    choose one OR boundary and validate with the remaining ORs.
  - Verify: planner tests cover mixed OR validation, OR-only proposal, multiple
    OR-only clauses, and the no-split multi-output OR boundary rule.
  - Files: `CQ/plan.clj`, `CT/plan_test.clj`.

## M5 - Internal Query Path

- [x] **T13 - Clojure to Kotlin execution adapter**
  - Acceptance: add a narrow internal entry point that takes a conformed query,
    planned stages, database/index state, and input bindings, then invokes the
    Kotlin `BindingSet` executor.
  - Verify: one all-triple query runs through the internal path in a focused
    query test without changing public `h/q` dispatch.
  - Files: `src/main/clojure/hooray/query.clj`, `CQ/plan.clj`,
    `src/test/clojure/hooray/query_test.clj`.

- [x] **T14 - Final result shaping parity**
  - Acceptance: internal BindingSet execution feeds the existing final result
    shaping for `:find`, aggregates, `:keys`, `:strs`, and `:syms` rather than
    duplicating user-facing formatting rules.
  - Verify: parity tests run representative existing generic queries through
    the internal new path and compare results.
  - Files: `src/main/clojure/hooray/query.clj`,
    `src/test/clojure/hooray/query_test.clj`.

## M6 - OR Execution

- [x] **T15 - OR validator executor**
  - Acceptance: mixed-stage OR receives seeded rows, runs branches independently
    as branch-complete existence checks, and keeps a row when at least one
    branch completes for the current row shape. OR does not enter ordinary
    count tables.
  - Verify: query tests cover issue #14, predicates inside branches, functions
    inside branches, and level-by-level validation while ordinary patterns
    introduce variables.
  - Files: `KC/OrPattern.kt`, `KT/OrPatternTest.kt`,
    `src/test/clojure/hooray/query_test.clj`.

- [x] **T16 - Dedicated OR proposal boundary executor**
  - Acceptance: when OR is the only available proposer class, each branch runs
    to completion for all required OR output variables, branch outputs union as
    distinct full rows, and remaining OR clauses validate the unioned rows.
  - Verify: query tests cover OR-only intersections, multiple OR-only clauses,
    and the sibling-continuation failure shape.
  - Files: `KC/OrPattern.kt`, `KT/OrPatternTest.kt`,
    `src/test/clojure/hooray/query_test.clj`.

## M7 - Public Generic Cutover

- [ ] **T17 - Route `:generic` through the new engine**
  - Acceptance: public `:generic` queries call the new planner/executor from
    one narrow switch point; the old scalar generic source remains in the tree
    but no longer defines active `:generic` semantics.
  - Verify: existing generic query tests pass; OR regression tests pass.
  - Files: `src/main/clojure/hooray/query.clj`,
    `src/test/clojure/hooray/query_test.clj`.

- [ ] **T18 - Final verification and unsupported-case audit**
  - Acceptance: unsupported or not-yet-ported behavior fails explicitly rather
    than silently falling back to branch-leaking behavior; lightweight
    planner/executor introspection remains plain data.
  - Verify: `./gradlew test` passes; optional final gate `./gradlew clean test`
    passes before review.
  - Files: likely `src/main/clojure/hooray/query.clj`, `CQ/plan.clj`, targeted
    tests only if the audit finds gaps.

---

## Summary

18 tasks, 7 milestones. Critical path:
`T1 -> T2 -> T3 -> T4 -> T5 -> T6 -> T7 -> T10 -> T11 -> T12 -> T13 -> T15 -> T16 -> T17 -> T18`.

Potential parallel work:

- T10 can start once T3 names the stage and pattern contracts.
- T8 and T9 can proceed after T3 independently of T7.
- OR query tests for T15 and T16 can be drafted once T12 fixes the planned
  shapes.

Do not implement code until the spec, plan, and this task breakdown have been
reviewed and approved.
