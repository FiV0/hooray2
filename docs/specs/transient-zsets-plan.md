# Implementation Plan: Transient ZSet Construction

## Overview

Add Clojure-compatible transient builders for `ZSet` and `IndexedZSet`, then use them for both initial incremental index construction and transaction delta construction. The pipeline boundary remains immutable: transient builders are finalized before creating `ZSetIndices` or stepping the incremental pipeline.

## Architecture Decisions

- `ZSet` and `IndexedZSet` implement `IEditableCollection`; their transient counterparts implement the public transient map interfaces directly.
- `persistent!` freezes the mutable builder state without doing zero-weight cleanup. Zero cleanup happens in the explicit add/retract mutation helpers that maintain Z-set invariants.
- Initial setup and delta construction share one transient construction path so AEV/AVE behavior cannot drift.
- No dependencies or benchmark harness are added.

## Dependency Graph

1. Clojure transient API tests define the Kotlin interop contract.
2. Kotlin transient implementations make `transient`, `assoc!`, `dissoc!`, `conj!`, `get`, `count`, and `persistent!` available.
3. Transient index-construction helpers in `incremental.clj` depend on those interfaces.
4. Existing incremental query setup and delta computation depend on finalized immutable `ZSetIndices`.

## Task List

### Phase 1: Transient Collection Contract

## Task 1: Add ZSet transient API tests

**Description:** Add tests that describe the expected transient map behavior for `ZSet` before implementing it.

**Acceptance criteria:**
- [ ] A transient `ZSet` supports assoc, dissoc, conj, lookup, count, and persistent conversion.
- [ ] Mutating a transient after `persistent!` throws.
- [ ] Persistent `assoc` behavior is unchanged by the transient tests.

**Verification:**
- [ ] Tests fail before implementation: `./gradlew test --tests org.hooray.incremental.ZSetTest`

**Dependencies:** None

**Files likely touched:**
- `src/test/kotlin/org/hooray/incremental/ZSetTest.kt`

**Estimated scope:** S

## Task 2: Implement ZSet transient API

**Description:** Make immutable `ZSet` editable and add a direct public-interface transient map implementation.

**Acceptance criteria:**
- [ ] `(transient zset)` can mutate entries and persist back to `ZSet`.
- [ ] The transient instance rejects all operations after persistence.
- [ ] `persistent!` does not filter zero-weight entries.

**Verification:**
- [ ] Tests pass: `./gradlew test --tests org.hooray.incremental.ZSetTest`
- [ ] Kotlin compile succeeds: `./gradlew compileKotlin compileTestKotlin`

**Dependencies:** Task 1

**Files likely touched:**
- `src/main/kotlin/org/hooray/incremental/ZSet.kt`
- `src/test/kotlin/org/hooray/incremental/ZSetTest.kt`

**Estimated scope:** M

### Checkpoint: ZSet Contract

- [ ] `ZSetTest` passes.
- [ ] Kotlin main and test sources compile.

### Phase 2: Nested Indexed Transients

## Task 3: Add IndexedZSet transient API tests

**Description:** Add tests for transient `IndexedZSet`, including nested child persistence and empty-child handling.

**Acceptance criteria:**
- [ ] A transient `IndexedZSet` supports assoc, dissoc, conj, lookup, count, and persistent conversion.
- [ ] Nested transient children are recursively persisted into immutable Z-set values.
- [ ] Empty immutable child Z-sets are not retained after final construction.

**Verification:**
- [ ] Tests fail before implementation: `./gradlew test --tests org.hooray.incremental.IndexedZSetTest`

**Dependencies:** Task 2

**Files likely touched:**
- `src/test/kotlin/org/hooray/incremental/IndexedZSetTest.kt`

**Estimated scope:** S

## Task 4: Implement IndexedZSet transient API

**Description:** Make immutable `IndexedZSet` editable and add transient map support that can finalize nested transient children.

**Acceptance criteria:**
- [ ] `(transient indexed-zset)` behaves like a transient map for Clojure interop.
- [ ] Persistent conversion returns immutable `IndexedZSet`.
- [ ] Nested transient children are finalized before storage in the immutable result.

**Verification:**
- [ ] Tests pass: `./gradlew test --tests org.hooray.incremental.IndexedZSetTest`
- [ ] Kotlin compile succeeds: `./gradlew compileKotlin compileTestKotlin`

**Dependencies:** Task 3

**Files likely touched:**
- `src/main/kotlin/org/hooray/incremental/IndexedZSet.kt`
- `src/test/kotlin/org/hooray/incremental/IndexedZSetTest.kt`

**Estimated scope:** M

### Checkpoint: Transient Types

- [ ] `ZSetTest` and `IndexedZSetTest` pass together.
- [ ] Kotlin main and test sources compile.

### Phase 3: Incremental Index Construction

## Task 5: Add Clojure regression tests for transient index construction

**Description:** Add behavior tests that pin `db->zset-indices` and `calc-zset-indices` outputs while allowing the implementation to switch from persistent rebuilding to transients.

**Acceptance criteria:**
- [ ] Initial database indexing produces the same AEV/AVE paths as the current behavior.
- [ ] Transaction delta indexing keeps the existing add/retract/overwrite behavior.
- [ ] Zero-weight add/retract results remove leaf entries at mutation time.

**Verification:**
- [ ] Tests fail or expose old construction assumptions before implementation: `./gradlew test --tests 'hooray.incremental_test*'`

**Dependencies:** Task 4

**Files likely touched:**
- `src/test/clojure/hooray/incremental_test.clj`

**Estimated scope:** S

## Task 6: Switch incremental.clj to transient construction

**Description:** Replace the persistent `index-triple` reduction path for initial setup and deltas with explicit transient AEV/AVE mutation helpers.

**Acceptance criteria:**
- [ ] `db->zset-indices` builds AEV and AVE through transients and persists once at the boundary.
- [ ] `calc-zset-indices` uses the same transient path for adds and retracts.
- [ ] Cardinality-one overwrite and cardinality-many duplicate/no-op semantics are preserved.

**Verification:**
- [ ] Clojure tests pass: `./gradlew test --tests 'hooray.incremental_test*'`
- [ ] Clojure compile succeeds: `./gradlew compileClojure compileTestClojure`

**Dependencies:** Task 5

**Files likely touched:**
- `src/main/clojure/hooray/incremental.clj`
- `src/test/clojure/hooray/incremental_test.clj`

**Estimated scope:** M

### Checkpoint: Incremental Behavior

- [ ] Clojure incremental tests pass.
- [ ] Kotlin Z-set transient tests pass.
- [ ] Compile tasks pass for Kotlin and Clojure.

### Phase 4: Full Verification

## Task 7: Run full verification and tighten docs

**Description:** Run the full suite and update the spec if implementation details changed during the work.

**Acceptance criteria:**
- [ ] Full test suite passes or any failures are clearly identified as unrelated.
- [ ] Spec and plan match the implemented behavior.
- [ ] No commits are created unless explicitly requested.

**Verification:**
- [ ] Full suite: `./gradlew test`
- [ ] Git review: `git status --short`

**Dependencies:** Task 6

**Files likely touched:**
- `docs/specs/transient-zsets.md`
- `docs/specs/transient-zsets-plan.md`

**Estimated scope:** S

### Checkpoint: Complete

- [ ] `./gradlew test` has been run.
- [ ] Acceptance criteria from `docs/specs/transient-zsets.md` are satisfied.
- [ ] Remaining risks or failures are reported with exact commands and error summaries.

## Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Kotlin implementation of Clojure transient interfaces misses a method expected by core functions | High | Test through both Kotlin interface calls and Clojure `transient` / `assoc!` / `persistent!` behavior |
| Zero cleanup is implemented in `persistent!` instead of mutation helpers | Medium | Keep tests that persist zero values directly and separate tests for add/retract cleanup |
| Transient children leak into immutable `IndexedZSet` | High | Add recursive persistence tests and type assertions on finalized children |
| Initial setup and delta construction drift | Medium | Share helper functions for both `db->zset-indices` and `calc-zset-indices` |
| Full suite test selector for Clojure tests is imprecise | Low | Fall back to `./gradlew test` and report exact Gradle behavior |

## Parallelization Opportunities

- Task 1 and Task 3 can be drafted independently after the expected transient API is agreed.
- Task 2 and Task 4 should be sequential because `IndexedZSet` nested persistence depends on the `ZSet` transient behavior.
- Task 5 can be prepared while Task 4 is in progress, but Task 6 should wait for both Kotlin transient types.

## Open Questions

None. The previously open spec questions are resolved in `docs/specs/transient-zsets.md`.
