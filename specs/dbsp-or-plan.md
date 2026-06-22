# Implementation Plan: DBSP-Or

Companion plan for `specs/dbsp-or.md`.
Status: **Historical** — documents the original PR #12 task plan.

Note: the current `:standard` DBSP planner now uses an explicit relation tree
with `:triple`, `:join`, and `:union` nodes. This task plan is retained as
history for the original OR implementation, not as the current planner shape.

## Overview

Add `or` branch support to the DBSP-standard incremental engine. All code
changes are confined to `src/main/clojure/hooray/dbsp.clj` and
`src/test/clojure/hooray/dbsp_test.clj` — no Kotlin changes, no changes to
`hooray.query` / `hooray.incremental` / `hooray.core`. The existing
`PlusOp` + `DistinctOp` are reused (no new operator primitives). Nested `or`
clauses are handled recursively, not flattened.

## Architecture Decisions (recap from spec)

- **`:or` is a pseudo-pattern at the top level of `:where`** — each entry of
  `:patterns` in the plan is tagged `:kind :triple` or `:kind :or`.
- **Recursive nesting** — `compile-pattern`, `pattern-plan`, and
  `assemble-pattern` all dispatch on `:kind`. A branch of an `:or` descriptor
  may itself be `:kind :or`. No flatten pass.
- **Set semantics via `DistinctOp`** — every `:or` block, including
  single-branch and inner-nested, emits one `DistinctOp` over the union of its
  branches.
- **Left-to-right fold** — branches are unioned in plan/query order via a
  chain of `PlusOp`s for determinism.
- **Flat leaf inputs** — after this change `plan->circuit` returns `:inputs`
  as a flat vector of `InputHandle`s and a parallel `:leaves` vector of
  `{:order …}` records; `push-deltas!` walks the leaves rather than
  `:patterns`.

## Dependency Graph

```
[T1 :kind tag refactor] ──┬──> [T3 compile-pattern :or]
[T2 :leaves refactor]   ──┘         │
                                    v
                          [T4 pattern-plan :or]
                                    │
                                    v
                          [T5 assemble-pattern :or]
                                    │
                          ┌─────────┴─────────┐
                          v                   v
                       [T6 E2E]            [T7 nested
                       flat or             E2E]
```

T1 and T2 are independent silent refactors and can run in either order or
parallel. T3 depends on both. T4 depends on T3. T5 depends on T4. T6 and T7
both depend on T5 and can run in any order (they only add tests). There is
no cross-engine equivalence task: the `:wcoj` engine does not yet support
`or` branches, so there is no `:wcoj` baseline to compare against. Reference
behaviour for tests comes from the static (`hooray.query/query`) engine and
from hand-computed deltas.

## Task List

### Phase 1: Foundation refactor — no behaviour change

#### Task 1: Tag triple descriptors and plans with `:kind :triple`

**Description.** Today every descriptor returned by `compile-pattern` is
implicitly a triple, and every plan node returned by `pattern-plan` is
implicitly a triple plan. Add an explicit `:kind :triple` tag to both shapes
and route downstream call sites through a `:kind` lookup (so adding `:kind
:or` in T3/T4 is a small additive change rather than a restructure).
Behaviour is unchanged; existing tests still pass byte-for-byte.

**Acceptance criteria:**
- [ ] Every triple descriptor returned by `compile-pattern` carries
      `:kind :triple`.
- [ ] Every plan node in `(:patterns plan)` carries `:kind :triple`.
- [ ] `assemble-pattern` dispatches via `case (:kind …)` (with only the
      `:triple` arm in this task) rather than assuming the shape.
- [ ] No change to delta output for any existing query — every existing
      `dbsp_test.clj` deftest passes unchanged.

**Verification:**
- [ ] `./gradlew test --tests "*dbsp*" --tests "*Dbsp*"` passes.
- [ ] `./gradlew build` succeeds.
- [ ] Manual check: `(:kind (first (:patterns (dbsp/plan '{:find [n] :where
      [[?e :name n]]}))))` returns `:triple` at the REPL.

**Dependencies:** None.

**Files likely touched:**
- `src/main/clojure/hooray/dbsp.clj`
- `src/test/clojure/hooray/dbsp_test.clj` (only to update tests that assert on
  full descriptor/plan maps)

**Estimated scope:** S (1-2 files, mechanical tagging).

---

#### Task 2: Flat `:leaves` refactor for `plan->circuit` / `push-deltas!`

**Description.** Today `:inputs` in the value returned by `plan->circuit` is a
vector parallel to `(:patterns plan)`, and `push-deltas!` reads each
pattern's `:order` to build the per-pattern delta z-set. Once `:or` blocks
have multiple leaf triples per top-level pattern, that one-to-one shape
breaks. Refactor now (with only triple patterns in play) so the structure is
correct before `:or` arrives.

After this task:
- `plan->circuit` returns `{:circuit … :inputs [<handle> …] :leaves [{:order
  …} …] :output …}`.
- `:inputs` and `:leaves` are equal-length, in topological leaf order — for a
  query of all-triples (the only kind today) that order is the same as
  `:patterns` order.
- `push-deltas!` iterates `:leaves` and pushes per leaf using the leaf's
  `:order`.

**Acceptance criteria:**
- [ ] `plan->circuit` returns a `:leaves` vector with one entry per leaf
      triple, each carrying `:order`.
- [ ] `push-deltas!` reads only `:leaves`/`:inputs`, never `(:patterns plan)`.
- [ ] No change to delta output for any existing query.

**Verification:**
- [ ] `./gradlew test --tests "*dbsp*" --tests "*Dbsp*"` passes.
- [ ] `./gradlew build` succeeds.
- [ ] Manual check: for the chain query
      `'{:find [?a ?d] :where [[?a :r ?b] [?b :s ?c] [?c :t ?d]]}`, `(count
      (:leaves (plan->circuit (plan q))))` returns 3.

**Dependencies:** None (parallel to T1).

**Files likely touched:**
- `src/main/clojure/hooray/dbsp.clj`
- `src/test/clojure/hooray/dbsp_test.clj` (only assemble/inputs tests that
  read the prior shape)

**Estimated scope:** S (1-2 files, single-namespace refactor).

---

### Checkpoint: Foundation

- [ ] All existing tests pass under `:wcoj` and `:standard`.
- [ ] `./gradlew build` clean.
- [ ] Descriptor/plan tagging and flat-leaves shape are in place, ready for
      the `:or` arm to slot in.

---

### Phase 2: `or` implementation

#### Task 3: `compile-pattern` for `:or`

**Description.** Extend `compile-pattern` to handle the conformed clause
`[:or branches]`. Recursively call `compile-pattern` on each branch, allowing
branches with `:kind :triple` or `:kind :or`. Reject anything else with
`hooray.error/unsupported-ex`. Compute the `:or` descriptor's `:vars` from
the first compiled branch's `:vars` (encounter order).

**Acceptance criteria:**
- [ ] `compile-pattern` returns a descriptor of shape
      `{:index N :kind :or :branches […] :vars [...]}` for `:or` clauses.
- [ ] Each branch descriptor in `:branches` is itself `:kind :triple` or
      `:kind :or` (nesting preserved, not flattened).
- [ ] A branch whose clause-type is `:and`, `:not`, `:predicate`, or `:fn`
      throws `ex-info` (via `err/unsupported-ex`) at compile time, naming the
      offending clause.
- [ ] Unit tests:
  - flat `or` with single-variable branches
  - flat `or` with multi-variable branches in the same encounter order
  - flat `or` with multi-variable branches in different encounter orders
    (e.g. `(or [?p :name n] [v :age ?p])`) — `:vars` follows first branch
  - nested `or` (`(or A (or B C))`) — descriptor tree preserved
  - deeply nested `or` (≥3 levels)
  - rejection of each unsupported branch shape

**Verification:**
- [ ] `./gradlew test --tests "*dbsp*"` passes (new tests included).
- [ ] `./gradlew build` succeeds.

**Dependencies:** T1.

**Files likely touched:**
- `src/main/clojure/hooray/dbsp.clj` (extend `compile-pattern`)
- `src/test/clojure/hooray/dbsp_test.clj` (new `compile-pattern-or-test`)

**Estimated scope:** M (2 files, recursive logic, several test cases).

---

#### Task 4: `pattern-plan` for `:or`

**Description.** Extend `pattern-plan` to dispatch on `:kind`. For a
`:kind :or` descriptor with target `T`, recursively call `pattern-plan` on
each branch descriptor with the same target `T`. Return
`{:kind :or :descriptor … :branch-plans […] :out-vars T}`.

Because each branch (whether triple or nested or) is planned with the same
target, all branches will emit tuples in the same column layout when
assembled. The outer `plan` function's `left-deep-order`, key computation,
and `final-permute` logic require no change — they only read `:vars` and
`:out-vars` from pattern-plan results.

**Acceptance criteria:**
- [ ] `pattern-plan` dispatches on `:kind` and returns `{:kind :or …}` for
      `:or` descriptors.
- [ ] Each entry in `:branch-plans` has `:out-vars` equal to the `:or`
      block's `:out-vars`.
- [ ] Nested `or` branch plans preserve the recursive shape (`:branch-plans`
      can itself contain `:kind :or` plan nodes).
- [ ] Unit tests:
  - flat `or`-only query (no outer triples): branches all `:order :aev`,
    project to `:out-vars`
  - `or` joined with an outer triple: outer chain forces `:out-vars` and each
    branch's `pattern-plan` picks `:aev`/`:ave` accordingly
  - 2-var `or` joined into a 3-pattern chain
  - nested `or` plan: depth and branch counts match descriptor
  - plan determinism — `(= (plan q) (plan q))` for at least one `or`-bearing
    query

**Verification:**
- [ ] `./gradlew test --tests "*dbsp*"` passes (new tests included).
- [ ] `./gradlew build` succeeds.

**Dependencies:** T3.

**Files likely touched:**
- `src/main/clojure/hooray/dbsp.clj` (extend `pattern-plan` and any related
  helper)
- `src/test/clojure/hooray/dbsp_test.clj` (new plan tests for `or`)

**Estimated scope:** M.

---

#### Task 5: `assemble-pattern` for `:or` — Plus chain + Distinct

**Description.** Extend `assemble-pattern` so its `:or` arm:

1. Recursively assembles each branch plan, collecting `{:stream :handles}`
   for each.
2. Folds branch streams left-to-right with chained `PlusOp` (`addBinary`); if
   there is only one branch, the stream is used directly.
3. Feeds the union stream into `DistinctOp` (`addUnary`).
4. Returns `{:stream <distinct-out> :handles (vec (mapcat :handles
   branches))}`.

The `:handles` concatenation is the recursive step — a nested `or` returns
all its inner leaf handles concatenated, in plan order, so `plan->circuit`
sees one flat leaf list. `:leaves` (from T2) is collected in the same
recursive descent and records each leaf's `:order`.

**Acceptance criteria:**
- [ ] Single-branch `or` produces operator sequence `… permute distinct …`
      (no `plus`).
- [ ] Two-branch `or` produces `… permute permute plus distinct …`.
- [ ] k-branch `or` produces `(k - 1)` `plus` operators and exactly one
      `distinct` per `:or` node.
- [ ] Nested `or` (`(or A (or B C))`) produces two `distinct` and two `plus`
      operators total — matching the recursive structure.
- [ ] `:leaves` and `:inputs` lengths equal the total leaf-triple count
      across all top-level patterns (including all nested-or branches).
- [ ] Unit tests cover the operator-name sequences above and the leaf-count
      invariants.

**Verification:**
- [ ] `./gradlew test --tests "*dbsp*"` passes (new tests included).
- [ ] `./gradlew build` succeeds.

**Dependencies:** T2, T4.

**Files likely touched:**
- `src/main/clojure/hooray/dbsp.clj` (extend `assemble-pattern`, possibly
  factor a `assemble-triple` / `assemble-or` pair)
- `src/test/clojure/hooray/dbsp_test.clj` (new circuit-assembly tests)

**Estimated scope:** M.

---

### Checkpoint: Or end-to-end can run

- [ ] `(binding [h/*dbsp-version* :standard] (h/q-inc node '{:find [?e]
      :where [(or [?e :sex :male] [?e :sex :female])]}))` returns an
      `DbspQuery` and `compute-delta!` produces a delta when matching facts
      are transacted.
- [ ] All foundation tests still pass.

---

### Phase 3: End-to-end and equivalence

#### Task 6: End-to-end tests for flat `or`

**Description.** Add `dbsp_test.clj` deftests exercising the public API
(`h/q-inc` under `*dbsp-version* :standard`, `h/transact`,
`h/consume-delta!`) for flat (non-nested) `or` shapes. Covers prime → add →
retract sequences.

**Acceptance criteria:**
- [ ] Single-branch `(or B)` delta matches the bare-`B` delta on the same
      transaction sequence.
- [ ] Two-branch `or` over disjoint conditions returns the union.
- [ ] Two-branch `or` where both branches yield the same tuple produces it
      with weight 1; retracting one underlying fact emits no delta; retracting
      both emits `-1` once.
- [ ] `or` + outer triple join works on adds and retracts.
- [ ] 2-var `or` joined into a 3-pattern chain returns the expected join
      results.
- [ ] `or`-only query (no outer triples) returns the expected union.

**Verification:**
- [ ] `./gradlew test --tests "*dbsp*"` passes.
- [ ] `./gradlew build` succeeds.

**Dependencies:** T5.

**Files likely touched:**
- `src/test/clojure/hooray/dbsp_test.clj`

**Estimated scope:** S.

---

#### Task 7: End-to-end tests for nested `or`

**Description.** Add `dbsp_test.clj` deftests for nested `or` shapes — at
minimum `(or A (or B C))` and one deeper case. Each test compares against
the flat equivalent (e.g. `(or A B C)`), since recursion + idempotent
`Distinct` must produce identical delta multisets.

**Acceptance criteria:**
- [ ] Nested `or` query returns the same delta multiset as its flat-`or`
      equivalent on the same transaction sequence.
- [ ] Nested `or` joined with an outer triple works correctly.
- [ ] A 3-level-deep nested `or` works (sanity check on recursion).

**Verification:**
- [ ] `./gradlew test --tests "*dbsp*"` passes.
- [ ] `./gradlew build` succeeds.

**Dependencies:** T5.

**Files likely touched:**
- `src/test/clojure/hooray/dbsp_test.clj`

**Estimated scope:** S.

---

### Checkpoint: Complete

- [ ] Every spec success criterion (`specs/dbsp-or.md` §"Success Criteria")
      is covered by at least one passing test.
- [ ] `./gradlew test` (full suite) passes.
- [ ] `./gradlew build` clean.
- [ ] No file under `org.hooray.*`, `hooray.query`, `hooray.incremental`, or
      `hooray.core` has been modified.
- [ ] Spec marked Phase 4 (Implement) complete; PR opened linking the spec
      and this plan.

---

## Risks and Mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| Existing test assertions read raw descriptor/plan maps and break when `:kind` is added | M | T1 updates those tests in the same task; "no behaviour change" gate catches regressions. |
| `:leaves` refactor (T2) silently changes input order for triples → wrong deltas | H | Verify with the existing chain/triangle/self-join e2e tests — any reordering will surface as a wrong delta. |
| `DistinctOp` accumulator state interacts oddly with priming via full-DB delta in `compile-query` | M | Test that priming + first incremental tx produces the same delta as running the same tx sequence from scratch on a single-branch `or`. |
| Single-branch nested `or` builds an empty Plus chain → degenerate stream | L | T5 explicitly handles the `k = 1` case by feeding the lone branch straight into `Distinct`. |
| Variable layout ambiguity for `or` with vars not constrained by outer chain | M | Rely on the existing `lead-with` / `indices-of` to be deterministic; assert plan equality across runs in T4. |
| Hidden recursion in `validate-patterns` doesn't actually check nested-or free vars | M | T3 adds a unit test that planning rejects a malformed nested `or` (different free-var sets across branches) — even though the check lives in `hooray.query`, the test pins the contract. |

## Open Questions

None. All review items resolved in `specs/dbsp-or.md`. The plan is ready for
review.

## Parallelization Notes

- T1 and T2 are independent silent refactors and may run in parallel.
- T6 and T7 are test-only and may run in parallel after T5 lands.
- T3 → T4 → T5 is a strict chain.
