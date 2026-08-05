# Staged Composite Execution for the PrefixExtender Engine

Version 0.1

Status: Proposed

## Overview

Hooray2 should retain the restored `PrefixExtender` implementations for
ordinary query clauses while moving `or` and `not` out of the
single-variable extender protocol. The existing `GenericJoin` remains the
engine for triples, input relations, predicates, and functions, but it is run
in several prefix ranges separated by explicit composite stages.

The planner for this engine will live in
`src/main/clojure/hooray/plan2.clj`. It starts as a copy of `plan.clj` so that
descriptor construction, groundability, variable ordering, and logical stage
planning retain the semantics already used by the current `:generic` engine.
The two planners diverge only at runtime lowering:

- `plan.clj` lowers descriptors to `ExecPattern` and `IStage` values for
  `GenericJoinEngine`;
- `plan2.clj` lowers the same descriptors and logical stages to ordinary
  `PrefixExtender`s plus recursive OR and NOT stages for `GenericJoin`.

The intended query path is:

```text
validated, conformed query
          |
          v
plan2.clj: clauses and inputs -> descriptors
          |
          v
plan2.clj: descriptors -> logical stages
          |
          v
plan2.clj: logical stages -> compiled prefix scope
          |
          v
BindingSet between stage boundaries
List<Prefix> inside each GenericJoin range
          |
          v
existing projection and aggregation in query.clj
```

This is a separate implementation experiment. It does not replace or modify
the current `plan.clj`/`GenericJoinEngine` path while it is being developed.

## Current State

Hooray2 currently has two standard engines:

- `:generic` uses `plan.clj`, `ExecPattern`, `IStage`, `BindingSet`, and
  `GenericJoinEngine`.
- `:generic-old` compiles the complete query directly into positional
  `PrefixExtender`s and executes one `GenericJoin` over the complete variable
  order.

The ordinary extenders in the restored engine are a useful execution model:

- `GenericPrefixExtender` reads storage indexes;
- `GenericRelationPrefixExtender` represents `:in` relations and other
  materialized relations;
- `GenericPredicatePrefixExtender` filters at its last variable level;
- `GenericFnPrefixExtender` derives a function result after its inputs are
  bound.

The problem is not those leaf contracts. The problem is representing a whole
conjunction inside `GenericOrPrefixExtender` or `GenericNotPrefixExtender`.
Those classes are asked to express multi-clause, recursively nested behavior
through `count`, `propose`, and `intersect` for one variable at a time.

## Problem

### OR branch identity

An OR branch is a conjunction. Its constraints must finish as a unit before
its rows are unioned with rows from another branch. A variable-at-a-time union
can lose the relationship between an earlier triple and a later predicate.
For example:

```clojure
{:find [?name ?age]
 :where [[?e :age ?age]
         (or (and [?e :name "A"]
                  [(< ?age 30)])
             (and [?e :name "B"]
                  [(< ?age 40)]))
         [?e :name ?name]]}
```

The `< 40` predicate belongs only to the `B` branch. It must not validate a
prefix produced by the `A` branch.

An OR may also need to introduce more than one missing variable as one
logical operation. `PrefixExtender.propose` returns one column, so an OR that
produces a relation cannot be faithfully represented as an ordinary leaf
without materializing the completed branch rows first.

### NOT is a correlated subquery

NOT does not ground variables. It receives bindings from the outer scope,
executes its body against the distinct relevant bindings, and removes matching
outer rows. Keeping NOT inside a prefix extender obscures this correlation
boundary and makes an OR nested below NOT fall back to the same branch-identity
problem.

### Relation layouts are named at boundaries but positional in GenericJoin

`GenericJoin` and its extenders address variables by integer levels.
`BindingSet` addresses columns by symbols and supports projection and reorder.
The staged engine needs both:

- named `BindingSet` layouts at scope and stage boundaries;
- positional prefixes inside one `GenericJoin` range.

In particular, `GenericRelationPrefixExtender` requires its participating
levels to be sorted and assumes that relation columns follow that level order.
External relation bindings, incoming correlated bindings, and materialized OR
relations therefore have to be reordered before they become extenders. The
extender itself does not need a more general layout contract.

## Goals

- Keep ordinary `PrefixExtender` implementations close to their current
  contracts and algorithms.
- Reuse the descriptor construction, groundability rules, and logical planning
  behavior from `plan.clj`.
- Let `GenericJoin` resume from existing prefixes and stop at a composite
  boundary.
- Execute every OR branch as an isolated nested conjunction before union.
- Let one OR stage introduce several variables.
- Execute NOT as a correlated antijoin, including when its body contains OR.
- Preserve named layouts and row multiplicity across stage boundaries.
- Normalize materialized relation layouts before constructing positional
  relation extenders.
- Keep `:generic` unchanged while the new `:generic-old` path is built and
  compared against it.
- Make each implementation phase independently testable.

## Non-goals

- Replacing `GenericPrefixExtender`, `GenericPredicatePrefixExtender`, or
  `GenericFnPrefixExtender` with `ExecPattern` implementations.
- Reusing `GenericJoinEngine` for the new path.
- Changing query validation, query variable order, `:find` projection, or
  aggregation semantics.
- Adding cost-based stage ordering.
- Changing the incremental engines.
- Changing the `Join<T>` interface; seeded execution belongs specifically to
  `GenericJoin`.
- Generalizing `PrefixExtender` to propose several columns.
- Sharing planner helpers between `plan.clj` and `plan2.clj` before both
  designs are stable.
- Removing `GenericAndPrefixExtender` as part of this design. `and` branches
  are flattened by descriptor construction, but deleting any now-unused class
  should be a separate usage audit.
- Optimizing composite materialization before correctness and parity are
  established.

## Decision

### Keep the first two planning phases

`plan2.clj` copies the following concepts from `plan.clj`:

1. conformed clauses and `:in` arguments become recursive descriptors;
2. descriptor groundability is evaluated against a bound-variable set;
3. `plan-scope` produces logical stages containing `:added`, `:proposers`,
   `:participants`, and `:target-variables`;
4. nested scopes put relevant incoming variables first and otherwise preserve
   query variable order.

The copied planner deliberately remains separate rather than extracting a
shared namespace immediately. That keeps changes to the experimental prefix
engine from changing the production `:generic` planner by accident. Once both
paths are stable, identical helpers can be compared and extracted in a
separate refactor.

The existing groundability rules remain:

| Descriptor | Groundability |
|------------|---------------|
| Triple | Every unbound entity/value variable in descriptor order. |
| Input relation | The next variable after its bound relation prefix. |
| Function | Its output after all variable arguments are bound. |
| Predicate | Nothing; all referenced variables must already be bound. |
| OR | All missing OR variables only when every branch can derive the complete missing set. |
| NOT | Nothing; all body variables come from the outer scope. |

OR groundability remains a fixed point per branch, including nested
composites. NOT remains validation-only.

### Replace runtime lowering

The current final phase of `plan.clj` creates `ExecPattern` values and
`IStage` records. `plan2.clj` replaces that phase with a compiled scope made of
ordinary extenders and recursive stage data.

The following maps are illustrative; their exact record names are not part of
the public API:

```clojure
{:input-variables [?e]
 :variable-order [?e ?name ?age]
 :extenders [triple-extender predicate-extender]
 :stages [{:kind :generic
           :target-variables [?e ?name]}
          {:kind :or
           :variables [?e ?age]
           :added [?age]
           :target-variables [?e ?name ?age]
           :branches [compiled-branch-1 compiled-branch-2]}
          {:kind :not
           :variables [?e ?age]
           :target-variables [?e ?name ?age]
           :body compiled-body}]}
```

The ownership boundary is:

- a compiled scope owns every ordinary leaf extender in one conjunction;
- a generic stage runs a consecutive range of prefix levels;
- an OR stage owns one compiled scope per branch;
- a NOT stage owns one compiled body scope;
- the preceding target layout, or the unit layout for the first stage, is the
  physical input of a runtime stage;
- `input-variables` describes the correlated relation supplied to a nested
  scope, not a pre-populated prefix that skips its early levels.

### Lower logical stages into composite barriers

`plan-scope` can remain unchanged even though OR and NOT are no longer
participants in `GenericJoin`. One logical stage may lower to more than one
runtime stage:

| Logical stage | Prefix-engine lowering |
|---------------|------------------------|
| Ordinary proposer, ordinary validators | One generic stage. |
| Ordinary proposer plus fully bound OR/NOT validators | One generic stage, followed by one composite stage per OR/NOT in descriptor order. |
| OR as sole proposer | One proposing OR stage. |
| Validation-only OR/NOT participants | One composite stage per participant in descriptor order. |
| Leaf-only validation immediately after a proposing OR | Absorbed into that OR stage's correlation `GenericJoin`. |

Lowering tracks the physical layout before and after every logical stage. A
proposing OR compiles each branch against the variables available before that
stage. A validating OR or NOT that follows an ordinary proposal compiles
against the generic stage's target layout. This preserves the current
descriptor-to-`ExecPattern` rule: a proposer receives the old bound variables,
while a validator receives the variables made available by that proposal.

Ordinary extenders are scope-wide and select their own participating levels.
The runtime generic stage therefore does not need the logical participant list
to call them individually. The participant list is still useful during
planning and for identifying composite barriers during lowering.

The leaf-only validation case exists because a grouped OR proposal can add
several variables at once. `plan-scope` emits a following logical validation
stage for predicates or other leaves that became fully bound. Runtime OR
execution already runs the materialized OR relation through `GenericJoin`
together with every scope leaf extender over each newly added level. Those
leaves have therefore already validated at their normal last participating
level. The lowerer may absorb that logical bookkeeping stage. A leaf-only
zero-added stage in any other situation is a lowering error rather than a
silent no-op.

Adjacent generic runtime stages are coalesced by retaining the final target
layout. A composite stage is always a barrier, including a validation-only
composite that changes no variables.

### Compile ordinary descriptors to existing extenders

Leaf compilation moves from the `:generic-old` branches in `query.clj` into
`plan2.clj`:

- a triple descriptor chooses EAV, AEV, or AVE exactly as the restored
  compiler does and creates `GenericPrefixExtender`;
- a predicate descriptor creates `GenericPredicatePrefixExtender` using
  scope-relative levels and the existing argument-order adapter;
- a function descriptor creates `GenericFnPrefixExtender` after planning has
  placed every input before its output;
- a relation descriptor creates `GenericRelationPrefixExtender` after layout
  normalization;
- OR and NOT descriptors compile recursively to stages, never to extenders.

All levels are computed against the compiled scope's `variable-order`, not the
root query order. This is required because a nested scope moves its correlated
input variables to the front.

### Normalize relations before constructing positional extenders

Every relation enters the prefix engine through the same normalization rule:

1. determine the relation variables in scope variable order;
2. reorder the `BindingSet` to those variables;
3. map those variables to scope levels;
4. assert that the resulting levels are sorted and unique;
5. construct `GenericRelationPrefixExtender` with the reordered rows.

This applies to:

- scalar, collection, tuple, and relation `:in` descriptors;
- the dynamic incoming relation of an OR branch or NOT body;
- a materialized OR result re-entering its outer `GenericJoin`.

The current `GenericRelationPrefixExtender` can then keep its sorted-level and
trie-prefix assumptions. Conflicting declaration orders are handled once at
the named-layout boundary rather than inside every `count`, `propose`, and
`intersect` call.

An empty correlated-variable list does not need a relation extender. The
nested scope starts from the unit relation and executes once.

## GenericJoin Changes

`GenericJoin` remains unaware of OR, NOT, symbols, and `BindingSet`. It gains a
seeded entry point that runs its existing level loop from the arity of an
already materialized prefix relation to the join's configured target level.

An illustrative Kotlin signature is:

```kotlin
fun joinFrom(
    prefixes: List<Prefix>,
    startLevel: Int,
): List<ResultTuple>
```

The constructor's `levels` value is the exclusive target level. The method
runs levels in `startLevel until levels`. A stage that starts with two bound
variables and targets five variables constructs `GenericJoin(extenders, 5)`
and calls `joinFrom(rows, 2)`.

The contract is:

- every input prefix has arity `startLevel`;
- `startLevel` is between zero and the configured target level;
- levels below `startLevel` are not executed again;
- input row order and duplicates are retained unless extenders filter them;
- empty input returns empty output without inferring layout from a row;
- every executed level has at least one participating proposer;
- the existing `join()` delegates to `joinFrom` with one empty prefix and
  start level zero.

`Join<T>` stays unchanged. `LeapfrogJoin`, `CombiJoin`, and unrelated join
implementations do not acquire a seeded contract.

The staged executor constructs a `GenericJoin` per generic range. For a normal
range it supplies the scope's leaf extenders. For a proposing OR range it adds
one temporary `GenericRelationPrefixExtender` containing the completed OR
relation. This keeps candidate selection and intersection in the existing
generic join loop.

## Execution

### Scope execution

The root scope starts from `BindingSet([], [[]])`. A nested scope receives a
`BindingSet` whose variable set equals `input-variables`.

The nested input is not passed directly to `joinFrom` at its existing arity.
Doing that would skip leaf extenders that must validate correlated variables
at earlier levels. Instead, the executor:

1. projects and reorders the input to `input-variables`;
2. creates a dynamic relation extender for those leading scope levels;
3. adds it to the scope's static leaf extenders;
4. starts the nested scope from the unit relation at level zero.

The input relation and the branch's storage/predicate/function extenders then
jointly propose and validate the correlated prefix through the ordinary
`GenericJoin` loop.

Runtime stages fold over a current `BindingSet`:

- a generic stage resumes `GenericJoin` from the current row arity and wraps
  its rows in the stage target layout;
- a proposing OR materializes its branches and uses that relation as an
  additional extender while extending the original outer rows;
- a validating OR semijoins the outer rows with its materialized relation;
- NOT antijoins body matches from the outer rows.

If the current binding set is empty, no storage or nested branch work is
required. The executor still returns an empty `BindingSet` with the planned
target layout so later stages do not lose schema information.

### Generic stages

A generic stage has an input layout equal to the preceding stage target and a
target layout that is a longer prefix of the scope variable order. It runs all
scope leaf extenders over only the newly added levels.

Using all leaf extenders is intentional. Each extender's
`participatesInLevel` method determines where it proposes or validates, which
is the behavior of the restored one-shot engine. Staging changes when prefix
ranges run; it does not introduce a second participant-selection mechanism for
ordinary leaves.

### OR stages

For an OR stage with current outer input:

1. Compute the correlated variables as the OR variables already present in
   the input, preserving outer scope order.
2. Project the input to those variables and apply `distinctRows`.
3. Execute every branch scope independently with that projected relation as
   its incoming binding set.
4. Reorder each completed branch result to one common OR layout.
5. Distinct-union the complete branch relations.
6. If the OR adds no variables, semijoin the original outer input with the OR
   relation.
7. Otherwise, reorder the OR relation into outer scope order and wrap it in a
   `GenericRelationPrefixExtender` at the corresponding levels.
8. Run `GenericJoin` from the original outer rows to the OR target with the
   scope leaf extenders plus the temporary OR relation extender.

```text
outer BindingSet
       |
       +-- project correlated variables -- distinct -- branch 1 scope --+
       |                                                               |
       +-- project correlated variables -- distinct -- branch 2 scope --+-- distinct union
                                                                       |
                                        semijoin if validating <--------+
                                                                       |
                         relation extender + GenericJoin if proposing <-+
```

Branches are fully evaluated before union, so a predicate cannot migrate from
one branch to another. Reordering before union handles branches that introduce
the same variable set in different orders.

The relation extender sees the complete outer prefix when it proposes an OR
variable. It therefore checks all already-bound correlated columns through
its trie before exposing the next missing column. Unrelated outer columns stay
in the seeded prefixes and never enter the branch input.

The materialized OR relation has set semantics. Duplicate witnesses and
duplicate rows from overlapping branches produce one OR tuple. The outer
input retains bag semantics: duplicate outer rows remain duplicate after a
matching OR.

### NOT stages

NOT is always validation-only:

1. Project the outer input to the NOT variables and apply `distinctRows`.
2. Execute the compiled body scope with that relation as incoming input.
3. Project and distinct the matching body rows to the NOT variables.
4. Antijoin those matches from the original outer input.

The original layout is preserved. Unrelated columns and duplicate non-matching
outer rows remain intact. Because the body is a compiled scope, OR below NOT
uses the same isolated branch execution and never creates
`GenericOrPrefixExtender`.

## Layout, Multiplicity, and Empty-Input Semantics

- Every scope input and stage target contains distinct variables.
- Every generic target is a prefix of its scope variable order.
- A proposing OR adds exactly its missing variables and reaches its logical
  target layout.
- A validating OR and every NOT stage preserve the input layout.
- Branch results are reordered before union.
- Materialized input and OR relations are reordered before their scope levels
  are computed.
- Generic stages preserve prefix row multiplicity.
- OR branch relations use distinct union.
- OR semijoin and NOT antijoin preserve the multiplicity of retained outer
  rows.
- An empty result retains its planned `BindingSet.variables` even though no
  row exists from which to infer arity.

## Planner and Runtime Error Boundaries

- Existing query validation remains the first boundary for malformed queries,
  unequal OR variable sets, and unbound NOT variables.
- `plan2.clj` reports an insufficient-binding query error when logical planning
  cannot ground the next variable.
- Runtime lowering rejects a composite used in an unsupported participant or
  proposer shape.
- Runtime lowering rejects a leaf-only zero-added logical stage unless it is
  the validated absorption case after a proposing OR.
- Relation normalization rejects missing variables, duplicate variables, row
  arity mismatches, and non-prefix stage layouts before constructing an
  extender.
- `GenericJoin.joinFrom` rejects inconsistent prefix arity and invalid level
  ranges.
- Storage and user function failures retain their current behavior; this
  design does not introduce another exception abstraction.

## Query Integration

During development, `:generic` remains on `plan.clj` and
`GenericJoinEngine`. The restored one-shot `:generic-old` path remains
available until the staged prefix engine is complete.

At cutover, the `:generic-old` arm in `query.clj` becomes conceptually:

```clojure
(plan2/execute (plan2/plan db conformed-query args vars-in-join-order))
```

`plan2/execute` returns rows in the query variable order expected by the
existing `compile-find` path. Projection, aggregates, `:keys`, `:strs`, and
`:syms` remain unchanged.

The direct recursive `compile-pattern` path stays in place until this cutover
so each phase can be compared with the restored engine. It is removed from the
`:generic-old` path only after cross-engine query coverage passes.

## Alternatives Considered

### Keep evolving GenericOrPrefixExtender

A branch-aware trie can remember which branch produced an earlier prefix, but
later variable-level calls then carry composite execution state. Nested OR and
OR below NOT compound that state. Completing each branch before union is a
clearer boundary.

### Keep GenericNotPrefixExtender and remove only OR

This would leave nested scopes with two execution models. An OR below NOT
would still need special handling inside the NOT extender, and NOT correlation
would remain hidden in a one-column intersection. A recursive NOT stage uses
the same scope contract as OR branches.

### Reuse ExecPattern and GenericJoinEngine

That is the current `:generic` design and remains available as the reference
implementation. The purpose of this proposal is to test whether composite
staging can solve the same problems while retaining the simpler storage-backed
prefix extenders.

### Make PrefixExtender return multi-column relations

Changing `propose` and `intersect` to carry variable layouts or multi-column
rows would affect every leaf extender and move `BindingSet` concerns into the
hot prefix loop. Explicit stage boundaries keep the leaf interface small.

### Let GenericJoin own OR and NOT stages

`GenericJoin` has no variable symbols or relational operations. Teaching it
about recursive scopes, projection, union, semijoin, and antijoin would mix the
prefix kernel with the planner/runtime coordinator. The proposed change keeps
`GenericJoin` responsible for a seeded range of ordinary levels while
`plan2.clj` folds composite stages around it.

### Extract shared planning helpers immediately

Sharing descriptor and logical-planning code would reduce duplication, but it
would also couple the experiment to the current production planner. Copying
first gives both designs a stable comparison point. Deduplication is a later
refactor based on demonstrated identical behavior.

## Incremental Implementation Plan

Each phase should be separately reviewable. No phase changes the public
`:generic` engine.

### Phase 1: Seeded GenericJoin

Add `joinFrom` to concrete `GenericJoin`; keep query compilation unchanged.

Acceptance criteria:

- `join()` delegates to `joinFrom` and retains all existing results.
- Non-empty prefixes resume at an explicit start level.
- Levels below the start level do not execute again.
- Duplicate input prefixes remain duplicate.
- Empty input returns an empty result without losing the explicit target
  level.
- Invalid prefix arity and level ranges fail clearly.
- `Join<T>` and other join engines are unchanged.

Focused verification:

```bash
./gradlew test --tests 'org.hooray.algo.GenericJoinTest'
```

### Phase 2: Copy planning into plan2.clj

Copy descriptor construction and `plan-scope` from `plan.clj`, with a separate
`hooray.plan2` namespace and tests. Do not add runtime extenders yet.

Acceptance criteria:

- Equivalent descriptors and logical stages match `plan.clj` for the same
  conformed query.
- Incoming variables are trimmed and moved to the front in outer order.
- Functions ground outputs only after their arguments.
- OR groundability remains all-or-nothing per branch.
- NOT remains validation-only.
- A logical stage with an ordinary proposer and a composite validator lowers
  to a generic stage followed by the composite stage.
- A proposing OR followed by leaf-only validation lowers to one OR stage with
  the leaf validation absorbed into its correlation join.
- Proposing branches receive the pre-stage layout; validating bodies receive
  the post-generic layout.
- No implementation helper is extracted back into `plan.clj` in this phase.

Focused verification:

```bash
./gradlew test --tests 'hooray.plan2_test__init'
```

### Phase 3: Ordinary lowering and generic-stage execution

Compile triple, relation, predicate, and function descriptors to their current
extenders. Lower and execute generic ranges with `BindingSet` boundaries.

Acceptance criteria:

- Existing non-composite `:generic-old` queries return the same rows.
- Nested incoming bindings are replayed from level zero through a relation
  extender.
- External and incoming relations are normalized to scope order.
- The three conflicting relation-order query cases can be enabled and pass.
- Predicate argument order and function dependencies remain correct.
- Consecutive generic stages preserve layouts and multiplicity.
- Empty generic results retain the planned target layout.

Focused verification:

```bash
./gradlew test --tests 'org.hooray.iterator.*'
./gradlew test --tests 'hooray.plan2_test__init'
```

### Phase 4: OR stages

Add isolated branch execution, distinct union, validation semijoin, and the
temporary relation extender used for proposing OR stages.

Acceptance criteria:

- Branch predicates remain attached to the branch that produced the row.
- Predicate-only OR filters fully bound values.
- OR can introduce several variables in one stage.
- Branches may produce their common variable set in different orders.
- Nested OR groundability remains all-or-nothing.
- Duplicate branch witnesses collapse while duplicate outer rows remain.
- Unrelated outer columns are preserved.
- Leaf validation after a grouped OR proposal is absorbed into the OR
  correlation join exactly once.

The current public regressions include:

- `test-or-and-branch-predicates-stay-bound-to-same-prefix`;
- `test-or-branches-can-introduce-variables-in-different-orders`;
- `test-nested-or-groundability-is-all-or-nothing`.

Focused verification:

```bash
./gradlew test --tests 'hooray.query_test__init'
```

### Phase 5: NOT stages

Add correlated body execution and antijoin. Stop compiling NOT to
`GenericNotPrefixExtender` in the staged path.

Acceptance criteria:

- Existing NOT query behavior remains unchanged.
- Function results can bind variables used by NOT.
- OR inside NOT remains branch-isolated.
- NOT inside an OR branch sees only that branch's incoming relation.
- Duplicate outer rows are retained or removed without multiplication.
- Empty body matches preserve the input unchanged.

Focused verification:

```bash
./gradlew test --tests 'hooray.query_test__init'
```

### Phase 6: Cut over :generic-old

Route `:generic-old` through `plan2/plan` and `plan2/execute`. Keep `:generic`
as the independent reference engine and continue using
`with-each-query-engine` to run the public query suite against both.

Acceptance criteria:

- All enabled `query_test.clj` cases pass for both engines.
- The three conflicting relation-order regressions are enabled and pass.
- Restored extender unit tests remain green.
- Projection and aggregation results are unchanged.
- `query.clj` no longer constructs composite prefix extenders for
  `:generic-old`.

Full verification:

```bash
./gradlew test
git diff --check
```

### Phase 7: Cleanup

After cutover and parity verification:

- remove `GenericOrPrefixExtender` and its direct tests;
- remove `GenericNotPrefixExtender` and its direct tests;
- remove their imports and compilation branches from `query.clj`;
- remove the obsolete one-shot `:generic-old` composite compilation path;
- retain ordinary extenders and `GenericJoin` tests;
- audit `GenericAndPrefixExtender` separately rather than deleting it as an
  implicit consequence.

Do not remove the old composite path before cutover. Keeping it available
through the earlier phases makes behavior comparisons and rollback simple.

## Consequences and Risks

- `plan.clj` and `plan2.clj` intentionally duplicate descriptor and logical
  planning code during the experiment. Changes to shared query semantics must
  be applied consciously to both until a later extraction.
- OR and NOT allocate intermediate `BindingSet` relations at explicit
  boundaries.
- Distinct correlated inputs prevent duplicate outer rows from repeating
  nested work, but different correlated keys can still repeat storage scans.
- Constructing a `GenericJoin` per generic range recomputes level participant
  sets. This is acceptable for correctness-first implementation and should be
  measured before optimization.
- Replaying nested inputs from level zero is necessary for correct validation
  of correlated variables, but it can do more work than resuming directly from
  their arity.
- Relation normalization allocates reordered rows at materialization
  boundaries. It keeps the hot extender contract simple and directly resolves
  conflicting column-order cases.
- The existing `:generic` engine remains an independent semantic oracle, which
  reduces migration risk but means two standard implementations continue to
  exist.

Performance work follows correctness and must compare equivalent queries and
data. Plausible later optimizations include caching branch results by distinct
correlated key, reusing level-participant tables between stages, and avoiding
relation reorders when a `BindingSet` is already in scope order.
