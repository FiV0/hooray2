# Spec: BindingSet WCO query engine

Status: **Phase 1 (Specify)** - awaiting approval before Plan.
Builds on: issue #14, the current `GenericJoin` / `PrefixExtender` engine, and
Datatoad's staged `plan_body` / `wco_join_inner` machinery.

## Objective

Replace the generic scalar-prefix query engine with a relation-shaped engine
that evaluates query stages over a `BindingSet`. The engine should stay close to
Datatoad's staged WCO join model, but without Datatoad's `FactLSM`, `Forest`,
distributed `Comms`, or recursive rule lifecycle.

The central behavior change is:

- A stage introduces **one or more variables** at once.
- One or more executable patterns participate in the stage.
- A proposer expands the current binding rows with the introduced variables.
- The remaining participants semijoin/filter the expanded rows.
- `or` follows the Datatoad-style conservative rule: it does not compete as a
  proposer in mixed outer WCO stages. It validates rows proposed by outer
  patterns, and only proposes in a dedicated OR boundary when it is the only
  pattern that can introduce the missing OR variables.

This intentionally replaces the current `GenericJoin` core. The scalar
`PrefixExtender` API is not expressive enough for branch-local identity because
it can only carry the next scalar extension, not the row correlations that
created that extension.

## Assumptions and scope

- Public query shape stays unchanged: `h/q`, `:find`, `:where`, `:in`, triples,
  `and`, `or`, `not`, predicates, and functions keep their existing surface
  syntax.
- This spec targets the current `:generic` query path first.
- The `:leapfrog` path and DBSP incremental paths are out of scope for the first
  implementation, except that they must keep compiling.
- The new planner lives in an explicit namespace, for example
  `hooray.query.plan`, instead of being hidden inside `hooray.query`.
- The Kotlin runtime should live outside `org.hooray.iterator`, for example
  under `org.hooray.engine`, because it is no longer an iterator/prefix
  extender implementation detail.
- "Semi-naive WCO" in this spec means staged WCO-style evaluation over the
  current `BindingSet`. It does not mean full recursive semi-naive Datalog.
- Use Hooray vocabulary in public/internal docs: pattern, variable, binding,
  stage. Avoid introducing Datatoad's atom/term vocabulary as the user-facing
  model.
- Keep the result representation simple and list-backed in v1. Do not implement
  Datatoad's `FactLSM` or a trie-backed relation store.

## Core model

The new in-flight result representation is a `BindingSet`:

```kotlin
data class BindingSet(
    val variables: List<Any>,
    val rows: List<ResultTuple>
)
```

`variables` defines the column layout for every row. A row's values are
correlated across all variables in the binding set. This is the property the old
scalar prefix API loses when `or` unions branch proposals.

The planner produces stages:

```kotlin
data class Stage(
    val introduces: List<Any>,
    val participants: List<ExecPattern>,
    val targetVariables: List<Any>
)
```

`introduces` is the variable group this stage may add to the binding set.
`participants` are the executable patterns that either propose values for those
variables or validate rows once those variables are present. `targetVariables`
is the row layout after the stage.

The planning and execution interfaces should be split:

```kotlin
interface PlanPattern {
    val variables: Set<Any>
    fun groundable(bound: Set<Any>): Set<Any>
}

interface ExecPattern {
    val variables: Set<Any>

    fun count(input: BindingSet, introduces: List<Any>): List<Int>

    fun propose(
        input: BindingSet,
        introduces: List<Any>,
        targetVariables: List<Any>
    ): BindingSet

    fun validate(
        input: BindingSet,
        targetVariables: List<Any>
    ): BindingSet
}
```

`PlanPattern.groundable` is the Datatoad-inspired planning primitive: given the
currently bound variables, report which additional variables this pattern can
ground.

`ExecPattern.count` is per input row. It is used to select the best proposer for
each row in a multi-participant stage, following the shape of Datatoad's
`wco_join_inner`.

`ExecPattern.propose` expands rows with introduced variables.

`ExecPattern.validate` is a semijoin/filter over rows that contain at least the
variables this stage is constraining. If all pattern variables are present it is
ordinary validation; if only some are present it is an existential semijoin that
keeps rows with at least one completion.

## Planning semantics

The planner starts from the conformed query that `hooray.query` already builds.
It converts each top-level `:where` clause into a `PlanPattern`, then produces a
sequence of stages.

Planning rules:

1. Start with variables supplied by `:in` as the initial bound set.
2. Repeatedly choose a not-yet-bound variable or variable group that at least
   one pattern can ground from the current bound set.
3. Prefer variables that are mentioned by more participating patterns, matching
   Datatoad's most-constrained-first instinct.
4. Allow a stage to introduce multiple variables when one pattern naturally
   produces them together. Example: `[?e :name ?name]` can introduce both `?e`
   and `?name` from an empty binding set.
5. Include patterns that can constrain the newly introduced variables in that
   stage. This includes both fully covered validators and partially covered
   semijoin validators whose variables overlap the newly introduced variables.
6. Flatten `and` during planning. It is grouping syntax, not a runtime pattern.
7. Treat `or` as a first-class plan pattern with two planned roles. In ordinary
   mixed stages, `or` is a validator only: it keeps a row when at least one
   branch has a completion consistent with the row's currently bound OR
   variables.
8. Exclude `or` from count-based proposer selection when any outer non-OR
   pattern can propose the stage variables. This follows the Datatoad instinct:
   an OR/view-like pattern can constrain rows, but it should not act as the
   cheapest proposer in a mixed outer WCO join.
9. If `or` is the only pattern that can introduce the missing OR variables, plan
   a dedicated OR proposal boundary. That boundary proposes all still-unbound
   variables the OR participates in, not one variable at a time.
10. Keep all variables through the stage pipeline in v1. The only required
   projection is the final `:find` extraction.

The last point is intentional. Datatoad's `Salad` aggressively prunes columns
because it is also a recursive intermediate fact store. Hooray's first version
should not add projection machinery until there is a measured memory or
performance reason.

## Stage execution

Stage execution is the local, list-backed equivalent of Datatoad's
`wco_join_inner`.

This section describes ordinary stages and branch-internal stages. An `or` can
participate in an ordinary mixed stage as a validator, but it is not part of the
count-based proposer contest for that stage. If `or` is the only pattern that
can introduce its missing variables, the outer executor enters a dedicated OR
proposal boundary once with the current seed `BindingSet`, lets each branch run
its own internal stages to completion for all unbound OR variables, and resumes
outer execution with the unioned branch rows.

For each stage:

1. Receive an input `BindingSet`.
2. If the stage introduces no variables, run each participant as `validate`.
3. If the stage has one participant and introduces variables, call that
   participant's `propose`.
4. If the stage has multiple participants and introduces variables:
   - ask every proposer-eligible participant for per-row counts;
   - group input rows by the participant with the smallest count for that row;
   - call each participant's `propose` for only its assigned row group;
   - validate the proposed rows with every other participant;
   - concatenate the validated shards;
   - distinct full rows, not individual scalar values.
5. Reorder rows to `targetVariables`.

This preserves the useful WCO primitive: the cheapest available pattern proposes
candidate bindings, and all other relevant patterns validate them. Unlike the
old generic prefix engine, the unit of data is always a binding row.

In mixed stages, OR validators are not proposer-eligible participants. They do
not appear in the count table, and they validate the rows proposed by ordinary
patterns. A stage whose only possible proposer is OR is not an ordinary count
grouping stage; the planner emits the dedicated OR proposal boundary described
below.

The count grouping step is explicit. Given proposer-eligible participants
`P0..Pn`, the executor builds a logical count table over the input rows:

```text
row-index | input row | P0 count | P1 count | ... | chosen proposer
----------+-----------+----------+----------+-----+----------------
0         | [...]     | 12       | 3        | ... | P1
1         | [...]     | 1        | 8        | ... | P0
```

Rows whose smallest count is zero are dropped before proposal; one participating
pattern has proved that the row cannot produce an output for this stage. The
remaining rows are partitioned into proposer groups:

```text
P0 shard = input rows where P0 had the smallest positive count
P1 shard = input rows where P1 had the smallest positive count
...
```

Each proposer expands only its shard. The executor then validates each expanded
shard with every non-proposer participant and merges the validated shards back
into one `BindingSet`. This is the local `BindingSet` version of Datatoad's
"append counts, partition by best atom, propose, semijoin with others" loop.

## Pattern behavior

### Triple patterns

Triple patterns use the existing `eav`, `ave`, and `aev` indexes.

- `groundable(bound)` returns variables that can be produced from the currently
  bound variables and constants.
- `count` estimates the number of matching rows for each input row.
- `propose` extends each input row with one or more triple variables.
- `validate` checks that the row's triple exists when all triple variables are
  present, or that the row has at least one indexed completion when only a
  subset of triple variables is present.

Constants are constraints, not variables.

### Input bindings

`:in` bindings are represented as source patterns or as the initial
`BindingSet`.

- Scalar bindings introduce one variable.
- Tuple and relation bindings introduce multiple variables together.
- Relation bindings must preserve tuple correlation by producing full rows.

### Predicates

Predicates never propose.

They validate once all argument variables are present. Existing arity limits may
remain in v1.

### Functions

Functions can propose their return variable once their input variables are
present. If the return variable is already bound, they validate by recomputing
the function and comparing the result.

### `and`

`and` is flattened into its child patterns. It has no separate runtime
executor.

### `or`

`or` has two planned roles: validator and dedicated proposer. In v1 it never
competes as a normal proposer in a mixed outer WCO stage. This mirrors the
Datatoad approach where view-like patterns are useful as seeded subplans and
validators, but are not treated as cheap index atoms during mixed proposer
selection.

In validator mode:

1. The input `BindingSet` already contains the variables being checked at this
   point in the outer plan.
2. Each branch receives the same input rows as seed rows.
3. For each row, the OR keeps the row if at least one branch has a completion
   consistent with all currently bound OR variables in that row.
4. The OR does not need to remember which branch validated the row earlier,
   because each validation asks the complete branch question again for the
   current row shape.

This mode may run level by level with the outer WCO executor. It is safe because
it is a branch-complete existential semijoin, not a scalar per-level child
filter.

In dedicated proposer mode:

1. Determine the seed variables: the variables already present in the input
   `BindingSet`.
2. Require that no outer non-OR pattern can propose the missing OR variables for
   this stage.
3. Determine the OR output variables: all free variables of the `or` pattern
   that are not present in the seed.
4. Each branch receives the same seed `BindingSet`.
5. Each branch is planned/executed independently with the seed variables treated
   as already bound and the OR output variables as required output.
6. Inside a branch, the normal staged WCO executor can still introduce variables
   level by level and prune early.
7. A branch may validate seeded variables, introduce missing OR variables, or do
   both.
8. Branch predicates and functions validate only that branch's rows.
9. Each branch returns rows covering the requested target layout, normally
   `seed variables + OR output variables`.
10. The `or` executor unions distinct full rows across branches.

No branch id is exposed to users. No scalar proposal from one branch is allowed
to satisfy a predicate from another branch without the rest of the row that made
it valid. The implementation also does not carry hidden branch ids in the outer
`BindingSet`; instead, branch-local identity is preserved either by asking the
complete branch-existence question in validator mode or by completing the branch
plan before a dedicated OR proposal boundary returns.

This directly addresses issue #14. In the motivating query, the outer pattern
`[?e :age ?age]` seeds the `or` with rows shaped like `[?e ?age]`. Branch A
validates its copy of those seed rows with `[?e :name "A"]` and `(< ?age 30)`;
branch B validates its separate copy with `[?e :name "B"]` and `(< ?age 40)`.
Only branch B returns `[b 35]`. Because branches return full rows, branch B's
predicate cannot validate branch A's entity.

If an `or` branch also mentions variables that are not yet bound, the same seed
rule applies. For example:

```clojure
[?e :age ?age]
(or
 (and [?e :name ?name]
      [(= ?name "A")])
 (and [?e :title ?name]))
```

The `or` receives seed rows over `[?e ?age]` and has `?name` as an OR output
variable. Each branch starts from the same seeded rows, extends only its own
branch-local rows with `?name`, validates its own predicates, and emits rows
over `[?e ?age ?name]`. Sibling branches never exchange intermediate rows.

If OR is the only pattern that can propose its variables, it proposes all of
them inside the same OR boundary. For example:

```clojure
(or
 (and [?a :r ?x]
      [?x :p ?c])
 (and [?a :s ?x]
      [?x :q ?c]))
```

If no outer pattern can introduce `?x` or `?c`, the outer executor must not ask
the OR to propose `?x`, union `[?a ?x]` rows back into the top-level
`BindingSet`, and later ask the same OR to propose `?c`. That would lose which
branch produced each `?x`, allowing the second branch to continue a row produced
by the first branch. Instead, the dedicated OR proposal boundary runs each
branch to completion for `[?x ?c]`:

```text
seed [?a]
  branch 1 internally introduces ?x, then ?c
  branch 2 internally introduces ?x, then ?c
union complete [?a ?x ?c] rows
return to outer plan
```

This may materialize a larger result at the OR boundary than a fully interleaved
outer generic join would, but only when OR is the only available proposer. If
another outer pattern introduces `?x` and later `?c`, the OR validates each
stage level by level by checking whether a single branch has a completion for
the current row.

Future work may relax this rule and allow OR to act as a mixed-stage proposer
when it also participates in joins with outer patterns. That requires more
sophisticated join machinery than v1, because the executor would need to avoid
splitting branch-local continuation state across outer stages.

### `not`

`not` is an antijoin validator. It only executes when all variables needed by
the inner pattern are already bound, preserving the existing safety rule.

## Result representation

The engine should return a `BindingSet` internally until final result shaping.

Example:

```kotlin
BindingSet(
    variables = listOf("?e", "?age", "?name"),
    rows = listOf(
        persistentListOf(e1, 35, "A"),
        persistentListOf(e2, 35, "B")
    )
)
```

Final `:find` handling extracts columns from this binding set in find order and
keeps existing aggregate/result behavior.

There is no separate projection data structure in v1. A stage may reorder to a
target layout, but it should not drop variables unless the implementation later
adds a proven-safe liveness analysis.

## Compatibility with the current engine

The old `PrefixExtender` hierarchy may remain temporarily as an adapter or as
legacy code, but it should no longer define the generic engine semantics.

The replacement path should be wired so that `:generic` queries use:

```text
conformed query
  -> hooray.query.plan planned stages
  -> org.hooray.engine BindingSet executor
  -> final find/result shaping
```

Do not fix `or` by adding hidden branch state to `GenericOrPrefixExtender`.
That would preserve the wrong abstraction and continue to make multi-variable
branch correlation awkward.

## Tests

Required coverage:

- Planner tests:
  - triple stages can introduce one variable;
  - triple stages can introduce multiple variables;
  - predicates become validators, not proposers;
  - `and` is flattened;
  - `or` has explicit validator and proposer modes;
  - validating `or` can participate in ordinary staged semijoin;
  - proposing `or` with multiple unbound output variables is planned as one
    boundary, not split across several outer stages;
  - `:in` relation bindings preserve tuple correlation.
- Engine tests:
  - single-participant proposing stage;
  - multi-participant stage with per-row proposer selection;
  - zero-introduce validation stage;
  - distinct full-row union;
  - target variable reordering.
- Query-level tests:
  - existing generic static query parity;
  - issue #14 branch isolation;
  - overlapping `or` branches deduplicate full rows;
  - predicates and functions inside `or` branches validate branch-local rows;
  - validating `or` can safely run level by level when other patterns introduce
    the variables;
  - proposing `or` cannot let later output variables be satisfied by a sibling
    branch after an earlier output variable was produced;
  - `not` remains an antijoin validator once all variables are bound.

Motivating regression:

```clojure
{:find [?name ?age]
 :where [[?e :age ?age]
         (or
          (and [?e :name "A"]
               [(< ?age 30)])
          (and [?e :name "B"]
               [(< ?age 40)]))
         [?e :name ?name]]}
```

With facts:

```clojure
[{:db/id "a" :name "A" :age 35}
 {:db/id "b" :name "B" :age 35}]
```

Expected result:

```clojure
[["B" 35]]
```

Verification commands:

```bash
./gradlew test
./gradlew clean test
```

## Boundaries

Always:

- Keep public query syntax and result shape unchanged.
- Preserve branch-local row correlation for `or`.
- Keep `BindingSet` simple and list-backed in v1.
- Keep unsupported cases explicit.
- Keep unrelated `:leapfrog` and DBSP behavior compiling.

Ask before:

- Making the new engine affect `:leapfrog`.
- Reworking DBSP incremental planning.
- Adding a trie, LSM, or FactLSM-like storage structure.
- Adding cost-based global planning beyond local per-row counts.
- Adding projection/liveness pruning beyond final `:find`.

Never:

- Expose branch ids in query results.
- Fix `or` by weakening predicate validation.
- Preserve scalar `GenericJoin` as the semantic center of the generic engine.
- Commit unrelated local working-tree changes as part of this spec.

## Success criteria

- Issue #14 passes because `or` branches execute in isolation and union complete
  binding rows.
- Existing generic query behavior remains compatible.
- A pattern can introduce one or more variables in a single stage, but a
  proposing `or` boundary is never split across several outer stages.
- Other patterns validate proposed rows through semijoin-style filtering.
- The implementation has an explicit planner namespace and a separate
  relation-shaped execution core.
- The generic engine no longer depends on scalar `PrefixExtender` as its core
  abstraction.
