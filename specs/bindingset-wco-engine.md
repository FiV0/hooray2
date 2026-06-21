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
- `or` branches execute as isolated seeded subplans and union full binding rows,
  not scalar extensions.

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

`ExecPattern.validate` is a semijoin/filter over rows that already contain the
pattern's required variables.

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
5. Include patterns whose variables become fully covered as validators in that
   stage.
6. Flatten `and` during planning. It is grouping syntax, not a runtime pattern.
7. Treat `or` as a first-class plan pattern. It reports the branch-free
   variables it can produce and compiles to a seeded branch executor.
8. Keep all variables through the stage pipeline in v1. The only required
   projection is the final `:find` extraction.

The last point is intentional. Datatoad's `Salad` aggressively prunes columns
because it is also a recursive intermediate fact store. Hooray's first version
should not add projection machinery until there is a measured memory or
performance reason.

## Stage execution

Stage execution is the local, list-backed equivalent of Datatoad's
`wco_join_inner`.

For each stage:

1. Receive an input `BindingSet`.
2. If the stage introduces no variables, run each participant as `validate`.
3. If the stage has one participant and introduces variables, call that
   participant's `propose`.
4. If the stage has multiple participants and introduces variables:
   - ask every participant for per-row counts;
   - group input rows by the participant with the smallest count for that row;
   - call each participant's `propose` for only its assigned row group;
   - validate the proposed rows with every other participant;
   - concatenate the validated shards;
   - distinct full rows, not individual scalar values.
5. Reorder rows to `targetVariables`.

This preserves the useful WCO primitive: the cheapest available pattern proposes
candidate bindings, and all other relevant patterns validate them. Unlike the
old generic prefix engine, the unit of data is always a binding row.

The count grouping step is explicit. Given participants `P0..Pn`, the executor
builds a logical count table over the input rows:

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
- `validate` checks that the row's triple exists.

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

`or` is a seeded branch executor. It is both a validator and a producer:
already-bound outer variables arrive as seed columns, and variables not yet
bound may be introduced by the branch plans.

For an input `BindingSet`:

1. Determine the seed variables: the variables already present in the input
   `BindingSet`.
2. Determine the OR-introduced variables: free variables of the `or` pattern
   that are not present in the seed.
3. Each branch receives the same seed `BindingSet`.
4. Each branch is planned/executed independently with the seed variables treated
   as already bound and the OR-introduced variables as required output.
5. A branch may validate seeded variables, introduce missing OR variables, or do
   both.
6. Branch predicates and functions validate only that branch's rows.
7. Each branch returns rows covering the requested target layout, normally
   `seed variables + OR-introduced variables`.
8. The `or` executor unions distinct full rows across branches.

No branch id is exposed to users. No scalar proposal from one branch is allowed
to satisfy a predicate from another branch without the rest of the row that made
it valid.

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

The `or` receives seed rows over `[?e ?age]` and introduces `?name`. Each branch
starts from the same seeded rows, extends only its own branch-local rows with
`?name`, validates its own predicates, and emits rows over `[?e ?age ?name]`.
Sibling branches never exchange intermediate rows.

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
  - `or` becomes a seeded branch pattern;
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
- A pattern can introduce one or more variables in a single stage.
- Other patterns validate proposed rows through semijoin-style filtering.
- The implementation has an explicit planner namespace and a separate
  relation-shaped execution core.
- The generic engine no longer depends on scalar `PrefixExtender` as its core
  abstraction.
