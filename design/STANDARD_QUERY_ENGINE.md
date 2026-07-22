# Standard Query Engine

## Overview

The standard query engine evaluates a conformed `:where` clause over ordered
`BindingSet` values. Compilation turns clauses into patterns, planning arranges
those patterns into stages, and `GenericJoinEngine` executes the stages.
In planning we are mainly concerned with variables, the order they are introduced in
and how many are introduced at the same time. The execution is concerned
with tuples and how they get built up.

A query roughly goes through the following stages:
```text
query -> conformed clauses -> Clojure planning patterns -> Stage lists
      -> GenericJoinEngine -> result BindingSet -> :find projection
```

A top-level `:where`, an `and` body, an `or` branch, and a `not` body are all
planning scopes. Conjunction is represented by the stages of a scope rather
than by an executable `AndPattern`.

## Binding sets and stages

A `BindingSet` contains an ordered variable list and rows of the same arity.
Column order is part of its contract. The unit binding set has no variables and one
empty row; it is the input for a query without external bindings and for nested
plans before their outer bindings are injected.

A `Stage` describes one layout transition:

- `added` lists variables absent from the input and introduced by the stage.
- `participants` lists the patterns that constrain the stage output.
- `targetVariables` is the exact output layout. Its variable set equals the
  input variables plus `added`.

An empty `added` list makes the stage validation-only. Participant indexes are
distinct within a stage, and all variable lists contain no duplicates.

## Pattern contracts

There are 4 concepts in the planning pipeline that appear throughout.
`descriptors` are just clauses (`triple`, `or-pattern` etc..) that supplemented
with additional data to make planning easier. The `:where` query AST is pretty
much preserved. A logical stage map is a pure Clojure planning representation of what
will later become an `IStage`. `ExecPattern` is the executor contract
of one pattern (which can have sub-plans) in the `GenericJoinEngine`.

### Descriptor maps

Every descriptor contains `:idx`, `:kind`, ordered `:variables`, and a
`:groundable` function. Clause descriptors also retain `:clause`; an `or`
descriptor contains a vector of descriptor vectors in `:branches`, a `not`
descriptor contains its child descriptors in `:children`, and an input
relation descriptor contains its `:binding-set`.

For example, a triple descriptor has this shape:

```clojure
{:kind :triple
 :idx 1
 :variables [?e ?name]
 :groundable groundable-fn
 :clause {:e [:variable ?e]
          :a [:constant :name]
          :v [:variable ?name]}}
```

Calling `(groundable bound-vars)` returns only variables the descriptor can introduce from
the supplied bound-variable set. An `or` is groundable only when every branch can reach all of its missing
variables.

### Logical stage maps

A logical stage is still pure Clojure data. `:proposers` identifies the
participants that are allowed to introduce `:added`; `:participants` also
includes descriptors that can validate the resulting layout. This distinction
between `:proposers` and `:participants` is retained explicitly because
recursive stage construction will need it a later step.

A logical stage look as follows:

```clojure
{:added [?name]
 :proposers [1]
 :participants [1 2]
 :target-variables [?e ?name]}
```

`?name` gets added in this stage. The exec-pattern with index 1 acts as proposer and both
1 and 2 participate in this stage. It essentially means 1 proposes and 2 validates.
The final variable layout after this stage should be `target-variables`.

### `ExecPattern`

`ExecPattern` is the executor contract for a runtime pattern. It exposes a
stable `idx`, its variable set, proposal counts, and proposing or validation of
a `BindingSet` through `join`. With multiple participants, `GenericJoinEngine` uses
`ExecPattern.count` to choose the cheapest proposer independently for each
input row.

`ExecPattern.count` updates one proposal per input row when the pattern has a
strictly cheaper positive candidate count. A participant that cannot propose
the stage's `added` list leaves the proposals unchanged. `ExecPattern.join`
has two modes:

- A non-empty `added` list asks the pattern to extend the input and return
  exactly `targetVariables`.
- An empty `added` list asks the pattern to filter rows without changing the
  input layout.

### `IStage`

`IStage` is the executor contract for one binding-layout transition.

```clojure
{:added [?name]
 :participants [triple-pattern predicate-pattern]
 :target-variables [?e ?name]}
```

## Planning and Stage construction

The planner is working on one conjunctive query scope at a time.
A scope is either the top-level scope, one branch of an `or` or a `not` scope.
Inner scopes are planned when the particular pattern (`or` or `not`) are required
in the outer scope for the first time. For `or` and `not`, the planner
might introduce an incoming bound set that comes from the outer scope to correctly
plan an inner scope. `not` requires it because it is doing an antijoin. `or`
might do filters/joins on outer variables. This incoming relation only exists
at runtime. It is used for correct planning, but actually not introduced into the
runtime Stages. The `or` and `not` pattern add the incoming relation pattern to
the stages at runtime. There is a bit of an awkward split here where we try to do
planning up front, but runtime relations are only available, well, at runtime.

An ordinary pattern can propose the next single variable exactly when its
`groundable` function returns a set containing that variable. Other unfinished
patterns join the stage as validators only when every one of their variables is present in
the target layout. In particular, an `or` does not validate (from its perspective)
a partially grounded tuple. Once its complete tuple is bound, it validates the tuple with a semijoin.
Similarly a `not` only validates when all it's participating variables are bound.

The planner select the next legal variable in query variable order. After emitting a stage, it uses
that stage's target layout as the next bound set and reevaluates immediate
groundability. A stage may add several variables when a pattern requires them
as one proposal.

When no pattern other than an `or` can introduce the next variable, an `or` may introduce
all of its missing variables if its `groundable` function returns all of them.
That proposing stage contains only the `or` because `OrPattern.count` is a no-op.
Patterns made fully valid by the grouped proposal run in the following
validation-only stage. This is currently the only case where we introduce
more than one variable at a time.

Every top-level variable must be present in the initial input or groundable by
a pattern in the scope. An `or` requires variables not groundable by every
branch to be bound by its input. A `not` requires all its variables to be bound
by its input. Nested execution receives these bindings through a projected
`RelationPattern`.

For the algorithm to be correct it is important that for every variable part of a pattern,
the pattern is at least once part of a proposal or validation stage where that variables
gets introduced or validated respectively.

## Stage execution

`GenericJoinEngine` folds the stage list over the input `BindingSet`.

For a validation-only stage, every participant receives `added = []`. Each
participant filters the rows in place, after which the engine applies the
stage-level reorder.

For a proposing stage with one participant, the engine calls that pattern's
proposing `join` directly. The planner therefore makes a single participant
only when it can introduce the complete `added` list.

For a proposing stage with several participants, execution is row-local:

1. Every participant contributes a candidate count for every input row.
2. The cheapest positive proposal wins; equal counts retain the earlier
   participant.
3. Rows without a proposer are discarded.
4. Rows are grouped by proposer, and the winner extends each shard group.
5. Every other participant validates the extended rows with `added = []`.
6. The validated groups are unioned in the stage target layout.

The proposer must return `targetVariables`; validators must preserve the layout
they receive.

## Pattern semantics

| Pattern | Planning and execution rules |
|---------|------------------------------|
| `TriplePattern` | Uses a fixed attribute and constant or variable entity/value positions. Every unbound pattern variable is groundable. It proposes through AEV or AVE according to the bound side and validates exact or partial bindings existentially. A constant-only triple is a validation filter. It rejects `count` and proposing `join` calls whose `added` list is not a non-empty subset of its variables. Entity and value variables must differ. |
| `RelationPattern` | Represents a materialized relation, including external and nested-plan inputs. Underneath a Relation is represented by tire, meaning for any prefix in variable order only the next variable can be introduced. Its trie proposes that prefix segment and validates a bound prefix. Bound relation variables followed by `added` must form a prefix of the relation's variable order for the Pattern to participate in a proposal or validation.|
| `PredicatePattern` | Grounds nothing. Once all variable arguments are bound, it evaluates its unary or binary predicate and filters rows without changing their layout. |
| `FunctionPattern` | Grounds only its output, and only after all variable arguments are bound. It evaluates a unary or binary function to propose the output, or compares the computed value with an already bound output. The output differs from every argument variable. |
| `OrPattern` | Planning walks each branch's ordered stages and exposes only variables producible by every branch. Branches have the same variables and introduction order. Execution injects the relevant input columns into each branch, unions branch results distinctly, and matches them back to the full input. It proposes all missing variables only as a stage's sole participant and validates only when every OR variable is bound. |
| `NotPattern` | Grounds and proposes nothing. All of its variables are bound by the outer input. It injects those bindings into its nested stages and removes matching input rows with an antijoin. Unrelated input columns are preserved. |

After the last stage, the `:find` projection is applied. If aggregates exists these are applied before the final projection.
