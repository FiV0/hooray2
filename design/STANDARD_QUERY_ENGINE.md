# Standard Query Engine

## Overview

The standard query engine evaluates a conformed `:where` clause over ordered
`BindingSet` values. Compilation turns clauses into patterns, planning arranges
those patterns into stages, and `GenericJoinEngine` executes the stages. The
planner works with variable shapes; the executor works with rows.

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

| Contract | Consumer | Responsibility |
|----------|----------|----------------|
| `hooray.plan/Pattern` | planner | Holds ordered variables, immediate groundability, and the corresponding executable pattern |
| `ExecPattern` | executor | Exposes a stable `idx`, its variable set, proposal counts, and row execution through `join` |

The Clojure planning record contains `idx`, ordered `variables`, a `groundable`
function, and its `exec-pattern`. Calling `groundable` returns only variables
the pattern can introduce immediately from the supplied bound-variable set.
The planner calls it again after each emitted stage, so dependencies become
available as the plan advances rather than through a separate fixed-point
calculation.

`ExecPattern.count` updates one proposal per input row when the pattern has a
strictly cheaper positive candidate count. A participant that cannot propose
the stage's `added` list leaves the proposals unchanged. `ExecPattern.join`
has two modes:

- A non-empty `added` list asks the pattern to extend the input and return
  exactly `targetVariables`.
- An empty `added` list asks the pattern to filter rows without changing the
  input layout.

## Compilation and planning

Compilation resolves constants, attributes, predicates, functions, database
indexes, and external relations. It preserves the clause tree and assigns a
Clojure planning pattern to every node.

Planning is recursive and proceeds from variable shapes to executable stages:

1. Compile primitive clauses into Clojure planning records that contain their
   `ExecPattern` values.
2. Recursively plan `or` branches and `not` bodies with an external-binding
   executor available as a fallback proposer.
3. Attach those child stage lists to the composite executor.
4. Derive the composite's Clojure groundability from the child stages.
5. Emit runtime stages containing only `ExecPattern` participants.

An `or` planning node walks each already-planned branch once in stage order.
Internal stages contribute their added variables. An external-binding stage
can be crossed only when its variables are supplied by the outer bound set;
otherwise that branch cannot advance. The OR exposes the ordered intersection
of the branch results. This accounts for sequential branch dependencies without
recomputing closure over the branch participants.

Every `or` branch is planned with the same variable set and introduction order.
The projected input relation supplies whichever external-binding stages are
satisfied by the actual outer input and constrains internally proposed stages
that introduce an already bound branch variable.
A `not` body is planned with all of its variables supplied by its outer input.
The nested stages start from the unit binding set. At execution time a projected
`RelationPattern` supplies the applicable outer bindings and constrains the
nested rows.

### Stage construction

The planner starts with the incoming variables as bound and selects the next
legal introduction in query variable order. After emitting a stage, it uses
that stage's target layout as the next bound set and reevaluates immediate
groundability. A stage may add several variables when a pattern requires them
as one proposal.

An ordinary pattern can propose the next single variable exactly when its
`groundable` function returns that variable. Other unfinished patterns join
the stage as validators only when every one of their variables is present in
the target layout. In particular, an OR does not validate a partially grounded
tuple. Once its complete tuple is bound, it validates the tuple with a semijoin
and cannot combine values from different branches.

When no ordinary pattern can introduce the next variable, an OR may introduce
all of its missing variables if its `groundable` function returns all of them.
That proposing stage contains only the OR because `OrPattern.count` is a no-op.
Patterns made fully valid by the grouped proposal run in the following
validation-only stage.

Every top-level variable must be present in the initial input or groundable by
a pattern in the scope. An `or` requires variables not groundable by every
branch to be bound by its input. A `not` requires all its variables to be bound
by its input. Nested execution receives these bindings through a projected
`RelationPattern`.

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
4. Rows are grouped by proposer, and the winner extends each group.
5. Every other participant validates the extended rows with `added = []`.
6. The validated groups are unioned in the stage target layout.

The proposer must return `targetVariables`; validators must preserve the layout
they receive.

## Pattern semantics

| Pattern | Planning and execution rules |
|---------|------------------------------|
| `TriplePattern` | Uses a fixed attribute and constant or variable entity/value positions. Every unbound pattern variable is groundable. It proposes through AEV or AVE according to the bound side and validates exact or partial bindings existentially. A constant-only triple is a validation filter. It rejects `count` and proposing `join` calls whose `added` list is not a non-empty subset of its variables. Entity and value variables must differ. |
| `RelationPattern` | Represents a materialized relation, including external and nested-plan inputs. Planning exposes only the next variable of a legal relation prefix. Its trie proposes that prefix segment and validates a bound prefix. Bound relation variables followed by `added` must form a prefix of the relation's variable order; unrelated input columns may interleave without changing that order. |
| `PredicatePattern` | Grounds nothing. Once all variable arguments are bound, it evaluates its unary or binary predicate and filters rows without changing their layout. |
| `FunctionPattern` | Grounds only its output, and only after all variable arguments are bound. It evaluates a unary or binary function to propose the output, or compares the computed value with an already bound output. The output differs from every argument variable. |
| `OrPattern` | Clojure planning walks each branch's ordered stages and exposes only variables producible by every branch. Branches have the same variables and introduction order. Execution injects the relevant input columns into each branch, unions branch results distinctly, and matches them back to the full input. It proposes all missing variables only as a stage's sole participant and validates only when every OR variable is bound. |
| `NotPattern` | Grounds and proposes nothing. All of its variables are bound by the outer input. It injects those bindings into its nested stages and removes matching input rows with an antijoin. Unrelated input columns are preserved. |

After the last stage, the query layer reorders and projects the binding set to
the `:find` layout and applies result shaping.
