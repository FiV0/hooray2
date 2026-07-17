# Standard Query Engine

## Overview

The standard query engine evaluates a conformed `:where` clause over ordered
`BindingSet` values. Compilation turns clauses into patterns, planning arranges
those patterns into stages, and `GenericJoinEngine` executes the stages. The
planner works with variable shapes; the executor works with rows.

```text
query -> conformed clauses -> PlanPattern tree -> Stage lists
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
| `PlanPattern` | planner | Exposes `orderedVariables` and the variables reachable from a bound-variable shape through `groundable` |
| `ExecPattern` | executor | Exposes a stable `idx`, its variable set, proposal counts, and row execution through `join` |
| `Pattern` | stages | Combines both contracts after any nested scopes have been planned |

`PlanPattern.groundable(bound)` is a dependency query. It returns unbound
variables in pattern order that the pattern can derive from `bound`. The
planner repeatedly calls it to a fixed point because one pattern can make
another pattern groundable. It does not authorize an arbitrary `join` call;
each pattern still has a specific legal proposal shape.

`ExecPattern.count` updates one proposal per input row when the pattern has a
strictly cheaper positive candidate count. `ExecPattern.join` has two modes:

- A non-empty `added` list asks the pattern to extend the input and return
  exactly `targetVariables`.
- An empty `added` list asks the pattern to filter rows without changing the
  input layout.

## Compilation and planning

Compilation resolves constants, attributes, predicates, functions, database
indexes, and external relations. It preserves the clause tree and assigns a
`PlanPattern` to every node.

Planning is recursive and proceeds from variable shapes to executable stages:

1. Plan a scope against its incoming variable layout.
2. Derive the scope's stage shapes from its `PlanPattern` values.
3. Lower each stage participant to an executable `Pattern` for that shape.
4. When lowering `or` or `not`, recursively plan its child scopes and attach
   their stage lists to the composite pattern.
5. Emit the runtime `Stage` after all its participants are executable.

An `or` planning node derives groundability directly from its child
`PlanPattern` values. Child stages are materialized during lowering. Recursion
terminates because each call descends into a child clause.

Every `or` branch is planned with the same variable set and introduction order.
A `not` body is planned with all of its variables supplied by its outer input.
The nested stages start from the unit binding set. At execution time a projected
`RelationPattern` supplies the applicable outer bindings and constrains the
nested rows.

### Stage construction

The planner starts with the incoming variables as bound and computes grounding
closure across the scope. It selects the next legal introduction in query
variable order while respecting function and composite dependencies. A stage
may add several variables when a pattern requires them as one proposal.

A pattern participates when it can either propose the complete `added` list or
validate the layout produced by the stage. Predicates and negation participate
only as validators and are never the sole participant of a proposing stage. A
function proposes its output once its arguments are bound, or validates once
its output is also bound. Remaining filters over an already complete layout
form a validation-only stage.

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
| `TriplePattern` | Uses a fixed attribute and constant or variable entity/value positions. Every unbound pattern variable is groundable. It proposes through AEV or AVE according to the bound side and validates exact or partial bindings existentially. A constant-only triple is a validation filter. Entity and value variables must differ. |
| `RelationPattern` | Represents a materialized relation, including external and nested-plan inputs. Its trie proposes the next prefix segment and validates a bound prefix. Bound relation variables followed by `added` must form a prefix of the relation's variable order; unrelated input columns may interleave without changing that order. |
| `PredicatePattern` | Grounds nothing. Once all variable arguments are bound, it evaluates its unary or binary predicate and filters rows without changing their layout. |
| `FunctionPattern` | Grounds only its output, and only after all variable arguments are bound. It evaluates a unary or binary function to propose the output, or compares the computed value with an already bound output. The output differs from every argument variable. |
| `OrPattern` | Computes each branch's grounding closure and exposes only variables groundable by every branch. Branches have the same variables and introduction order. Execution injects the relevant input columns into each branch, unions branch results distinctly, and matches them back to the full input. It proposes all missing variables only as a stage's sole participant; otherwise it validates with a semijoin. |
| `NotPattern` | Grounds and proposes nothing. All of its variables are bound by the outer input. It injects those bindings into its nested stages and removes matching input rows with an antijoin. Unrelated input columns are preserved. |

After the last stage, the query layer reorders and projects the binding set to
the `:find` layout and applies result shaping.
