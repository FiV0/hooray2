# Spec: Hooray Incremental Stream Pipeline

## Objective

Replace the current N→1 dynamic incremental join with a compiled stream/circuit
model: an initial **Clojure analysis phase** produces a reusable graph of nodes
and streams, decides which relations are base relations versus derived
relations, and selects the stream operators needed by each clause. The per-tick
driver walks that pre-built graph rather than re-discovering it.

The compiled graph must accommodate streams, first-class `mapIndex`,
`integrate`, `delay`, and `differentiate` operators, the incremental WCOJ
source, `ZSetGenericJoin`, and simple transforms (`project`, `distinct`,
`filter`). The first implementation targets streams over base relations only:
the operator concepts are present immediately, while derived relations and a
general trace implementation are explicitly out of scope for v1.

Success means: (a) `IncrementalGenericJoin` semantics expressible as a
compiled circuit; (b) base relations expose AEV/AVE/EAV/VAE as physical index
streams, where a stream over EAV is simply the advancement in time of the EAV
index; (c) only demanded base index streams get wired; (d) derived relations
are not part of v1, but can later expose one natural stream and add explicit
`mapIndex`/`map_reindex`, `integrate`, `delay`, and `differentiate` nodes when
joins need a different variable order or previous accumulated state; (e)
existing Clojure incremental query tests pass through the new path; (f) the
disk-backed asymmetry is visible in the graph, not buried in type machinery.

## Tech Stack

- Kotlin 2.1.20 / JVM 17 — runtime engine, stream/circuit types, operators
- Clojure 1.12.3 — query compiler frontend; produces `Circuit` via Kotlin builder
- Gradle (`./gradlew`) — build + test
- JUnit 5 for Kotlin tests; existing Clojure test setup for end-to-end

Reference (not a dependency): `/home/finn/src/github.com/feldera/feldera/crates/dbsp`

## Commands

```bash
./gradlew build                                # full build
./gradlew test                                 # all tests
./gradlew test --tests 'org.hooray.incremental.stream.*'   # new Kotlin tests
./gradlew test --tests '*query_inc*'           # Clojure incremental tests
git status --short --branch                    # state check
```

## Project Structure

Greenfield rewrite **alongside** the existing pipeline. Nothing in
`org.hooray.incremental.*` (current) is deleted in v1.

```text
src/main/kotlin/org/hooray/incremental/stream/
  Circuit.kt              compiled graph + InputHandle + step() entry
  CircuitSpec.kt          immutable compiled circuit specification
  InputHandle.kt          external write-side API for feeding ZSetIndices
  Stream.kt               Stream<T> plus aliases for ZSet/indexed/accumulated payloads
  Pattern.kt              CompiledPattern inputs, starting with compiled triples
  Relation.kt             relation capabilities for later derived relations
  Node.kt                 input, join, transform, stateful, and later derived nodes
  IndexSpec.kt            permutation + fixed-prefix + level participation
  ops/                    ZSetGenericJoin.kt, MapIndex.kt, Integrate.kt, Delay.kt,
                          Differentiate.kt, Project.kt, Distinct.kt
  analysis/               pattern compilation helpers, TypeCheck.kt

src/main/clojure/hooray/incremental/stream.clj
  compile-incremental-stream-q — analyzes query clauses and creates CircuitSpec

src/test/kotlin/org/hooray/incremental/stream/
  unit tests per operator + analysis pass tests + end-to-end circuit tests

src/test/clojure/hooray/
  existing query_inc_test.clj covers end-to-end behavior via the new compiler
  once parity is reached (compiler may dispatch on flag during migration)

docs/specs/hooray-incremental-stream-pipeline.md   (this file)
```

## Architecture

### The model in one paragraph

A compiled `Circuit` is a DAG of `Node`s connected by typed `Stream`s.
The circuit has exactly one external input value type: `ZSetIndices`,
containing the per-tick delta Z-sets for EAV, AEV, AVE, and VAE. Runtime input
is fed through an `InputHandle<ZSetIndices>`, following Feldera's split between
the external write-side handle and the internal input stream. Base streams are
views over that single input stream, not independent inputs.
Base relations are represented first by compiled triple patterns. Each compiled
triple pattern knows how to expose the AEV and/or AVE arrangement required by a
branch-local variable order. A stream over a
base physical index, for example EAV, is the advancement in time on that index:
each tick carries the EAV delta for that transaction. Derived relations are
**single-output nodes** producing one Z-set stream in their natural tuple order.
When a derived relation participates in a join, the graph explicitly creates
reindexing as a `MapIndex`/`map_reindex` operator that produces a new indexed
Z-set stream. When a join branch needs previous accumulated state, the graph
also expresses that with `integrate`, `delay`, and `differentiate` nodes rather
than hiding it inside the WCOJ kernel. The cost difference between "free
disk-backed permutation" and "computed reindex" is therefore visible in the
graph — different node kinds, not different runtime branches behind a shared
interface.

Base streams do not fundamentally need to save incoming deltas. They only need
to know the timestamp/view they are exposing for the current circuit step.
Their `integrate`/`delay`/`delay().integrate()` behavior can be backed by views
over the base indexes and their clock, not by copying every incoming delta into
per-stream storage. For implementation simplicity, the first base-stream
version may store the incoming delta batch on the base stream. A second,
independent implementation step should remove that storage and expose `ZSet`
views over the relevant base indexes in `ZSetIndices`.

### Lifecycle

```text
Query (Clojure)
   │
   ▼
[ Clojure analysis phase ]                                                 │
   • compute var-order                                                     │
   • compile each where clause to a CompiledPattern                        │
   • compile find/project transforms                                       │
   • create an IncrementalWcojJoinSpec and CircuitSpec                     │
   │                                                                       │
   ▼                                                                       │
Kotlin compiled circuit                                                    │
   │   IncrementalWcojJoinSpec(patterns, levels, canonicalOrder)           │
   │   transforms                                                          │
   │   InputHandle<ZSetIndices>                                            │
   │                                                                       │
   ▼                                                                       │
Circuit  ── runtime ───────────────────────────────────────────────────────┘
   input.set(next: ZSetIndices)
   .step() → ResultZSet
     • read the current input batch
     • compute output using each stateful node's previous state
     • advance state for the next step
```

The runtime API is `InputHandle` plus `step()`. Operators may still internally
separate "read previous state" from "advance state", but the public circuit
model exposes only clock advancement through `step()`.

### Relation capabilities, not stream capabilities

Generic `Stream` should not expose knowledge about which physical indexes exist.
A stream says only that it carries time-varying data and supports dataflow
operators such as `mapIndex`, `integrate`, `delay`, and `differentiate`.

Index availability belongs on a relation descriptor used by the Clojure
analysis phase:

```kotlin
interface Relation<Row> {
    val id: RelationId
    val canonicalStream: ZSetStream<Row>

    fun availableIndexes(): Set<IndexLayout>
    fun canProvide(layout: IndexLayout): Boolean
    fun index(layout: IndexLayout): IndexedZSetStream<*, *>
}
```

`BaseRelation` can provide EAV, AEV, AVE, and VAE as physical index streams
over the same `ZSetIndices` clock. `DerivedRelation` can provide its natural
layout directly and any other layout by inserting `mapIndex`/`map_reindex`.
Thus permutations are relation capabilities, not universal stream facts.

### Input handle

The circuit should expose an `InputHandle<ZSetIndices>` as its external write
boundary:

```kotlin
class InputHandle<T> {
    fun set(value: T)
    fun clear()
}

class Circuit(
    val input: InputHandle<ZSetIndices>
) {
    fun step(): ResultZSet

    // Migration convenience while adapting existing compute-delta! call sites.
    fun step(input: ZSetIndices): ResultZSet {
        this.input.set(input)
        return step()
    }
}
```

Conceptually:

```text
InputHandle<ZSetIndices>
  -> InputStream<ZSetIndices>
      -> BaseRelation(EAV)
      -> BaseRelation(AEV)
      -> BaseRelation(AVE)
      -> BaseRelation(VAE)
```

The handle is not per base relation and not per permutation. It writes the next
transaction's complete delta bundle. On each clock tick, the input node exposes
the currently buffered `ZSetIndices` to downstream base relation views. If no
input was set, the circuit should use empty `ZSetIndices`.

This separates feeding input for the next tick from advancing the circuit clock.
The existing `step(input: ZSetIndices)` shape remains useful as a compatibility
wrapper during migration, but the long-term model should be
`input.set(delta); circuit.step()`.

## API Shape

Types (illustrative; final generics may differ). There should be one generic
stream abstraction, following Feldera's `Stream<C, D>` shape. Names like
`ZSetStream`, `IndexedZSetStream`, and `AccumulatedStream` are aliases for
readability, not separate stream interfaces:

```kotlin
// ── values flowing on edges ─────────────────────────────────────────────
sealed interface Stream<out T> { val node: Node }

typealias ZSetStream<T> = Stream<ZSet<T, IntegerWeight>>
typealias IndexedZSetStream<K, V> = Stream<IndexedZSet<K, V, IntegerWeight>>
typealias AccumulatedStream<T> = Stream<AccumulatedZSet<T>>

// ── nodes ───────────────────────────────────────────────────────────────
sealed interface Node { val id: NodeId; val label: String }

interface DerivedNode<T> : Node {
    val output: ZSetStream<T>
}

// ── compiled inputs, aligned with the current IncrementalGenericJoin path ─
sealed interface CompiledPattern

data class CompiledTriplePattern(
    val entityConstant: Any?,
    val attribute: Any,
    val valueConstant: Any?,
    val entityVarCanonicalIndex: Int,
    val valueVarCanonicalIndex: Int
) : CompiledPattern

data class IncrementalWcojJoinSpec(
    val patterns: List<CompiledPattern>,
    val levels: Int,
    val canonicalOrder: VariableOrder
)

data class CircuitSpec(
    val input: InputHandle<ZSetIndices>,
    val source: IncrementalWcojJoinSpec,
    val transforms: List<TransformSpec>
)

class Circuit(private val spec: CircuitSpec) {
    val input: InputHandle<ZSetIndices>
    fun step(): ResultZSet
    fun step(input: ZSetIndices): ResultZSet
}
```

`mapIndex` is a first-class stream operator in v1:

```kotlin
fun <T, K, V> mapIndex(
    input: ZSetStream<T>,
    spec: IndexSpec<T, K, V>
): IndexedZSetStream<K, V>
```

`integrate`, `delay`, and `differentiate` are also first-class stream
operators in v1. They are part of the graph and type model even when the first
base-only implementation backs them with base-index views instead of a general
trace implementation:

```kotlin
fun <T> integrate(input: ZSetStream<T>): AccumulatedStream<T>

fun <T> delay(input: ZSetStream<T>): ZSetStream<T>

fun <T> delay(input: AccumulatedStream<T>): AccumulatedStream<T>

fun <T> differentiate(input: AccumulatedStream<T>): ZSetStream<T>
```

`derive` is not a separate primitive in this spec. If the code wants that name
for readability, it should be an alias or helper around `differentiate` so the
operator vocabulary stays small.

`IndexSpec` is worth keeping because `mapIndex` needs a structured,
inspectable description of the target layout. An opaque lambda would work for
execution, but it would make Clojure analysis, tests, branch planning, and
debugging lose the information that the index key is, for example, "variable
levels [1, 0] with this fixed prefix". `IndexSpec` is not used to choose AEV
versus AVE for base `CompiledTriplePattern`; that stays encoded in the compiled
pattern. It is the contract for explicit `mapIndex` nodes, especially derived
relations and canonicalization/reordering nodes.

```kotlin
data class IndexSpec<T, K, V>(
    val name: String,
    val keyLevels: List<Int>,        // which tuple positions form the key
    val valueLevels: List<Int>,      // which positions form the value
    val fixedPrefix: Prefix = emptyList()
)
```

For base triple patterns, arrangement selection still happens through
`CompiledTriplePattern.view(variableOrder, state)`. For derived relations or
explicit result reordering, `mapIndex(s, spec)` introduces a real operator. The
Clojure analysis layer may expose this as `map_reindex` to make clear that the
operator changes the stream's join/index layout rather than only mapping row
values.

## Join Mapping

`IncrementalWcojJoin` is not the circuit. It is the source sub-circuit created
from the compiled patterns that Clojure already produces today. The current
non-stream path is:

```text
compile-inc-pattern
  -> CompiledTriplePattern(...)
  -> IncrementalJoinOperator(compiledPatterns, levels)
  -> IncrementalWcojJoinEngine(patterns, levels)
```

The stream/circuit version should preserve that shape:

```text
compile-inc-pattern
  -> CompiledPattern values
  -> IncrementalWcojJoinSpec(patterns, levels, canonicalOrder)
  -> CircuitSpec(source = IncrementalWcojJoinSpec, transforms = ...)
```

For base-only v1, `CompiledTriplePattern` remains the concrete pattern type.
It already owns the base-index knowledge that matters today:

- which constants are fixed;
- which canonical variable indexes correspond to entity and value;
- whether AEV, AVE, or both arrangements are available;
- how to derive a branch-local view from `variableOrder` and state.

The expansion creates branches for the incremental delta formula. For each
branch, permutations happen outside the WCOJ kernel:

```text
relation streams
  -> mapIndex/map_reindex into branch variable order
  -> integrate/delay/differentiate where the delta formula needs current vs previous state
  -> ZSetGenericJoin
  -> mapIndex/permutation back to canonical order when needed
  -> sum/merge branch outputs
```

`ZSetGenericJoin` is the lower-level operator. It takes N streams whose ZSets
already arrive in the correct order and produces a result delta stream. It does
not select base indexes, decide delta terms, run `variableOrderForDeltaTerm`,
or undo permutations. Those are graph-construction responsibilities of
`IncrementalWcojJoin` and the Clojure analysis phase.

For derived operators, the likely shape is to `mapIndex` before a branch of
`variableOrderForDeltaTerm`, then apply another `mapIndex`/canonicalization
operator that currently corresponds to `permutateToCanonical`. If the derived
operator participates as an accumulated relation rather than the current delta,
the graph must say so explicitly with `integrate` and `delay`, and use
`differentiate` when an accumulated stream needs to become a delta stream again.

## Circuit Analysis (the new Clojure phase)

The first analysis phase is in Clojure. For base-only v1, it should stay close
to the current `hooray.incremental/compile-incremental-q` path:

1. **VariableOrder** — compute `var-order` with the existing query planner.

2. **PatternCompilation** — compile each supported where clause into a
   `CompiledPattern`. The first concrete implementation remains
   `CompiledTriplePattern`, not relation-builder calls.

3. **IncrementalWcojSpec** — group compiled patterns into an
   `IncrementalWcojJoinSpec(patterns, levels, canonicalOrder)`.

4. **TransformCompilation** — compile `find`, `project`, `distinct`, and later
   derived operators into transform specs.

5. **DerivedRelationExpansion** — for function applications and other derived
   relations, insert the natural output stream first, then add `map_reindex`
   when the join needs a different key/value order. Insert `integrate`,
   `delay`, and `differentiate` nodes when the branch needs accumulated,
   previous, or delta views. Derived relations are not part of v1; when they
   are added later, they may need a general trace implementation behind the
   same graph vocabulary.

6. **TypeCheck** — for each `join`, assert `left.spec.keyType == right.spec.keyType`;
   for `differentiate(integrate(x))`, the identity contract holds at the type
   level. Type errors abort circuit construction.

Analysis output is captured in a `CircuitSpec`/`Schedule` value the runtime
walks per tick.
Tests for analysis live in `src/test/kotlin/org/hooray/incremental/stream/analysis/`
and assert e.g. "this query compiles a triple pattern that needs both AEV and
AVE arrangements."

## Migration Strategy

- New code lives in `org.hooray.incremental.stream.*`; old `IncrementalPipeline`
  and `IncrementalGenericJoin` untouched.
- New Clojure entry: `hooray.incremental.stream/compile-incremental-stream-q`.
  This should mirror the existing `compile-incremental-q`: compute `var-order`,
  compile `where` clauses to `CompiledPattern`s, compile transforms, and return
  a circuit object.
- Implementation step 1: target only streams over base relations, with
  first-class `mapIndex`, `integrate`, `delay`, and `differentiate` nodes.
  Allow base streams to save the incoming delta batch internally. This keeps
  the graph model visible before optimizing base stream storage.
- Implementation step 2: still target only base relations, but remove saved
  base deltas. Base streams instead expose timestamped `ZSet` views over
  `ZSetIndices`, and base-backed `integrate`/`delay` read from those views.
- The primary runtime API should become `circuit.input.set(delta)` followed by
  `circuit.step()`. Keep `circuit.step(delta)` as a compatibility wrapper while
  migrating existing Clojure call sites.
- Later step, explicitly after v1: introduce derived relations and, if needed,
  general trace storage for delayed accumulated state beyond what base indexes
  can provide.
- A dynamic var `*circuit-version*` in `incremental.clj` dispatches between old
  and new path. Existing `query_inc_test.clj` runs under both during
  parity bring-up.
- Cut over when all tests pass on the new path. Removing
  `IncrementalGenericJoin` is **Ask first** — a deliberate separate change.

## Code Style

One illustrative shape, aligned with the current compiled-pattern API:

```kotlin
data class IncrementalWcojJoinSpec(
    val patterns: List<CompiledPattern>,
    val levels: Int,
    val canonicalOrder: VariableOrder
)

class IncrementalCircuit(private val spec: CircuitSpec) {
    val input = spec.input

    fun step(): ResultZSet {
        val delta = input.takeOrEmpty()
        return runStep(delta)
    }
}
```

Guidelines:
- Clojure analysis/spec construction and per-tick evaluation (`Circuit.step`)
  are separate concerns and live in separate files.
- External input feeding (`InputHandle`) and circuit clock advancement
  (`Circuit.step`) are separate concerns, even though v1 may provide
  `step(input)` as a wrapper.
- Permutation choice is data on `IndexSpec`, never branches inside operators.
- Physical index availability is data on `Relation`, not methods on generic
  `Stream`.
- V1 has accumulated-state operators at the stream level:
  `integrate : ZSetStream<T> -> AccumulatedStream<T>`,
  `delay : Stream<T> -> Stream<T>`, and
  `differentiate : AccumulatedStream<T> -> ZSetStream<T>`. What v1 excludes is
  a general trace implementation and derived relations.
- Use Kotlin sealed interfaces for `Node` / `Stream` taxonomies — exhaustive
  `when` over node kinds is the analysis pass's main idiom.
- Standard Read/Edit/Write tools for `.kt` and `.java`; MCP Clojure tools
  only for `.clj` / `.cljs` / `.cljc`.

## Testing Strategy

Layered, with most coverage at the lower layers since per-tick correctness is
where DBSP-style code breaks:

- **Stream/operator unit tests** (`src/test/kotlin/.../stream/ops/`):
  `mapIndex` produces correct weights/keys; `ZSetGenericJoin` matches a
  reference implementation on randomized already-ordered inputs; base-backed
  `integrate`, `delay`, and `differentiate` satisfy their stream contracts.
- **Analysis tests** (`src/test/kotlin/.../stream/analysis/`):
  given fixture compiled patterns, assert the required AEV/AVE arrangements,
  the expanded incremental WCOJ branch graph, and type-checking behavior.
- **Circuit-level tests** (Kotlin): tiny end-to-end queries built directly
  from `CircuitSpec`, stepped tick by tick, compared against full-query results.
  Include tests for `input.set(delta); step()` and the compatibility
  `step(delta)` wrapper.
- **End-to-end Clojure tests** (`src/test/clojure/hooray/query_inc_test.clj`):
  parameterize over old vs new compiler; assert identical results on every
  existing case (sanity, basic queries, retractions, DBSP distinct
  semantics, triangle WCOJ bad case).
- **Mixed-permutation regression**: a single query that consumes AVE and
  AEV views of the same base relation; assert both views see consistent
  deltas across ticks and the result matches the full query.

## Boundaries

**Always:**
- Keep the existing Clojure incremental public API (`query`, `compute-delta!`,
  `pop-result!`) working throughout migration.
- Preserve signed Z-set weights, zero-weight cleanup, and the rule that one
  `step()` reads previous state, computes output, then advances state for the
  next step.
- Validate the new compiled circuit's output against the full-query result on
  every existing test before declaring parity.

**Ask first:**
- Deleting any code under `org.hooray.incremental.*` (old path) — separate
  change after migration.
- Adding new dependencies (e.g., a third-party graph library).
- Replacing the `ZSet` / `IndexedZSet` / `IntegerWeight` data model.
- Changing the on-disk EAV/AEV/AVE/VAE format.
- Changing public Clojure query syntax.

**Never:**
- Treat AEV/AVE/EAV/VAE of the same base relation as streams with independent
  time semantics — they are siblings driven by the same input delta.
- Advance accumulated state before the current `step()` has computed all
  outputs from the previous state.
- Rediscover query structure at runtime. Clojure analysis fixes the compiled
  patterns and transforms in `CircuitSpec`; during `step()`, a compiled pattern
  may still select among its predeclared arrangements (for example AEV vs AVE)
  for the branch-local variable order.
- Put physical index capability methods on generic `Stream`; use relation
  descriptors instead.
- Add separate external handles for EAV/AEV/AVE/VAE; the external input is one
  `InputHandle<ZSetIndices>`.
- Let `ZSetGenericJoin` perform permutation selection or canonicalization;
  inputs must already be arranged by the expansion phase.

## Success Criteria

1. Clojure analysis returns a `CircuitSpec` whose source
   `IncrementalWcojJoinSpec` and transforms are deterministic and
   inspectable.
2. A query needing AVE and AEV views of the same base relation compiles
   without duplicating source state; the compiled triple pattern exposes both
   arrangements as needed.
3. The only external circuit input is `ZSetIndices`; base relation streams are
   views over that input.
4. The primary circuit runtime API separates input feeding from clock
   advancement via `InputHandle<ZSetIndices>` and `Circuit.step()`, with
   `Circuit.step(input)` retained as a migration wrapper.
5. `IncrementalWcojJoin` expands to branch-local `mapIndex`, `integrate`,
   `delay`, `differentiate`, `ZSetGenericJoin`, and canonicalization nodes; it
   is not modeled as the `Circuit` itself.
6. A base-only first version works with base streams saving incoming deltas.
7. A second base-only version removes saved base deltas and uses timestamped
   `ZSet` views over base indexes instead.
8. A derived stream reindexed two different ways uses two explicit
   `MapIndex` operator nodes; the graph reflects the cost.
9. Every test currently passing under `org.hooray.incremental.*` passes
   when `query_inc_test.clj` runs through the stream pipeline.
10. Adding derived relations and a general trace implementation later should
   not require changing the first-class `integrate`, `delay`, and
   `differentiate` operator vocabulary.

## Decisions

1. **Package/namespace naming.** Kotlin uses
   `org.hooray.incremental.stream.*`; Clojure uses
   `hooray.incremental.stream`.
2. **Output cardinality.** v1 has a single output per query, matching the
   current `pop-result!` model.
3. **Trace and derived relations.** v1 has no trace implementation and no
   derived relations. The stream operator vocabulary still includes
   `integrate`, `delay`, and `differentiate`; base-only implementations may
   back those operators with base-index views.
4. **Migration switch mechanism.** Use dynamic var `*circuit-version*` in
   `incremental.clj`.
