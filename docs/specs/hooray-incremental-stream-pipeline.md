# Spec: Hooray Incremental Stream Pipeline

## Objective

Replace the current N→1 dynamic incremental join with a compiled stream/circuit
model: an initial **Clojure analysis phase** produces a reusable graph of nodes
and streams, decides which relations are base relations versus derived
relations, selects the stream operators needed by each clause, and computes the
operator schedule. The per-tick driver walks that pre-built graph rather than
re-discovering it.

The compiled graph must accommodate (at the API/type level):
`mapIndex`, `join`, `delay` (z⁻¹), `integrate` (to an accumulated view),
`differentiate`/`derive`, plus simple transforms (`project`, `distinct`,
`filter`).
All streams should expose these operators uniformly, but the first
implementation only targets streams over base relations. The general
`TraceStream` implementation is deferred until derived relations participate in
joins; base relation streams can implement `integrate` and `delay` through base
index views.

Success means: (a) `IncrementalGenericJoin` semantics expressible as a
compiled circuit; (b) base relations expose AEV/AVE/EAV/VAE as physical index
streams, where a stream over EAV is simply the advancement in time of the EAV
index; (c) only demanded base index streams get wired; (d) derived relations
expose one natural stream and add explicit `integrate`, `delay`, and
`mapIndex`/`map_reindex` operators when joins need delayed state or a different
variable order; (e) existing Clojure incremental query tests pass through the
new path; (f) the disk-backed asymmetry is visible in the graph, not buried in
type machinery.

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
  Circuit.kt              compiled graph + step(input) entry
  CircuitBuilder.kt       builder used by the Clojure analysis phase
  InputHandle.kt          external write-side API for feeding ZSetIndices
  Stream.kt               Stream<T> / ZSetStream<T> / IndexedStream<K,V> / TraceStream<T>
  Relation.kt             relation capabilities: physical indexes vs computed reindexes
  Node.kt                 BaseSource / DerivedNode / operator nodes
  IndexSpec.kt            permutation + fixed-prefix + level participation
  ops/                    ZSetGenericJoin.kt, MapIndex.kt, Integrate.kt, Delay.kt, Differentiate.kt, Project.kt, Distinct.kt
  analysis/               PermutationDemand.kt, TopoSchedule.kt, TypeCheck.kt
  trace/                  TraceStream API for later derived-relation joins

src/main/clojure/hooray/incremental/stream.clj
  compile-incremental-stream-q — analyzes base/derived relations and builds Circuit via Kotlin builder

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
Base relations are **multi-output source nodes**: a single `BaseSource` can
emit up to four `IndexedStream`s (one per permutation), but only the ones a
downstream consumer demands are actually wired and updated. A stream over a
base physical index, for example EAV, is the advancement in time on that index:
each tick carries the EAV delta for that transaction. Derived relations are
**single-output nodes** producing one `ZSetStream` in their natural tuple order.
When a derived relation participates in a join, the graph explicitly creates
the delayed accumulated state the join needs, and reindexing a derived stream is
always an explicit `MapIndex`/`map_reindex` operator that produces a new
`IndexedStream`. The cost difference between "free disk-backed permutation" and
"computed reindex" is therefore visible in the graph — different node kinds,
not different runtime branches behind a shared interface.

Base streams do not fundamentally need to save incoming deltas. They only need
to know the timestamp/view they are exposing for the current circuit step. For
implementation simplicity, the first base-stream version may store the incoming
delta batch on the base stream. A second, independent implementation step should
remove that storage and expose a `ZSet` view over the relevant base index in
`ZSetIndices`; base `integrate`, `delay`, and `delay().integrate()` operators
then work from those views.

### Lifecycle

```text
Query (Clojure)
   │
   ▼
[ Clojure analysis phase ]                                                 │
   • classify base datom relations versus derived relations                │
   • choose base index streams such as EAV/AEV/AVE/VAE                     │
   • insert derived operators, integrate, delay, and map_reindex           │
   • compute operator schedule and join inputs                             │
   │                                                                       │
   ▼                                                                       │
CircuitBuilder                                                             │
   │   .baseRelation(":r/to")                                              │
   │   .view(EAV) / .mapIndex(...) / .integrate(...) / .delay(...)         │
   │   .build()                                                            │
   │                                                                       │
   ▼                                                                       │
Circuit  ── runtime ───────────────────────────────────────────────────────┘
   input.set(next: ZSetIndices)
   .step() → ResultZSet
     • eval phase (all operators, against z⁻¹)
     • commit phase (advance accumulated state)
```

The two-phase `eval` / `commit` discipline already in `IncrementalPipeline`
is preserved; what changes is that Clojure decides the schedule once while
creating the circuit, not rediscovered each tick.

### Why this shape over Model α (one stream + cached indexBy)

Model α requires the planner to track "which permutations of which base
streams are actually consumed" as a separate analysis to avoid materializing
all 4 permutations every tick. That demand-tracking exists either way. In
Model B/γ it **is** the graph — each consumed permutation is a wired stream,
each unconsumed one isn't. One less layer.

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
    fun index(layout: IndexLayout): IndexedStream<*, *>
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

Types (illustrative; final generics may differ):

```kotlin
// ── values flowing on edges ─────────────────────────────────────────────
sealed interface Stream<out T> { val node: Node }

interface ZSetStream<T>             : Stream<ZSet<T, IntegerWeight>>
interface IndexedStream<K, V>       : Stream<IndexedZSet<K, V, IntegerWeight>> {
    val spec: IndexSpec<*, K, V>
}
interface TraceStream<T>            : Stream<Trace<T>>   // efficient impl deferred

// ── nodes ───────────────────────────────────────────────────────────────
sealed interface Node { val id: NodeId; val label: String }

interface BaseSource : Node {
    fun view(perm: Perm, fixedPrefix: Prefix = emptyList()): IndexedStream<*, *>
    fun zset(): ZSetStream<Triple>          // unindexed delta of triples
}

interface DerivedNode<T> : Node {
    val output: ZSetStream<T>
}

// ── builder ─────────────────────────────────────────────────────────────
class CircuitBuilder {
    fun addInputZSetIndices(): Pair<ZSetIndicesStream, InputHandle<ZSetIndices>>
    fun baseRelation(id: RelationId): BaseSource
    fun mapIndex(s: ZSetStream<T>, spec: IndexSpec<T, K, V>): IndexedStream<K, V>
    fun zSetGenericJoin(inputs: List<IndexedStream<*, *>>, variableOrder: VariableOrder): ZSetStream<ResultTuple>
    fun incrementalWcojJoin(inputs: List<WcojRelationInput>, canonicalOrder: VariableOrder): ZSetStream<ResultTuple>
    fun integrate(s: ZSetStream<T>): TraceStream<T>      // base-backed first, general trace later
    fun differentiate(t: TraceStream<T>): ZSetStream<T>
    fun derive(t: TraceStream<T>): ZSetStream<T> = differentiate(t)
    fun delay(s: ZSetStream<T>): ZSetStream<T>           // z⁻¹
    fun project(s: ZSetStream<T>, fn: (T) -> U): ZSetStream<U>
    fun distinct(s: ZSetStream<T>): ZSetStream<T>
    fun output(s: ZSetStream<T>): OutputHandle<T>
    fun build(): Circuit                                 // validates and freezes analyzed graph
}

// ── compiled, reusable ──────────────────────────────────────────────────
class Circuit {
    val input: InputHandle<ZSetIndices>
    fun step(): Map<OutputHandle<*>, ZSet<*, IntegerWeight>>
    fun step(input: ZSetIndices): Map<OutputHandle<*>, ZSet<*, IntegerWeight>>
}
```

`IndexSpec` describes a permutation request without bolting query semantics
onto the type:

```kotlin
data class IndexSpec<T, K, V>(
    val name: String,
    val keyLevels: List<Int>,        // which tuple positions form the key
    val valueLevels: List<Int>,      // which positions form the value
    val fixedPrefix: Prefix = emptyList()
)
```

For base relations, `IndexSpec` maps to one of {EAV, AEV, AVE, VAE}; for
derived relations, `mapIndex(s, spec)` introduces a real operator. The Clojure
analysis layer may expose this as `map_reindex` to make clear that the operator
changes the stream's join/index layout rather than only mapping row values.

## Join Mapping

`IncrementalWcojJoin` is not the circuit. It is a sub-circuit expansion created
by the Clojure analysis phase. It takes semantic relation inputs and emits one
canonical result delta stream:

```kotlin
data class WcojRelationInput(
    val relation: Relation<*>,
    val atom: RelationAtom,
    val participatesInLevels: List<Int>
)

fun incrementalWcojJoin(
    inputs: List<WcojRelationInput>,
    canonicalOrder: VariableOrder
): ZSetStream<ResultTuple>
```

Each `WcojRelationInput` must provide enough information for the analysis phase
to choose delta, integrated, delayed, and reindexed views:

- the base or derived relation;
- the clause binding information: constants, variables, and tuple positions;
- available/indexable layouts through the relation capability API;
- level participation for the WCOJ variable order;
- access to delta/current/old views through stream operators.

The expansion creates branches for the incremental delta formula. For each
branch, permutations happen outside the WCOJ kernel:

```text
relation streams
  -> mapIndex/map_reindex into branch variable order
  -> delay/integrate as required by the delta term
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
`variableOrderForDeltaTerm`, then apply the inverse/canonical mapping that
currently corresponds to `permutateToCanonical`.

## Circuit Analysis (the new Clojure phase)

The first analysis phase is in Clojure. It owns relation classification and
operator selection before Kotlin runtime evaluation starts. It should produce a
circuit description with these passes:

1. **PermutationDemand** — walk consumers of each `BaseSource`. The union of
   permutations any `IndexedStream` view exposes from that source is the set
   the runtime needs to accept on input. Permutations not in the set are not
   wired into the circuit.

2. **IncrementalWcojExpansion** — expand each semantic incremental WCOJ join
   into branch-local `mapIndex`, `integrate`, `delay`, `ZSetGenericJoin`, and
   canonicalization nodes. The lower-level `ZSetGenericJoin` inputs must already
   be ordered correctly.

3. **DerivedRelationExpansion** — for function applications and other derived
   relations, insert the natural output stream first, then add `integrate` and
   `delay` when a join must use prior relation state, and add `map_reindex`
   when the join needs a different key/value order.

4. **TopoSchedule** — Kahn's algorithm over the node DAG. Detect cycles;
   the only legal cycle is one closed by a `delay` node (z⁻¹). Failing
   cycles abort `build()` with a clear error pointing at the offending nodes.

5. **TypeCheck** — for each `join`, assert `left.spec.keyType == right.spec.keyType`;
   for `differentiate(integrate(x))`, the identity contract holds at the type
   level. Type errors abort `build()`.

Analysis output is captured in a `Schedule` value the runtime walks per tick.
Tests for analysis live in `src/test/kotlin/org/hooray/incremental/stream/analysis/`
and assert e.g. "this query demands AVE and AEV of `:r/to`, only EAV of `:s/to`."

## Migration Strategy

- New code lives in `org.hooray.incremental.stream.*`; old `IncrementalPipeline`
  and `IncrementalGenericJoin` untouched.
- New Clojure entry: `hooray.incremental.stream/compile-incremental-stream-q`.
  This is the initial analysis phase and should know about base relations,
  derived relations, and the required stream operators.
- Implementation step 1: target only streams over base relations, and allow
  base streams to save the incoming delta batch internally. This keeps the
  graph model visible before optimizing base stream storage.
- Implementation step 2: still target only base relations, but remove saved
  base deltas. Base streams instead expose timestamped `ZSet` views over
  `ZSetIndices`; `BaseOperator` implementations of `integrate`,
  `delay().integrate()`, and `delay` use those views.
- The primary runtime API should become `circuit.input.set(delta)` followed by
  `circuit.step()`. Keep `circuit.step(delta)` as a compatibility wrapper while
  migrating existing Clojure call sites.
- Later step: introduce general trace support for derived operators that are
  not backed by base relation indexes.
- A boolean (env var or `compile-incremental-q` arg) dispatches between old
  and new path. Existing `query_inc_test.clj` runs under both during
  parity bring-up.
- Cut over when all tests pass on the new path. Removing
  `IncrementalGenericJoin` is **Ask first** — a deliberate separate change.

## Code Style

One illustrative shape — base relations stay dumb, all analysis is data:

```kotlin
class BaseSource(
    override val id: NodeId,
    val relationId: RelationId,
) : Node {
    private val views = mutableMapOf<Perm, IndexedStreamImpl>()
    override val label = "base($relationId)"

    fun view(perm: Perm, fixedPrefix: Prefix): IndexedStream<*, *> =
        views.getOrPut(perm) { IndexedStreamImpl(this, perm, fixedPrefix) }

    // populated by the analysis pass
    internal val demandedPerms: Set<Perm> get() = views.keys
}
```

Guidelines:
- Graph construction (`CircuitBuilder`) and per-tick evaluation (`Circuit.step`)
  are separate concerns and live in separate files.
- External input feeding (`InputHandle`) and circuit clock advancement
  (`Circuit.step`) are separate concerns, even though v1 may provide
  `step(input)` as a wrapper.
- Permutation choice is data on `IndexSpec`, never branches inside operators.
- Physical index availability is data on `Relation`, not methods on generic
  `Stream`.
- Delayed/accumulated state lives in named `delay` / `integrate` nodes, not
  hidden inside join operators.
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
  `delay`/`integrate`/`differentiate` satisfy their stream contracts.
- **Analysis tests** (`src/test/kotlin/.../stream/analysis/`):
  given a fixture query graph, assert the demanded-permutation set per
  base, the relation capability choices, the expanded incremental WCOJ branch
  graph, the schedule's topo order, and rejection of illegal cycles.
- **Circuit-level tests** (Kotlin): tiny end-to-end queries built directly
  via `CircuitBuilder`, stepped tick by tick, compared against full-query
  results. Include tests for `input.set(delta); step()` and the compatibility
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
- Preserve signed Z-set weights, zero-weight cleanup, and the two-phase
  `eval`/`commit` discipline.
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
- Mutate accumulated state during `eval` (before all operators have seen the
  same z⁻¹).
- Build a circuit that decides demand at runtime — demand is fixed at
  `build()` time.
- Put physical index capability methods on generic `Stream`; use relation
  descriptors instead.
- Add separate external handles for EAV/AEV/AVE/VAE; the external input is one
  `InputHandle<ZSetIndices>`.
- Let `ZSetGenericJoin` perform permutation selection or canonicalization;
  inputs must already be arranged by the expansion phase.
- Commit or push without explicit user request.

## Success Criteria

1. `CircuitBuilder.build()` returns a `Circuit` whose schedule and
   per-base demanded-permutation set are deterministic and inspectable.
2. A query needing AVE and AEV views of the same base relation compiles
   without duplicating source state; both views share one `BaseSource` node.
3. The only external circuit input is `ZSetIndices`; base relation streams are
   views over that input.
4. The primary circuit runtime API separates input feeding from clock
   advancement via `InputHandle<ZSetIndices>` and `Circuit.step()`, with
   `Circuit.step(input)` retained as a migration wrapper.
5. `IncrementalWcojJoin` expands to branch-local `mapIndex`, `integrate`,
   `delay`, `ZSetGenericJoin`, and canonicalization nodes; it is not modeled as
   the `Circuit` itself.
6. A base-only first version works with base streams saving incoming deltas.
7. A second base-only version removes saved base deltas and uses timestamped
   `ZSet` views over base indexes instead.
8. A derived stream reindexed two different ways uses two explicit
   `MapIndex` operator nodes; the graph reflects the cost.
9. Every test currently passing under `org.hooray.incremental.*` passes
   when `query_inc_test.clj` runs through the stream pipeline.
10. Adding a real `TraceStream` implementation later requires no API change
   to operators that already consume `TraceStream` (`differentiate`, future
   bilinear `join_with_trace`).

## Open Questions

1. **Package/namespace naming.** Proposed: Kotlin `org.hooray.incremental.stream.*`,
   Clojure `hooray.incremental.stream`. OK, or prefer something else
   (`org.hooray.circuit`, `hooray.stream`)?
2. **Output cardinality.** v1: assume one `OutputHandle` per query (matches
   current `pop-result!`). Multi-output circuits possible later. Confirm
   single-output is enough for v1.
3. **Trace timing.** The current plan defers general traces until derived
   relations need them. Is any non-base derived relation required before the
   base-only stream circuit reaches parity?
4. **Migration switch mechanism.** Env var, Clojure compile-time flag, or
   a parameter on `query`? Preference?
5. **Cycle policy.** v1: only `delay`-closed cycles allowed. Is that
   enough for the planned recursive Datalog work, or do we need a more
   general feedback primitive sooner?
