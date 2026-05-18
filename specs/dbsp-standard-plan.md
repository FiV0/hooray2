# Implementation Plan: DBSP-Standard

Status: **Phase 2 (Plan)** — awaiting review before Tasks.
Companion to `specs/dbsp-standard.md`.

## Component inventory

| ID | Component | Language | Depends on |
|----|-----------|----------|-----------|
| **A** | Circuit framework — `Operator` traits, `Stream`, `InputHandle`, `OutputHandle`, `Circuit` builder + topological scheduler | Kotlin `org.hooray.dbsp` | existing `ZSet` algebra |
| **B1** | Linear/IO operators — `SourceOp`, `FilterOp`, `MapOp` | Kotlin | A |
| **B2** | Primitive operators — `IntegrateOp`, `DifferentiateOp`, `Z1Op`, `PlusOp`(±), `DistinctOp` | Kotlin | A |
| **B3** | Join operators — `StreamJoinOp` (non-incremental bilinear), `IncrementalJoinOp` (fused) | Kotlin | A, B2 |
| **C** | `Tuple` value type — array wrapper with structural `equals`/`hashCode` | Kotlin | — |
| **D** | Query analysis — pattern compile, join graph, left-deep order, join keys, permutation + intermediate-`Map` assignment | Clojure `hooray.dbsp` | existing `hooray.query` parser |
| **E** | Circuit assembly — plan → Kotlin `Circuit` via interop | Clojure `hooray.dbsp` | A, B1–B3, D |
| **F** | Delta path — `compute-delta!`: build per-pattern `AEV`/`AVE` deltas, push via `InputHandle`, `step`, read `OutputHandle`, format result | Clojure `hooray.dbsp` | E |
| **G** | Core integration — `*dbsp-version*` var, `q-inc` records version, `transact` dispatch | Clojure `hooray.core` | F |

Dependency graph (critical path in **bold**):

```
C ─┐
   ├─► A ─► B1 ─┐
   │      └► B2 ─► B3 ─┐
   │                   ├─► E ─► F ─► G
D ─────────────────────┘
```

## Implementation order (milestones)

**M1 — Circuit framework (A, C).**
Build `Tuple`, the `Operator`/`Stream` types, `InputHandle`/`OutputHandle`, and
`Circuit` with a Kahn-style topological scheduler. `Circuit` is immutable after
build; `step` evaluates each node once in dependency order.
→ Checkpoint **CP1**.

**M2 — Operators (B1, B2).**
`SourceOp`/`FilterOp`/`MapOp`, then the primitives `IntegrateOp`,
`DifferentiateOp`, `Z1Op`, `PlusOp`, `DistinctOp`. All independent once A exists
— parallelizable.
→ Checkpoint **CP2**.

**M3 — Joins + oracle (B3).**
`StreamJoinOp` first, then the fused `IncrementalJoinOp`. Build a *composed*
incremental join in test code from `IntegrateOp`/`Z1Op`/`StreamJoinOp`/`PlusOp`
and differential-test the fused operator against it.
→ Checkpoint **CP3**.

**M4 — Query analysis (D).** *Pure Clojure — can run in parallel with M1–M3.*
Pattern descriptors, join graph, deterministic left-deep order, per-step join
keys, per-pattern `AEV`/`AVE` assignment, intermediate-`Map` column orders.
The plan is a plain data structure (EDN), independently testable.
→ Checkpoint **CP4**.

**M5 — Assembly + delta path (E, F).**
Compile a plan into a Kotlin `Circuit` via interop; build per-pattern
`AEV`/`AVE` delta `ZSet`s from `tx-data` (adapt the retract/cardinality logic of
`hooray.incremental/calc-zset-indices`, but emit flat permuted `ZSet`s); wire
`compute-delta!`.
→ Checkpoint **CP5**.

**M6 — Core integration (G).**
Add `^:dynamic *dbsp-version*` (default `:wcoj`) to `hooray.core`; `q-inc`
records it on the `!inc-qs` entry; `transact` dispatches per query.
→ Checkpoint **CP6**.

## Verification checkpoints

| CP | Verifies | How |
|----|----------|-----|
| **CP1** | Framework builds an immutable DAG and steps in topological order | Kotlin unit: `source → map → output` and a diamond DAG; assert eval order + single eval/node |
| **CP2** | Each operator's semantics | Kotlin unit per operator; `D∘I = id` and `I∘D = id`; `Distinct` sign behaviour |
| **CP3** | Fused `IncrementalJoin` correctness incl. `Δa⋈Δb` cross term | Kotlin differential test: fused vs composed oracle over randomized delta sequences; empty-key (Cartesian) case |
| **CP4** | Analysis is deterministic and correct | Clojure unit: stable left-deep order; join keys; permutation + intermediate-`Map` assignment; single-pattern + Cartesian |
| **CP5** | A circuit assembled from a real plan produces correct deltas | Clojure: feed hand-traced queries to the circuit directly; assert deltas |
| **CP6** | Dispatch works and `:wcoj` is untouched | Full existing suite green with default var; `:standard` end-to-end suite; cross-engine multiset-equality test |

Final gate = Success Criteria 1–7 in `specs/dbsp-standard.md`.

## Risks & mitigations

1. **`IncrementalJoin` old/new-integral timing** *(highest)* — the bilinear
   formula is easy to get subtly wrong (which side delayed, cross term).
   *Mitigate:* the composed oracle (M3) + randomized differential tests; derive
   the formula in code comments; CP3 must pass before M5.
2. **Array tuple equality** — Kotlin/JVM arrays use identity `hashCode`, so a raw
   `Array<Any?>` cannot be a `ZSet` key. *Mitigate:* component **C** — a `Tuple`
   wrapper with `contentEquals`/`contentHashCode`, built first; never key a
   `ZSet` on a bare array.
3. **Clojure↔Kotlin interop** — passing predicates/projection fns into operator
   constructors; generics erasure. *Mitigate:* operator constructors take plain
   SAM/`java.util.function`-style interfaces; a tiny interop spike at the start
   of M5 before full assembly.
4. **`transact` dispatch regressing `:wcoj`** — shared-code edit. *Mitigate:*
   default `*dbsp-version*` `:wcoj`; the `:standard` branch is purely additive;
   CP6 runs the entire existing suite unchanged.
5. **Delta construction divergence** — flat permuted `AEV`/`AVE` deltas must
   agree with what the WCOJ path derives (retracts, cardinality, prev-value).
   *Mitigate:* reuse `calc-zset-indices` logic; the cross-engine multiset test
   at CP6 catches divergence.
6. **Topological scheduling with shared sources** — self-joins fan one
   attribute's facts into two `Source`s; the scheduler must handle a node with
   multiple consumers and multiple operators sharing an upstream. *Mitigate:*
   CP1 diamond-DAG test; self-join end-to-end case at CP6.
7. **Cartesian / empty join key** — join on `{}`. *Mitigate:* explicit empty-key
   case in CP3 and CP6.

## Parallelization

- **D (M4)** is pure Clojure and shares nothing with M1–M3 → build concurrently.
- Within **M2**, all six operators are mutually independent → concurrent.
- Critical path: **A → B2 → B3 → E → F → G**. M3 (joins) is the longest single
  link and gates M5.

## Out of scope (restated)

No `or`/`and`/`not`/predicates/functions/aggregates/pull/rules; no nested
circuits or feedback loops; no cost-based planning; no cross-query delta
sharing; no disk-backed traces.
