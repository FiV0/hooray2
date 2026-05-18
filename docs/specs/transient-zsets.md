# Spec: Transient ZSet Construction

## Assumptions

1. The immediate performance problem is initial incremental pipeline setup in `compile-incremental-q`, specifically `db->zset-indices`.
2. The runtime output must remain immutable `ZSet` and `IndexedZSet` values; transients are only a construction-time optimization.
3. The public Clojure shape should be idiomatic: `(transient zset)`, `assoc!`, `dissoc!`, `conj!`, `get`, and `(persistent! transient-zset)` should work.
4. We should not add a new dependency or switch the storage model away from Kotlin/Clojure standard collection interfaces.
5. The typo in the request refers to `IndexedZSet.kt`.

## Objective

Speed up construction of incremental query indices by adding transient Clojure-compatible builders for `ZSet` and `IndexedZSet`.

Today, `src/main/clojure/hooray/incremental.clj` builds initial AEV and AVE indices by reducing triples through persistent-map style `assoc` and nested `update-in`. Each triple creates fresh `ZSet` / `IndexedZSet` nodes along the updated path. The new behavior should mutate transient builder nodes during setup, then freeze exactly once into the existing immutable Kotlin Z-set types before handing indices to `ZSetIndices` and `IncrementalPipeline.step`.

## Tech Stack

- Kotlin JVM 2.1.20
- Clojure 1.12.3
- Java 17 toolchain
- Gradle with Clojurephant
- Existing `org.hooray.incremental.ZSet`, `IndexedZSet`, `IntegerWeight`, and `IZSet` model

## Commands

- Build and full test suite: `./gradlew test`
- Kotlin compile check: `./gradlew compileKotlin compileTestKotlin`
- Clojure compile check: `./gradlew compileClojure compileTestClojure`
- Targeted Kotlin tests: `./gradlew test --tests org.hooray.incremental.ZSetTest --tests org.hooray.incremental.IndexedZSetTest`
- Targeted Clojure behavior tests: `./gradlew test --tests 'hooray.incremental_test*'`

## Project Structure

- `src/main/kotlin/org/hooray/incremental/ZSet.kt`: immutable `ZSet`; add `IEditableCollection` support and transient implementation.
- `src/main/kotlin/org/hooray/incremental/IndexedZSet.kt`: immutable `IndexedZSet`; add `IEditableCollection` support and transient implementation.
- `src/main/clojure/hooray/zset.clj`: Clojure-facing convenience helpers if needed.
- `src/main/clojure/hooray/incremental.clj`: switch initial and delta index construction to transient mutation where appropriate.
- `src/test/kotlin/org/hooray/incremental/ZSetTest.kt`: transient API tests for `ZSet`.
- `src/test/kotlin/org/hooray/incremental/IndexedZSetTest.kt`: transient API tests for `IndexedZSet`, including recursive persistence.
- `src/test/clojure/hooray/incremental_test.clj`: regression tests that transient construction yields the same AEV/AVE outputs.

## Code Style

Keep the immutable classes as the primary public model. Put transient code close to the immutable type it builds, and use Clojure interface names directly.

```kotlin
class ZSet<K, W : Weight<W>> private constructor(
    private val data: Map<K, W>,
    private val zero: W
) : IZSet<K, W, ZSet<K, W>>, IPersistentMap, IEditableCollection {
    override fun asTransient(): ITransientCollection =
        TransientZSet(data, zero)
}
```

```clojure
(defn- assoc-weight! [zset k weight]
  (assoc! zset k weight))
```

Prefer small private helpers in `incremental.clj` over reusing persistent `update-in` for transient mutation. Keep casts explicit in Kotlin where Clojure interop requires them.

## Design

`ZSet` and `IndexedZSet` should implement `clojure.lang.IEditableCollection`. Calling Clojure `transient` on either type should return an object implementing the Clojure transient map interfaces.

Because `clojure.lang.ATransientMap` has package-private abstract hooks in `clojure.lang`, Kotlin classes in `org.hooray.incremental` should implement the public interfaces directly rather than extending `ATransientMap`:

- `ITransientMap`
- `ITransientAssociative`
- `ITransientCollection`
- `ILookup`
- `Counted`

The transient objects should keep mutable maps internally, guard against use after `persistent!`, and return themselves from mutating operations, matching Clojure transient style.

Required operations:

- `assoc`: mutate the key to value mapping and return the same transient object.
- `without`: remove a key and return the same transient object.
- `conj`: accept `Map.Entry`, 2-element `IPersistentVector`, or a seq of map entries.
- `valAt`: support Clojure `get`.
- `count`: support Clojure `count`.
- `persistent`: freeze once into `ZSet` or `IndexedZSet`.

`IndexedZSet` persistence must recursively freeze nested transient children before constructing the immutable `IndexedZSet`, so nested construction can use transient nodes at multiple levels.

`incremental.clj` should add transient index-construction helpers for AEV and AVE paths. Both `db->zset-indices` and `calc-zset-indices` should move to the transient construction path immediately. `db->zset-indices` is the highest-impact path because it runs during initial pipeline setup over every existing triple, but using the same construction path for transaction deltas keeps the behavior uniform.

`persistent!` should not be responsible for filtering zero weights from `ZSet`. Instead, zero-weight cleanup belongs at the mutation points that add or retract data directly from `ZSet` / `IndexedZSet`. When a direct transient update combines weights and the result is zero, it should remove that leaf entry. When a nested `IndexedZSet` child becomes empty, the parent path should be removed.

## Testing Strategy

Add Kotlin tests proving Clojure-transient interface behavior:

- `(transient zset)` equivalent via `.asTransient()` can `assoc`, `without`, `conj`, `valAt`, `count`, and `persistent`.
- Mutating a transient after `persistent` throws.
- `persistent` returns immutable `ZSet` / `IndexedZSet` instances with the same equality and lookup behavior as existing persistent construction.
- Nested `IndexedZSet` transients recursively persist child transients.
- Empty nested IZSets are still filtered from `IndexedZSet`.

Add Clojure regression tests proving index construction behavior:

- `db->zset-indices` produces the same AEV and AVE paths as the current persistent reduction.
- Existing `calc-zset-indices-*` tests continue to pass.
- An initial database with many triples can initialize an incremental query without building through persistent `index-triple`.

No benchmark or checked performance target is required for this change. The verification target is structural and behavioral: the construction path should use transients and the existing tests should keep passing.

## Boundaries

- Always: Preserve immutable `ZSet` and `IndexedZSet` as the values passed into the incremental pipeline.
- Always: Preserve existing AEV/AVE index semantics and query results.
- Always: Test both Kotlin interop and Clojure `transient` / `assoc!` / `persistent!` behavior.
- Always: Remove zero-weight leaves when add/retract mutation logic directly updates a `ZSet` or nested `IndexedZSet`.
- Always: Leave `persistent!` as a freeze operation, not a zero-filtering operation.
- Ask first: Changing zero-weight storage semantics for ordinary persistent `assoc`.
- Ask first: Adding benchmark libraries or dependencies.
- Ask first: Replacing `ZSet` / `IndexedZSet` construction with a non-Clojure builder API instead of transient interfaces.
- Never: Commit or push without explicit user request.
- Never: Make the incremental pipeline consume mutable transient data.

## Success Criteria

1. `ZSet` and `IndexedZSet` can be passed to Clojure `transient`, mutated with transient map operations, and finalized with `persistent!`.
2. `db->zset-indices` no longer reduces the initial database through per-triple persistent `index-triple` updates.
3. `calc-zset-indices` also uses the transient construction path for transaction deltas.
4. Initial incremental query setup still calls `.step` with immutable `ZSetIndices`.
5. Existing incremental query tests and Z-set algebra tests pass.
6. Tests cover use-after-persistent failure and recursive persistence of nested indexed Z-set transients.
7. No new dependencies are introduced.

## Decisions

1. Move `calc-zset-indices` to transients immediately, not only initial setup.
2. Do not remove zeros in `persistent!`; remove zeros when add/retract mutation logic directly updates the Z-set structures.
3. Do not add a benchmark or checked performance target in this change.
