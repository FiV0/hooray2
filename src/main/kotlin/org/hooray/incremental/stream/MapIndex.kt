package org.hooray.incremental.stream

import org.hooray.incremental.IndexedZSet
import org.hooray.incremental.IntegerWeight
import org.hooray.incremental.ZSet
import org.hooray.incremental.index

/**
 * Graph node for the `mapIndex` operator. Takes a `ZSetStream<T>` (input
 * tuples) and emits an `IndexedZSetStream<K>` whose entries are grouped
 * by the positions listed in `spec.keyLevels`.
 *
 * The phantom `V` parameter records what the inner ZSet's value type
 * represents in the type system; v1 keeps the full tuple as the inner
 * value (matching `ZSet.index`'s convention), so V should usually be
 * `T` or `List<Any>`. A future projection-aware MapIndex can shrink
 * the inner ZSet by `spec.valueLevels`.
 */
class MapIndexNode<T, K, V>(
    override val id: NodeId,
    override val label: String,
    val input: ZSetStream<T>,
    val spec: IndexSpec<T, K, V>
) : Node, DerivedNode<IndexedZSet<K, IntegerWeight>> {
    override val output: Stream<IndexedZSet<K, IntegerWeight>> = SimpleStream(this)
}

/**
 * Graph builder. Wires a `MapIndexNode` between `input` and a fresh
 * `IndexedZSetStream<K>` output.
 */
fun <T, K, V> mapIndex(
    id: NodeId,
    label: String,
    input: ZSetStream<T>,
    spec: IndexSpec<T, K, V>
): IndexedZSetStream<K> = MapIndexNode(id, label, input, spec).output

/**
 * Pure computation behind `mapIndex` for tuples represented as
 * `List<Any>`. Filters by `fixedPrefix`, then groups remaining tuples
 * by the key extracted from `keyLevels`. Each group's inner ZSet
 * holds the full original tuples with their weights.
 */
fun computeMapIndex(
    zset: ZSet<List<Any>, IntegerWeight>,
    spec: IndexSpec<List<Any>, *, *>
): IndexedZSet<Any, IntegerWeight> {
    require(spec.keyLevels.isNotEmpty()) { "IndexSpec.keyLevels must not be empty" }

    val extractKey: (List<Any>) -> Any = when (spec.keyLevels.size) {
        1 -> { tuple -> tuple[spec.keyLevels[0]] }
        else -> { tuple -> spec.keyLevels.map { tuple[it] } }
    }

    val prefix = spec.fixedPrefix
    val filteredMap = LinkedHashMap<List<Any>, IntegerWeight>()
    for ((tuple, weight) in zset.entries()) {
        if (prefix.isNotEmpty()) {
            var matches = true
            for (i in prefix.indices) {
                if (i >= tuple.size || tuple[i] != prefix[i]) {
                    matches = false
                    break
                }
            }
            if (!matches) continue
        }
        filteredMap[tuple] = weight
    }
    val filtered = ZSet.fromMap(filteredMap)
    return filtered.index(extractKey)
}
