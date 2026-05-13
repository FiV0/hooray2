package org.hooray.incremental.stream

import kotlinx.collections.immutable.persistentListOf
import org.hooray.algo.ResultTuple
import org.hooray.incremental.IntegerWeight
import org.hooray.incremental.ResultZSet
import org.hooray.incremental.ZSet
import org.hooray.incremental.ZSetPrefixExtender

/**
 * Stream-level WCOJ kernel. Inputs are already-arranged IndexedZSet
 * streams paired with the canonical-tuple positions each participates
 * in. This operator does **not** select base indexes, pick delta terms,
 * compute variable orders, or undo permutations — those are graph-
 * construction responsibilities of `IncrementalWcojJoin` and the
 * Clojure analysis phase.
 */
class ZSetGenericJoinNode(
    override val id: NodeId,
    override val label: String,
    val inputs: List<IndexedZSetStream<*>>,
    val participatingLevelsPerInput: List<List<Int>>,
    val levels: Int
) : Node, DerivedNode<ResultZSet> {
    init {
        require(inputs.size == participatingLevelsPerInput.size) {
            "Each input must have its own participatingLevels list"
        }
    }

    override val output: ZSetStream<ResultTuple> = SimpleStream(this)
}

fun zSetGenericJoin(
    id: NodeId,
    label: String,
    inputs: List<IndexedZSetStream<*>>,
    participatingLevelsPerInput: List<List<Int>>,
    levels: Int
): ZSetStream<ResultTuple> =
    ZSetGenericJoinNode(id, label, inputs, participatingLevelsPerInput, levels).output

/**
 * Pure WCOJ kernel — accepts pre-built extenders and the canonical
 * tuple length. Direct extraction of the join loop in
 * `IncrementalWcojJoinEngine.zSetGenericJoin`; identical semantics.
 */
fun computeZSetGenericJoin(
    extenders: List<ZSetPrefixExtender>,
    levels: Int
): ResultZSet {
    var prefixes = ZSet.singleton<ResultTuple>(persistentListOf())

    for (level in 0 until levels) {
        val participating = extenders.filter { it.participatesInLevel(level) }
        require(participating.isNotEmpty()) {
            "No extenders participate in level $level, cannot perform join"
        }

        var nextPrefixes = ZSet.empty<ResultTuple>()
        for ((prefix, prefixWeight) in prefixes.entries()) {
            val minIndex = participating.indices.minBy { participating[it].count(prefix) }
            var extensions = participating[minIndex].propose(prefix)
            for (i in participating.indices) {
                if (i != minIndex) {
                    extensions = participating[i].intersect(prefix, extensions)
                }
            }
            for ((extension, extensionWeight) in extensions.entries()) {
                val nextPrefix = prefix + extension
                val weight = prefixWeight.multiply(extensionWeight)
                nextPrefixes = nextPrefixes.add(ZSet.singleton(nextPrefix, weight))
            }
        }
        prefixes = nextPrefixes
        if (prefixes.isEmpty()) break
    }
    return prefixes
}
