package org.hooray.incremental.stream

import org.hooray.incremental.IntegerWeight
import org.hooray.incremental.ZSet

/**
 * Element-wise predicate over a ZSetStream. Surviving rows keep their
 * weights; rejected rows are dropped. Pure (no state across ticks).
 */
class FilterNode<T>(
    override val id: NodeId,
    override val label: String,
    val input: ZSetStream<T>,
    val predicate: (T) -> Boolean
) : Node, DerivedNode<ZSet<T, IntegerWeight>> {
    override val output: ZSetStream<T> = SimpleStream(this)
}

fun <T> filter(
    id: NodeId,
    label: String,
    input: ZSetStream<T>,
    predicate: (T) -> Boolean
): ZSetStream<T> = FilterNode(id, label, input, predicate).output

fun <T> computeFilter(
    zset: ZSet<T, IntegerWeight>,
    predicate: (T) -> Boolean
): ZSet<T, IntegerWeight> {
    val kept = LinkedHashMap<T, IntegerWeight>()
    for ((value, weight) in zset.entries()) {
        if (predicate(value)) kept[value] = weight
    }
    return ZSet.fromMap(kept)
}
