package org.hooray.incremental.stream

import org.hooray.incremental.IntegerWeight
import org.hooray.incremental.ZSet

/**
 * Graph node for the `project` operator. Each input tuple is passed
 * through `projection`; tuples that map to the same output value have
 * their weights summed, and zero-weight results are dropped.
 */
class ProjectNode<In, Out>(
    override val id: NodeId,
    override val label: String,
    val input: ZSetStream<In>,
    val projection: (In) -> Out
) : Node, DerivedNode<ZSet<Out, IntegerWeight>> {
    override val output: ZSetStream<Out> = SimpleStream(this)
}

fun <In, Out> project(
    id: NodeId,
    label: String,
    input: ZSetStream<In>,
    projection: (In) -> Out
): ZSetStream<Out> = ProjectNode(id, label, input, projection).output

/** Pure projection over a ZSet. Coalesces and drops zeros. */
fun <In, Out> computeProject(
    zset: ZSet<In, IntegerWeight>,
    projection: (In) -> Out
): ZSet<Out, IntegerWeight> {
    val accumulator = HashMap<Out, IntegerWeight>()
    for ((value, weight) in zset.entries()) {
        val projected = projection(value)
        accumulator.merge(projected, weight) { a, b ->
            val sum = a.add(b)
            if (sum.isZero()) null else sum
        }
    }
    return ZSet.fromMap(accumulator)
}
