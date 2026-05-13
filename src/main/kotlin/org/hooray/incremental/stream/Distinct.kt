package org.hooray.incremental.stream

import org.hooray.incremental.IntegerWeight
import org.hooray.incremental.ZSet

/**
 * Stream-level Distinct operator. Mirrors the eval/commit semantics of
 * `org.hooray.incremental.IncrementalDistinct`: the per-tick output is
 * the delta of "is the accumulated weight strictly positive?".
 *
 * Phase 2 places this with the stateless operators for ordering, but
 * Distinct is genuinely stateful — Phase 4 will likely extract a common
 * StatefulNode abstraction; until then, the eval/commit pair lives on
 * this node directly.
 */
class DistinctNode<T>(
    override val id: NodeId,
    override val label: String,
    val input: ZSetStream<T>
) : Node, DerivedNode<ZSet<T, IntegerWeight>> {
    private val state: MutableMap<T, Int> = mutableMapOf()
    private var pending: ZSet<T, IntegerWeight>? = null

    override val output: ZSetStream<T> = SimpleStream(this)

    fun eval(input: ZSet<T, IntegerWeight>): ZSet<T, IntegerWeight> {
        pending = input
        if (input.isEmpty()) return ZSet.empty()

        val outputMap = HashMap<T, IntegerWeight>()
        for ((tuple, weight) in input.entries()) {
            val old = state[tuple] ?: 0
            val new = old + weight.value
            when {
                old <= 0 && new > 0 -> outputMap[tuple] = IntegerWeight.ONE
                old > 0 && new <= 0 -> outputMap[tuple] = IntegerWeight.MINUS_ONE
            }
        }
        return ZSet.fromMap(outputMap)
    }

    fun commit() {
        val delta = pending ?: return
        for ((tuple, weight) in delta.entries()) {
            val new = (state[tuple] ?: 0) + weight.value
            if (new == 0) state.remove(tuple) else state[tuple] = new
        }
        pending = null
    }
}

fun <T> distinct(id: NodeId, label: String, input: ZSetStream<T>): ZSetStream<T> =
    DistinctNode(id, label, input).output
