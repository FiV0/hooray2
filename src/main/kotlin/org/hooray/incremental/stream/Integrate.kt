package org.hooray.incremental.stream

import org.hooray.incremental.IntegerWeight
import org.hooray.incremental.ZSet

/**
 * Integrate (DBSP convention): at tick N, eval() exposes the cumulative
 * sum of inputs from ticks 0..N inclusive. The held state is still the
 * delayed integral (sum through tick N-1) so that the eval/commit split
 * stays consistent — eval reads the delayed state but emits delayed +
 * current. commit() folds the current input into the held state so the
 * next tick sees it as "previous".
 *
 * This convention is what makes `differentiate(integrate(s)) == s` hold.
 *
 * Output type AccumulatedStream<T> distinguishes accumulated state from
 * per-tick delta streams in the type system.
 */
class IntegrateNode<T>(
    override val id: NodeId,
    override val label: String,
    val input: ZSetStream<T>
) : Node, DerivedNode<AccumulatedZSet<T>> {
    private var accumulated: ZSet<T, IntegerWeight> = ZSet.empty()
    private var pending: ZSet<T, IntegerWeight>? = null

    override val output: AccumulatedStream<T> = SimpleStream(this)

    fun eval(input: ZSet<T, IntegerWeight>): AccumulatedZSet<T> {
        pending = input
        return AccumulatedZSet(accumulated.add(input))
    }

    fun commit() {
        val delta = pending ?: return
        accumulated = accumulated.add(delta)
        pending = null
    }
}

fun <T> integrate(id: NodeId, label: String, input: ZSetStream<T>): AccumulatedStream<T> =
    IntegrateNode(id, label, input).output
