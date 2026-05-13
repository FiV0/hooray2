package org.hooray.incremental.stream

import org.hooray.incremental.IntegerWeight
import org.hooray.incremental.ZSet

/**
 * One-tick buffer. `eval(x)` returns the value held at the start of
 * the tick (initialized to `zero`); `commit()` stores `x` as the new
 * held value for the next tick.
 *
 * Generic over P so the same node serves both `ZSetStream` (delta)
 * delay and `AccumulatedStream` delay — the type parameter records
 * which one a given instance is.
 */
class DelayNode<P>(
    override val id: NodeId,
    override val label: String,
    val input: Stream<P>,
    private val zero: P
) : Node, DerivedNode<P> {
    private var held: P = zero
    private var pending: P? = null

    override val output: Stream<P> = SimpleStream(this)

    fun eval(input: P): P {
        pending = input
        return held
    }

    fun commit() {
        val p = pending ?: return
        held = p
        pending = null
    }
}

/** Delay a delta stream by one tick. Tick 0 yields an empty ZSet. */
fun <T> delay(id: NodeId, label: String, input: ZSetStream<T>): ZSetStream<T> {
    val zero: ZSet<T, IntegerWeight> = ZSet.empty()
    return DelayNode(id, label, input, zero).output
}

/**
 * Delay an accumulated stream by one tick. Tick 0 yields an empty
 * accumulated state. Named `delayAccumulated` because Kotlin would
 * otherwise see this as a conflicting overload of `delay` once
 * typealiases are erased.
 */
fun <T> delayAccumulated(id: NodeId, label: String, input: AccumulatedStream<T>): AccumulatedStream<T> {
    val zero = AccumulatedZSet<T>(ZSet.empty())
    return DelayNode(id, label, input, zero).output
}
