package org.hooray.incremental.stream

import org.hooray.incremental.IntegerWeight
import org.hooray.incremental.ZSet

/**
 * Differentiate: emits the current accumulated value minus the value
 * held from the previous tick. Reversing `integrate` — for any delta
 * stream `s`, `differentiate(integrate(s)) == s` per tick.
 *
 * The held value starts empty, so at tick 0 the output equals the
 * input itself.
 */
class DifferentiateNode<T>(
    override val id: NodeId,
    override val label: String,
    val input: AccumulatedStream<T>
) : Node, DerivedNode<ZSet<T, IntegerWeight>> {
    private var held: ZSet<T, IntegerWeight> = ZSet.empty()
    private var pending: ZSet<T, IntegerWeight>? = null

    override val output: ZSetStream<T> = SimpleStream(this)

    fun eval(input: AccumulatedZSet<T>): ZSet<T, IntegerWeight> {
        pending = input.zset
        return input.zset.subtract(held)
    }

    fun commit() {
        val current = pending ?: return
        held = current
        pending = null
    }
}

fun <T> differentiate(
    id: NodeId,
    label: String,
    input: AccumulatedStream<T>
): ZSetStream<T> = DifferentiateNode(id, label, input).output
