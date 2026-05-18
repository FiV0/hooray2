package org.hooray.dbsp

/**
 * A typed handle to one operator's output within a [Circuit].
 *
 * Streams are produced by the circuit builder (`addInput`, `addUnary`,
 * `addBinary`) and passed back in to wire downstream operators. A stream
 * carries at most one value per circuit step.
 */
class Stream<D> internal constructor(internal val nodeId: Int)

/**
 * The external write end of a circuit input.
 *
 * Call [push] before a [Circuit.step] to set the value the corresponding
 * source emits in that step. If nothing is pushed, the source sees no input.
 */
class InputHandle<D> internal constructor() {
    private var pending: D? = null

    /** Sets the value the input source will emit on the next [Circuit.step]. */
    fun push(value: D) {
        pending = value
    }

    /** Consumes the pending value, clearing it; `null` if nothing was pushed. */
    internal fun poll(): D? {
        val value = pending
        pending = null
        return value
    }
}

/**
 * The external read end of a circuit output.
 *
 * After each [Circuit.step], [get] returns the value the wired stream produced
 * in that step.
 */
class OutputHandle<D> internal constructor() {
    private var current: D? = null
    private var stepped = false

    internal fun set(value: D) {
        current = value
        stepped = true
    }

    /** The output of the most recent [Circuit.step]. */
    @Suppress("UNCHECKED_CAST")
    fun get(): D {
        check(stepped) { "circuit has not been stepped yet" }
        return current as D
    }
}
