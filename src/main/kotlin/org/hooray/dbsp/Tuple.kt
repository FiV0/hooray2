package org.hooray.dbsp

/**
 * A positional tuple of values, backed by an array.
 *
 * JVM arrays use identity `equals`/`hashCode`, so a raw `Array` cannot be used
 * as a map or [org.hooray.incremental.ZSet] key. `Tuple` wraps an array and
 * provides structural equality via `contentEquals` / `contentHashCode`, making
 * it safe to key Z-sets on.
 *
 * Tuples are immutable; every transformation returns a new `Tuple`.
 */
class Tuple(values: Array<Any?>) {
    private val values: Array<Any?> = values.copyOf()

    /** Number of columns in this tuple. */
    val arity: Int get() = values.size

    /** The value in column [index]. */
    operator fun get(index: Int): Any? = values[index]

    /**
     * Selects columns by [order]: the result has `order.size` columns with
     * `result[i] == this[order[i]]`. Handles both permutation (reordering all
     * columns) and projection (dropping or duplicating columns).
     */
    fun permute(order: IntArray): Tuple {
        val selected = arrayOfNulls<Any?>(order.size)
        for (i in order.indices) selected[i] = values[order[i]]
        return Tuple(selected)
    }

    /** The leading [k] columns — used as a join key. */
    fun prefix(k: Int): Tuple = Tuple(values.copyOfRange(0, k))

    /** All columns from [from] onwards. */
    fun drop(from: Int): Tuple = Tuple(values.copyOfRange(from, values.size))

    /** The column values as a list — convenient for converting results to Clojure. */
    fun toList(): List<Any?> = values.toList()

    /** This tuple's columns followed by [other]'s columns. */
    fun concat(other: Tuple): Tuple {
        val combined = arrayOfNulls<Any?>(values.size + other.values.size)
        System.arraycopy(values, 0, combined, 0, values.size)
        System.arraycopy(other.values, 0, combined, values.size, other.values.size)
        return Tuple(combined)
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is Tuple) return false
        return values.contentEquals(other.values)
    }

    override fun hashCode(): Int = values.contentHashCode()

    override fun toString(): String = values.joinToString(", ", "[", "]")

    companion object {
        /** The empty (zero-column) tuple — the join key for a Cartesian product. */
        @JvmField
        val EMPTY = Tuple(emptyArray())

        /** Builds a tuple from its column values. */
        @JvmStatic
        fun of(vararg values: Any?): Tuple = Tuple(arrayOf(*values))
    }
}
