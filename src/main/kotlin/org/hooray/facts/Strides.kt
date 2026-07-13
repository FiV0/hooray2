package org.hooray.facts

/**
 * A store for non-decreasing offsets, optimized for evenly spaced offsets.
 *
 * Port of `columnar::primitive::offsets::Strides`. The first [length] offsets are
 * implicit, offset `i` being `(i + 1) * stride`; once a pushed offset breaks that pattern,
 * it and all subsequent offsets are stored explicitly in [spill]. Element `i` of the
 * container these offsets describe spans `[offset(i - 1), offset(i))`, with `offset(-1)`
 * taken as 0.
 *
 * The strided form is load-bearing beyond compression: a `Layer` whose lists all hold K
 * items keeps its bounds strided, which [strided] reports and `advanceBounds` uses to
 * skip per-list lookups.
 */
class Strides() {
    private var stride = 0
    private var length = 0
    private val spill = IntList(0)

    /** Mirrors `Strides::new(stride, length)`. */
    constructor(stride: Int, length: Int) : this() {
        this.stride = stride
        this.length = length
    }

    /** The number of offsets stored (Rust `Len::len`). */
    val size: Int get() = length + spill.size

    fun isEmpty(): Boolean = size == 0

    /** Mirrors `Strides::push`: the first push defines the stride, and spilling is sticky. */
    fun push(offset: Int) {
        if (length == 0) {
            stride = offset
            length = 1
        } else if (!spill.isEmpty()) {
            spill.push(offset)
        } else if (offset == stride * (length + 1)) {
            length += 1
        } else {
            spill.push(offset)
        }
    }

    /** Removes the last offset (Rust `Strides::pop`). */
    fun pop() {
        if (spill.isEmpty()) length -= 1 else spill.pop()
    }

    /** The `index`-th offset (Rust `Index::get`). */
    fun offset(index: Int): Int =
        if (index < length) (index + 1) * stride else spill[index - length]

    /** The lower bound of element `index`. */
    fun lower(index: Int): Int = if (index == 0) 0 else offset(index - 1)

    /** The upper bound of element `index`. */
    fun upper(index: Int): Int = offset(index)

    /** Both bounds of element `index`, packed as `(lower shl 32) or upper`. */
    fun bounds(index: Int): Long = (lower(index).toLong() shl 32) or upper(index).toLong()

    /** The common stride while no offset has spilled (Rust `strided()`), or -1 once spilled. */
    fun strided(): Int = if (spill.isEmpty()) stride else -1

    /** The final offset, i.e. the total count of underlying values; 0 when empty. */
    fun last(): Int = if (size == 0) 0 else offset(size - 1)

    fun clear() {
        stride = 0
        length = 0
        spill.clear()
    }

    companion object {
        fun boundsLower(bounds: Long): Int = (bounds ushr 32).toInt()
        fun boundsUpper(bounds: Long): Int = bounds.toInt()
    }
}
