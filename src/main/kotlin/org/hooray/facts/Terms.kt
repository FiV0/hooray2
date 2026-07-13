package org.hooray.facts

/**
 * A column of byte-string items: flat bytes plus item end-offsets.
 *
 * Mirrors datatoad's `Terms = Vecs<Vec<u8>, Strides>`. Item `i` spans
 * `data[bounds.lower(i) until bounds.upper(i)]`. Kernels interact with items through
 * index-based methods ([compareItems], [itemsEqual], [extendFromRange], [pushItemFrom]);
 * no per-item object is materialized outside of [itemToByteArray].
 */
class Terms {
    internal val data = ByteList()
    val bounds = Strides()

    /** The number of items (Rust `Len::len`). */
    val size: Int get() = bounds.size

    fun isEmpty(): Boolean = size == 0

    /** Total bytes across all items. */
    fun byteSize(): Int = data.size

    /** Appends one item (Rust `Push` / `push_literal`). The empty item is accepted. */
    fun pushItem(bytes: ByteArray, from: Int = 0, until: Int = bytes.size) {
        data.extendFrom(bytes, from, until)
        bounds.push(data.size)
    }

    /** Appends item `index` of `other` (Rust `values.push(other.get(index))`). */
    fun pushItemFrom(other: Terms, index: Int) {
        data.extendFrom(other.data.array, other.bounds.lower(index), other.bounds.upper(index))
        bounds.push(data.size)
    }

    /** Appends a four-byte big-endian item; the width-4 kernels' item writer. */
    internal fun pushU32BE(value: Int) {
        data.push((value ushr 24).toByte())
        data.push((value ushr 16).toByte())
        data.push((value ushr 8).toByte())
        data.push(value.toByte())
        bounds.push(data.size)
    }

    /**
     * Appends items `[lower, upper)` of `other` (Rust `Vecs::extend_from_self` at the
     * values level): one bulk byte copy, then the shifted item bounds. `Strides.push`
     * re-detects stride patterns, so fixed-width ranges keep the destination strided.
     */
    fun extendFromRange(other: Terms, lower: Int, upper: Int) {
        if (lower >= upper) return
        val byteBase = data.size
        val srcLower = if (lower == 0) 0 else other.bounds.offset(lower - 1)
        val srcUpper = other.bounds.offset(upper - 1)
        data.extendFrom(other.data.array, srcLower, srcUpper)
        for (index in lower until upper) {
            bounds.push(other.bounds.offset(index) - srcLower + byteBase)
        }
    }

    /**
     * Compares item `i` with item `j` of `other` as Rust `&[u8]: Ord` does: lexicographic
     * over unsigned bytes, with a shared prefix ordered by length.
     */
    fun compareItems(i: Int, other: Terms, j: Int): Int =
        java.util.Arrays.compareUnsigned(
            data.array, bounds.lower(i), bounds.upper(i),
            other.data.array, other.bounds.lower(j), other.bounds.upper(j),
        )

    fun itemsEqual(i: Int, other: Terms, j: Int): Boolean =
        java.util.Arrays.equals(
            data.array, bounds.lower(i), bounds.upper(i),
            other.data.array, other.bounds.lower(j), other.bounds.upper(j),
        )

    /** Copies item `i` out as a fresh array. For tests and debugging, not kernels. */
    fun itemToByteArray(i: Int): ByteArray =
        data.array.copyOfRange(bounds.lower(i), bounds.upper(i))

    fun clear() {
        data.clear()
        bounds.clear()
    }
}
