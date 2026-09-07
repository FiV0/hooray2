package org.hooray.facts

import org.hooray.UniversalComparator

/**
 * A column of items: a flat array of objects ordered by [UniversalComparator].
 *
 * Adapts datatoad's `Terms = Vecs<Vec<u8>, Strides>` from byte strings to in-memory
 * objects. Kernels interact with items through index-based methods ([compareItems],
 * [itemsEqual], [extendFromRange], [pushItemFrom]); items are only handed out via [get].
 *
 * Item equality is `compareItems == 0`, not `equals` — deduplication must be consistent
 * with the sort order, and `UniversalComparator` treats numerically equal cross-type
 * Numbers (say `1L` and `1.0`) as one item, where `equals` would not. `null` is a legal
 * item and sorts before everything else. Types outside `UniversalComparator`'s set throw
 * `IllegalArgumentException` on first compare, and stored collections must not be mutated
 * after insertion: layers share items by reference.
 */
class Terms {
    private var array = arrayOfNulls<Any>(16)

    /** The number of items (Rust `Len::len`). */
    var size: Int = 0
        private set

    fun isEmpty(): Boolean = size == 0

    /** The item at `index`. */
    operator fun get(index: Int): Any? {
        require(index < size) { "index $index out of bounds ($size items)" }
        return array[index]
    }

    /** Appends one item (Rust `Push` / `push_literal`). */
    fun pushItem(value: Any?) {
        if (size == array.size) array = array.copyOf(maxOf(16, size * 2))
        array[size] = value
        size += 1
    }

    /** Appends item `index` of `other` (Rust `values.push(other.get(index))`). */
    fun pushItemFrom(other: Terms, index: Int) {
        pushItem(other.array[index])
    }

    /**
     * Appends items `[lower, upper)` of `other` (Rust `Vecs::extend_from_self` at the
     * values level): one bulk copy of references.
     */
    fun extendFromRange(other: Terms, lower: Int, upper: Int) {
        if (lower >= upper) return
        val count = upper - lower
        if (size + count > array.size) array = array.copyOf(maxOf(16, maxOf(size + count, size * 2)))
        System.arraycopy(other.array, lower, array, size, count)
        size += count
    }

    /** Compares item `i` with item `j` of `other` by [UniversalComparator]. */
    fun compareItems(i: Int, other: Terms, j: Int): Int =
        UniversalComparator.compare(array[i], other.array[j])

    fun itemsEqual(i: Int, other: Terms, j: Int): Boolean =
        compareItems(i, other, j) == 0

    fun clear() {
        java.util.Arrays.fill(array, 0, size, null)
        size = 0
    }
}
