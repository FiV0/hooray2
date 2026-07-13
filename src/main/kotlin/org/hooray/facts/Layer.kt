package org.hooray.facts

/**
 * One trie layer: a sequence of lists of sorted, distinct items.
 *
 * Mirrors datatoad's `Layer<Terms>` (a `Lists<Terms> = Vecs<Terms, Strides>`), collapsed
 * into one class: [bounds] partitions the items of [values] into lists, where list `j`
 * holds the extensions of item `j` of the preceding layer. Layers are frozen by
 * convention once a kernel has produced them, and shared by reference where datatoad
 * shares `Rc<Layer>`.
 */
class Layer(val bounds: Strides = Strides(), val values: Terms = Terms()) {

    /** The number of lists (Rust `Lists::len`). */
    val size: Int get() = bounds.size

    /** The number of items across all lists. */
    val itemCount: Int get() = values.size

    fun isEmpty(): Boolean = size == 0

    fun listLower(i: Int): Int = bounds.lower(i)

    fun listUpper(i: Int): Int = bounds.upper(i)

    fun listBounds(i: Int): Long = bounds.bounds(i)

    /**
     * Appends lists `[lower, upper)` of `other` (Rust `Vecs::extend_from_self` at the
     * outer level): bulk-copies their items, then pushes the shifted list bounds.
     */
    fun extendFromSelf(other: Layer, lower: Int, upper: Int) {
        if (lower >= upper) return
        val itemBase = values.size
        val otherLower = if (lower == 0) 0 else other.bounds.offset(lower - 1)
        val otherUpper = other.bounds.offset(upper - 1)
        values.extendFromRange(other.values, otherLower, otherUpper)
        for (index in lower until upper) {
            bounds.push(other.bounds.offset(index) - otherLower + itemBase)
        }
    }
}
