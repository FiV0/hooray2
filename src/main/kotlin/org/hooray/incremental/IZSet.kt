package org.hooray.incremental

/**
 * Interface for Z-set-like structures that form a commutative group under addition.
 *
 * Uses F-bounded polymorphism to ensure operations return the correct concrete type.
 * This captures the common algebraic structure shared by both regular Z-sets and indexed Z-sets.
 *
 * @param T the concrete implementing type (e.g., ZSet or IndexedZSet)
 */
interface IZSet<T : IZSet<T>> {
    /**
     * Check if this Z-set is empty (contains no elements with non-zero weight).
     */
    fun isEmpty(): Boolean

    /**
     * Get the size (number of elements/groups with non-zero weight).
     */
    fun size(): Int

    /**
     * Add this Z-set to another Z-set.
     * This operation is commutative and associative, forming a commutative group.
     */
    fun add(other: T): T

    /**
     * Negate this Z-set (invert all weights).
     */
    fun negate(): T

    /**
     * Subtract another Z-set from this one.
     * Typically implemented as add(other.negate()).
     */
    fun subtract(other: T): T
}
