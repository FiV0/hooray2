package org.hooray.incremental

import org.hooray.algo.Extension

/**
 * Interface for Z-set-like structures that form a commutative group under addition.
 *
 * Uses F-bounded polymorphism to ensure operations return the correct concrete type.
 * This captures the common algebraic structure shared by both regular Z-sets and indexed Z-sets.
 *
 * @param K the type of keys in the Z-set
 * @param W the type of weights
 * @param T the concrete implementing type (e.g., ZSet or IndexedZSet)
 */
interface IZSet<K, W : Weight<W>, T : IZSet<K, W, T>> {
    /**
     * Check if this Z-set is empty (contains no elements with non-zero weight).
     */
    fun isEmpty(): Boolean

    /**
     * Get the size (number of elements/groups with non-zero weight).
     */
    val size: Int

    /**
     * Get all keys in this Z-set.
     */
    fun keys(): Set<K>

    /**
     * Get the weight of a key.
     * For ZSet, returns the actual weight of the key.
     * For IndexedZSet, returns ONE if the key is present, ZERO if not.
     */
    fun weight(key: K): W

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

    fun filterKeys(predicate: (T) -> Boolean): ZSet<Extension, IntegerWeight>
    fun containsKey(ext: Any): Boolean
}
