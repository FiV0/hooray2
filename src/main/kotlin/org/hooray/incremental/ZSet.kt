package org.hooray.incremental

import org.hooray.algo.Extension
import org.hooray.algo.Prefix

/**
 * A Z-set is a collection where each value has an associated integer weight.
 * Values with zero weight are not stored (invariant).
 * Z-sets form a commutative group under addition.
 *
 * @param K the type of values in the Z-set
 * @param W the type of weights
 */
class ZSet<K, W : Weight<W>> private constructor(
    private val data: Map<K, W>,
    private val zero: W
) : IZSet<K, W, ZSet<K, W>> {
    /**
     * Get the weight of a value. Returns zero if the value is not present.
     */
    override fun weight(key: K): W {
        return data[key] ?: zero
    }

    /**
     * Get the depth of this Z-set. Always returns 1 for ZSet.
     */
    override fun depth(): Int {
        return 1
    }

    /**
     * Get all entries (value-weight pairs) in this Z-set.
     */
    fun entries(): Set<Map.Entry<K, W>> {
        return data.entries
    }

    /**
     * Get all values in this Z-set.
     */
    override fun keys(): Set<K> {
        return data.keys
    }

    /**
     * Check if this Z-set is empty.
     */
    override fun isEmpty(): Boolean {
        return data.isEmpty()
    }

    /**
     * Get the size (number of non-zero weighted values) of this Z-set.
     */
    override val size: Int
        get() = data.size

    /**
     * Add this Z-set to another Z-set.
     * Weights of matching values are combined.
     */
    override fun add(other: ZSet<K, W>): ZSet<K, W> {
        val result = mutableMapOf<K, W>()

        // Add entries from this Z-set
        for ((key, weight) in data) {
            result[key] = weight
        }

        // Merge entries from the other Z-set
        for ((key, weight) in other.data) {
            result.merge(key, weight) { w1, w2 ->
                val sum = w1.add(w2)
                if (sum.isZero()) null else sum
            }
        }

        return ZSet(result, zero)
    }

    /**
     * Operator overloading for addition.
     * Allows using `zset1 + zset2` instead of `zset1.add(zset2)`.
     */
    operator fun plus(other: ZSet<K, W>): ZSet<K, W> = add(other)

    /**
     * Negate this Z-set (invert all weights).
     */
    override fun negate(): ZSet<K, W> {
        val result = data.mapValues { (_, weight) -> weight.negate() }
        return ZSet(result, zero)
    }

    /**
     * Subtract another Z-set from this Z-set.
     */
    override fun subtract(other: ZSet<K, W>): ZSet<K, W> {
        return add(other.negate())
    }

    override fun filterKeys(predicate: (ZSet<K, W>) -> Boolean): ZSet<Extension, IntegerWeight> {
        TODO("Not yet implemented")
    }

    override fun containsKey(ext: Any): Boolean {
        TODO("Not yet implemented")
    }

    /**
     * Filter to only positive weights.
     */
    fun positive(): ZSet<K, W> {
        val result = data.filterValues { weight ->
            !weight.isZero() && (weight as? IntegerWeight)?.value?.let { it > 0 } ?: true
        }
        return ZSet(result, zero)
    }

    /**
     * Create a distinct set (all weights = 1).
     * Only includes values that have non-zero weight.
     */
    fun distinct(): ZSet<K, IntegerWeight> {
        val result = data.keys.associateWith { IntegerWeight.ONE }
        return ZSet(result, IntegerWeight.ZERO)
    }

    /**
     * Multiply all weights by a scalar.
     */
    fun multiply(w: W): ZSet<K, W> {
        if (w.isZero()) {
            return ZSet(emptyMap(), zero)
        }
        val result = data.mapValues { (_, weight ) -> weight.multiply(w) }
        return ZSet(result, zero)
    }

    /**
     * Multiply this Z-set with another Z-set using Cartesian product.
     * For each pair of entries from both Z-sets, combines the values using the provided function
     * and multiplies their weights.
     */
    fun <K2, R> multiply(other: ZSet<K2, W>, combineFunc: (K, K2) -> R): ZSet<R, W> {
        val result = mutableMapOf<R, W>()

        for ((leftValue, leftWeight) in data) {
            for ((rightValue, rightWeight) in other.data) {
                val combined = combineFunc(leftValue, rightValue)
                val multipliedWeight = leftWeight.multiply(rightWeight)

                result.merge(combined, multipliedWeight) { w1, w2 ->
                    val sum = w1.add(w2)
                    if (sum.isZero()) null else sum
                }
            }
        }

        return ZSet(result, zero)
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is ZSet<*, *>) return false
        return data == other.data
    }

    override fun hashCode(): Int {
        return data.hashCode()
    }

    override fun toString(): String {
        return "ZSet(${data.entries.joinToString(", ") { "${it.key}: ${it.value}" }})"
    }

    fun naturalJoin(other: ZSet<K, W>): ZSet<K, W> {
        val (smaller, larger) = if (this.size <= other.size) this to other else other to this
        val result = mutableMapOf<K, W>()
        for ((key, weight) in smaller.data) {
            val otherWeight = larger.data[key]
            if (otherWeight != null) {
                val combinedWeight = weight.multiply(otherWeight)
                if (!combinedWeight.isZero()) {
                    result[key] = combinedWeight
                }
            }
        }
        return ZSet(result, zero)
    }

    companion object {
        /**
         * Create an empty Z-set with integer weights.
         */
        fun <K> empty(): ZSet<K, IntegerWeight> {
            return ZSet(emptyMap(), IntegerWeight.ZERO)
        }

        /**
         * Create an empty Z-set with a specified zero weight.
         */
        fun <K, W : Weight<W>> empty(zero: W): ZSet<K, W> {
            return ZSet(emptyMap(), zero)
        }

        /**
         * Create a Z-set from a collection, assigning weight 1 to each value.
         */
        fun <K> fromCollection(collection: Collection<K>): ZSet<K, IntegerWeight> {
            // Count occurrences to handle duplicates
            val counts = mutableMapOf<K, Int>()
            for (item in collection) {
                counts[item] = (counts[item] ?: 0) + 1
            }

            val data = counts.mapValues { (_, count) -> IntegerWeight(count) }
            return ZSet(data, IntegerWeight.ZERO)
        }

        /**
         * Create a Z-set from a map of values to weights.
         * Zero-weighted entries are automatically filtered out.
         */
        fun <K> fromMap(map: Map<K, IntegerWeight>): ZSet<K, IntegerWeight> {
            val data = map.filterValues { !it.isZero() }
            return ZSet(data, IntegerWeight.ZERO)
        }

        /**
         * Create a Z-set from a map of values to weights with a specified zero weight.
         * Zero-weighted entries are automatically filtered out.
         */
        fun <K, W : Weight<W>> fromMap(map: Map<K, W>, zero: W): ZSet<K, W> {
            val data = map.filterValues { !it.isZero() }
            return ZSet(data, zero)
        }

        /**
         * Create a Z-set with a single value and weight.
         */
        fun <K> singleton(key: K, weight: IntegerWeight = IntegerWeight.ONE): ZSet<K, IntegerWeight> {
            if (weight.isZero()) {
                return empty()
            }
            return ZSet(mapOf(key to weight), IntegerWeight.ZERO)
        }
    }
}