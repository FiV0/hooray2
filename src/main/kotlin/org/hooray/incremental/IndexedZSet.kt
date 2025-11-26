package org.hooray.incremental

/**
 * An indexed Z-set is a nested relation - a map where each key is associated with a Z-set.
 * It represents grouped data with weights, forming a commutative group under addition.
 *
 * Invariant: Empty ZSets are never stored in the data map.
 *
 * @param K the type of keys used for indexing
 * @param V the type of values in the nested Z-sets
 * @param W the type of weights
 */
class IndexedZSet<K, V, W : Weight<W>> private constructor(
    private val data: Map<K, ZSet<V, W>>,
    private val zero: W
) {
    /**
     * Get the Z-set associated with a key.
     * Returns null if the key is not present.
     */
    fun get(key: K): ZSet<V, W>? {
        return data[key]
    }

    /**
     * Get all keys in this indexed Z-set.
     */
    fun keys(): Set<K> {
        return data.keys
    }

    /**
     * Get all entries (key-ZSet pairs) in this indexed Z-set.
     */
    fun entries(): Set<Map.Entry<K, ZSet<V, W>>> {
        return data.entries
    }

    /**
     * Check if this indexed Z-set is empty.
     */
    fun isEmpty(): Boolean {
        return data.isEmpty()
    }

    /**
     * Get the number of groups (keys) in this indexed Z-set.
     */
    fun size(): Int {
        return data.size
    }

    /**
     * Get the number of groups (alias for size()).
     */
    fun groupCount(): Int {
        return size()
    }

    /**
     * Flatten this indexed Z-set into a regular Z-set by combining keys and values.
     */
    fun <R> flatten(combineFunc: (K, V) -> R): ZSet<R, W> {
        val result = mutableMapOf<R, W>()

        for ((key, zset) in data) {
            for ((value, weight) in zset.entries()) {
                val combined = combineFunc(key, value)
                result.merge(combined, weight) { w1, w2 ->
                    val sum = w1.add(w2)
                    if (sum.isZero()) null else sum
                }
            }
        }

        return ZSet.fromMap(result, zero)
    }

    /**
     * Deindex this indexed Z-set, concatenating all values from all groups.
     * This is a convenience method that discards keys.
     */
    fun deindex(): ZSet<V, W> {
        return flatten { _, v -> v }
    }

    /**
     * Add this indexed Z-set to another indexed Z-set.
     * Groups with matching keys have their Z-sets combined.
     */
    fun add(other: IndexedZSet<K, V, W>): IndexedZSet<K, V, W> {
        val result = mutableMapOf<K, ZSet<V, W>>()

        // Add entries from this indexed Z-set
        for ((key, zset) in data) {
            result[key] = zset
        }

        // Merge entries from the other indexed Z-set
        for ((key, zset) in other.data) {
            result.merge(key, zset) { z1, z2 ->
                val sum = z1.add(z2)
                if (sum.isEmpty()) null else sum
            }
        }

        return IndexedZSet(result, zero)
    }

    /**
     * Negate this indexed Z-set (invert all weights in all groups).
     */
    fun negate(): IndexedZSet<K, V, W> {
        val result = data.mapValues { (_, zset) -> zset.negate() }
        return IndexedZSet(result, zero)
    }

    /**
     * Subtract another indexed Z-set from this one.
     */
    fun subtract(other: IndexedZSet<K, V, W>): IndexedZSet<K, V, W> {
        return add(other.negate())
    }

    /**
     * Join this indexed Z-set with another indexed Z-set on matching keys.
     * The values from matching groups are combined using the Cartesian product.
     */
    fun <V2, R> join(
        other: IndexedZSet<K, V2, W>,
        combineFunc: (V, V2) -> R
    ): IndexedZSet<K, R, W> {
        val result = mutableMapOf<K, ZSet<R, W>>()

        // Find intersection of keys and perform Cartesian product for each
        for (key in keys()) {
            val leftZSet = data[key]
            val rightZSet = other.data[key]

            if (leftZSet != null && rightZSet != null) {
                val joined = cartesianProduct(leftZSet, rightZSet, combineFunc)
                if (!joined.isEmpty()) {
                    result[key] = joined
                }
            }
        }

        return IndexedZSet(result, zero)
    }

    /**
     * Perform Cartesian product of two Z-sets, multiplying weights.
     */
    private fun <V1, V2, R> cartesianProduct(
        left: ZSet<V1, W>,
        right: ZSet<V2, W>,
        combineFunc: (V1, V2) -> R
    ): ZSet<R, W> {
        val result = mutableMapOf<R, W>()

        for ((leftValue, leftWeight) in left.entries()) {
            for ((rightValue, rightWeight) in right.entries()) {
                val combined = combineFunc(leftValue, rightValue)
                val multipliedWeight = leftWeight.multiply(rightWeight)

                result.merge(combined, multipliedWeight) { w1, w2 ->
                    val sum = w1.add(w2)
                    if (sum.isZero()) null else sum
                }
            }
        }

        return ZSet.fromMap(result, zero)
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is IndexedZSet<*, *, *>) return false
        return data == other.data
    }

    override fun hashCode(): Int {
        return data.hashCode()
    }

    override fun toString(): String {
        return "IndexedZSet(${data.entries.joinToString(", ") { "${it.key}: ${it.value}" }})"
    }

    companion object {
        /**
         * Create an empty indexed Z-set with integer weights.
         */
        fun <K, V> empty(zero: IntegerWeight = IntegerWeight.ZERO): IndexedZSet<K, V, IntegerWeight> {
            return IndexedZSet(emptyMap(), zero)
        }

        /**
         * Create an indexed Z-set from a map of keys to Z-sets.
         * Empty ZSets are automatically filtered out.
         */
        fun <K, V, W : Weight<W>> fromMap(map: Map<K, ZSet<V, W>>, zero: W): IndexedZSet<K, V, W> {
            val data = map.filterValues { !it.isEmpty() }
            return IndexedZSet(data, zero)
        }

        /**
         * Create an indexed Z-set with a single key-ZSet pair.
         */
        fun <K, V> singleton(key: K, zset: ZSet<V, IntegerWeight>): IndexedZSet<K, V, IntegerWeight> {
            if (zset.isEmpty()) {
                return empty()
            }
            return IndexedZSet(mapOf(key to zset), IntegerWeight.ZERO)
        }
    }
}

/**
 * Index a Z-set by grouping values according to a key function.
 * Creates an indexed Z-set where each key maps to a Z-set of values.
 *
 * For ZSets with IntegerWeight, the zero parameter can be omitted.
 */
fun <K, V> ZSet<V, IntegerWeight>.index(keyFunc: (V) -> K): IndexedZSet<K, V, IntegerWeight> {
    val groups = mutableMapOf<K, MutableMap<V, IntegerWeight>>()

    for ((value, weight) in this.entries()) {
        val key = keyFunc(value)
        val group = groups.getOrPut(key) { mutableMapOf() }

        group.merge(value, weight) { w1, w2 ->
            val sum = w1.add(w2)
            if (sum.isZero()) null else sum
        }
    }

    // Convert groups to ZSets, filtering out empty ones
    val data = groups.mapValues { (_, valueWeightMap) ->
        ZSet.fromMap(valueWeightMap)
    }.filterValues { !it.isEmpty() }

    return IndexedZSet.fromMap(data, IntegerWeight.ZERO)
}

/**
 * Index a Z-set by grouping values according to a key function (generic weight version).
 * Creates an indexed Z-set where each key maps to a Z-set of values.
 *
 * @param keyFunc Function to extract the grouping key from each value
 * @param zero The zero element of the weight type
 */
fun <K, V, W : Weight<W>> ZSet<V, W>.index(keyFunc: (V) -> K, zero: W): IndexedZSet<K, V, W> {
    val groups = mutableMapOf<K, MutableMap<V, W>>()

    for ((value, weight) in this.entries()) {
        val key = keyFunc(value)
        val group = groups.getOrPut(key) { mutableMapOf() }

        group.merge(value, weight) { w1, w2 ->
            val sum = w1.add(w2)
            if (sum.isZero()) null else sum
        }
    }

    // Convert groups to ZSets, filtering out empty ones
    val data = groups.mapValues { (_, valueWeightMap) ->
        ZSet.fromMap(valueWeightMap, zero)
    }.filterValues { !it.isEmpty() }

    return IndexedZSet.fromMap(data, zero)
}
