package org.hooray.incremental

import clojure.lang.*
import org.hooray.algo.Prefix
import org.hooray.algo.ResultTuple

/**
 * An indexed Z-set is a nested relation - a map where each key is associated with an IZSet.
 * It represents grouped data with weights, forming a commutative group under addition.
 * Supports arbitrary nesting depth by allowing inner values to be any IZSet implementation.
 *
 * Invariant: Empty IZSets are never stored in the data map.
 *
 * @param K the type of keys used for indexing
 * @param W the type of weights
 */
class IndexedZSet<K, W : Weight<W>> private constructor(
    private val data: Map<K, IZSet<*, W, *>>,
    private val zero: W,
    private val one: W
) : IZSet<K, W, IndexedZSet<K, W>>, IPersistentMap {
    /**
     * Get the IZSet associated with a key.
     * Returns null if the key is not present or if the type doesn't match.
     * Uses a safe cast, so incorrect type assumptions return null rather than throwing.
     */
    @Suppress("UNCHECKED_CAST")
    fun <V, I : IZSet<V, W, I>> getTyped(key: K): I? {
        return data[key] as? I
    }

    /**
     * Get a nested value by following a path (prefix).
     * Uses the first element of the prefix as a key in this IndexedZSet.
     * If the prefix has only one element, returns the corresponding ZSet.
     * If the prefix has more elements and the value is an IndexedZSet,
     * recursively calls getByPrefix with the remaining path.
     * Returns null if the path cannot be followed.
     *
     * @param prefix The path to follow, where each element is a key at that level
     * @return The value at the end of the path, or null if not found
     */
    @Suppress("UNCHECKED_CAST")
    fun getByPrefix(prefix: Prefix): IZSet<K, W, *> {
        if (prefix.isEmpty()) {
            return this
        }
        val firstKey = prefix[0] as K
        val inner = data[firstKey] ?: return ZSet.empty(zero)

        return if (prefix.size == 1) {
            when (inner) {
                is ZSet<*, W> -> {
                    inner as ZSet<K, W>
                }
                is IndexedZSet<*, W> -> {
                    inner as IndexedZSet<K, W>
                }
                else -> {
                    throw IllegalArgumentException("Expected ZSet at the end of prefix, found ${inner::class}")
                }
            }
        } else {
            // Recursive case: inner must be an IndexedZSet
            val remainingPrefix = prefix.drop(1)
            when (inner) {
                is IndexedZSet<*, W> ->
                    inner.getByPrefix(remainingPrefix) as IZSet<K, W, *>
                else -> {
                    throw IllegalArgumentException("Expected IndexedZSet for intermediate prefix, found ${inner::class}")
                }
            }
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun extendLeavesStar(prefix: Prefix, mapFn: (Prefix, W) -> ZSet<K, W>): IndexedZSet<K, W> {
        // TODO optimize and get rid of casts
        // TODO unify this with ZSet.extendLeaves
        val result = mutableMapOf<K, IZSet<*, W, *>>()
        for ((key, inner) in data) {
            val extendedInner = when (inner) {
                is IndexedZSet<*, W> -> {
                    (inner as IndexedZSet<K, W>).extendLeavesStar((prefix + key) as Prefix, mapFn)
                }
                is ZSet<*, W> -> {
                    val newInner = mutableMapOf<K, ZSet<K, W>>()
                    for ((value, weight) in inner.entries()) {
                        val extension = mapFn((prefix + key + value) as Prefix, weight)
                        if (!extension.isEmpty()) {
                            newInner[value as K] = extension
                        }
                    }
                    fromMap(newInner, zero, one)
                }
                else -> throw IllegalStateException("Unexpected IZSet type: ${inner::class}")
            }
            if (!extendedInner.isEmpty()) {
                result[key] = extendedInner
            }
        }
        return IndexedZSet(result, zero, one)
    }

    override fun extendLeaves(mapFn: (Prefix, W) -> ZSet<K, W>): IndexedZSet<K, W> =
        extendLeavesStar(emptyList(), mapFn)

    @Suppress("UNCHECKED_CAST")
    fun forEachLeaf(mapFn: (Prefix, W) -> Unit) {
        // TODO optimize and get rid of casts
        // Likely something on the stack instead of heap
        fun recurse(current: IZSet<*, W, *>, prefix: Prefix) {
            when (current) {
                is IndexedZSet<*, W> -> {
                    for ((key, inner) in current.data.entries) {
                        val newPrefix = prefix + key
                        recurse(inner, newPrefix as Prefix)
                    }
                }
                is ZSet<*, W> -> {
                    for ((key, weight: W) in current.entries()) {
                        mapFn((prefix + key) as Prefix, weight)
                    }
                }
                else -> throw IllegalStateException("Unexpected IZSet type: ${current::class}")
            }
        }

        recurse(this, emptyList())
    }

    /**
     * Get all keys in this indexed Z-set.
     */
    override fun keys(): Set<K> {
        return data.keys
    }

    /**
     * Get the weight of a key.
     * Returns ONE if the key is present, ZERO if not.
     */
    override fun weight(key: K): W {
        return if (data.containsKey(key)) one else zero
    }

    /**
     * Get the depth of this indexed Z-set.
     * Returns 1 + depth of the first inner IZSet.
     * Throws IllegalStateException if this IndexedZSet is empty.
     */
    override fun depth(): Int {
        val firstInner = data.values.firstOrNull()
            ?: throw IllegalStateException("Cannot compute depth of empty IndexedZSet")
        return 1 + firstInner.depth()
    }

    /**
     * Check that two IndexedZSets have matching depth for algebraic operations.
     * Enabled by default, can be disabled by setting system property "zset.skip.depth.check" to "true".
     * Throws IllegalArgumentException if depths don't match.
     */
    private fun checkDepthMatch(other: IndexedZSet<K, W>, operation: String) {
        if (System.getProperty("zset.skip.depth.check") != "true") {
            if (!this.isEmpty() && !other.isEmpty()) {
                val thisDepth = this.depth()
                val otherDepth = other.depth()
                require(thisDepth == otherDepth) {
                    "Cannot $operation IndexedZSets of different depths: $thisDepth != $otherDepth"
                }
            }
        }
    }

    /**
     * Get all entries with a weight of ONE in this indexed Z-set.
     */
    override fun entries(): Set<Map.Entry<K, W>> {
        // TODO this might not be efficient
        return data.keys.associateWith { one }.entries
    }

    /**
     * Check if this indexed Z-set is empty.
     */
    override fun isEmpty(): Boolean {
        return data.isEmpty()
    }

    override val size: Int
        get() = data.size

    /**
     * Flatten this indexed Z-set into a regular Z-set by combining keys and values.
     * Values are type-erased so the combine function receives Any? for values.
     */
    @Suppress("UNCHECKED_CAST")
    fun <R> flatten(combineFunc: (K, Any?) -> R): ZSet<R, W> {
        val result = mutableMapOf<R, W>()

        for ((key, innerZSet) in data) {
            val izset = innerZSet as IZSet<Any?, W, *>
            for (value in izset.keys()) {
                val weight = izset.weight(value)
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
     * Type parameter V must match the actual value type in the nested IZSets.
     */
    @Suppress("UNCHECKED_CAST")
    fun <V> deindex(): ZSet<V, W> {
        return flatten { _, v -> v as V }
    }

    /**
     * Helper method to add two type-erased IZSets.
     * Uses unchecked casts to work around F-bounded polymorphism limitations.
     */
    @Suppress("UNCHECKED_CAST")
    private fun addInner(inner1: IZSet<*, W, *>, inner2: IZSet<*, W, *>): IZSet<*, W, *> {
        // We know both IZSets have compatible structure since they came from IndexedZSets with the same depth
        // The type system can't express this with F-bounded polymorphism and type erasure
        if (inner1 is IndexedZSet<*, *> && inner2 is IndexedZSet<*, *>) {
            val i1 = inner1 as IndexedZSet<Any?, W>
            val i2 = inner2 as IndexedZSet<Any?, W>
            return i1.add(i2)
        }
        if (inner1 is ZSet<*, *> && inner2 is ZSet<*, *>) {
            val z1 = inner1 as ZSet<Any?, W>
            val z2 = inner2 as ZSet<Any?, W>
            return z1.add(z2)
        }
        throw IllegalStateException("Unexpected IZSet types: ${inner1::class} and ${inner2::class}")
    }

    /**
     * Add this indexed Z-set to another indexed Z-set.
     * Groups with matching keys have their IZSets combined.
     * Requires both IndexedZSets to have the same depth.
     */
    override fun add(other: IndexedZSet<K, W>): IndexedZSet<K, W> {
        checkDepthMatch(other, "add")

        val result = mutableMapOf<K, IZSet<*, W, *>>()

        // Add entries from this indexed Z-set
        for ((key, innerZSet) in data) {
            result[key] = innerZSet
        }

        // Merge entries from the other indexed Z-set
        for ((key, otherInner) in other.data) {
            result.merge(key, otherInner) { inner1, inner2 ->
                val sum = addInner(inner1, inner2)
                if (sum.isEmpty()) null else sum
            }
        }

        return IndexedZSet(result, zero, one)
    }

    /**
     * Operator overloading for addition.
     * Allows using `izset1 + izset2` instead of `izset1.add(izset2)`.
     */
    operator fun plus(other: IndexedZSet<K, W>): IndexedZSet<K, W> = add(other)

    /**
     * Helper method to negate a type-erased IZSet.
     * Uses unchecked casts to work around F-bounded polymorphism limitations.
     */
    @Suppress("UNCHECKED_CAST")
    private fun negateInner(inner: IZSet<*, W, *>): IZSet<*, W, *> {
        if (inner is IndexedZSet<*, *>) {
            val i = inner as IndexedZSet<Any?, W>
            return i.negate()
        }
        if (inner is ZSet<*, *>) {
            val z = inner as ZSet<Any?, W>
            return z.negate()
        }
        throw IllegalStateException("Unexpected IZSet type: ${inner::class}")
    }

    /**
     * Negate this indexed Z-set (invert all weights in all groups).
     */
    override fun negate(): IndexedZSet<K, W> {
        val result = data.mapValues { (_, innerZSet) ->
            negateInner(innerZSet)
        }
        return IndexedZSet(result, zero, one)
    }

    /**
     * Subtract another indexed Z-set from this one.
     * Requires both IndexedZSets to have the same depth.
     */
    override fun subtract(other: IndexedZSet<K, W>): IndexedZSet<K, W> {
        checkDepthMatch(other, "subtract")
        return add(other.negate())
    }

    /**
     * Join this indexed Z-set with another indexed Z-set on matching keys.
     * The values from matching groups are combined using the Cartesian product.
     * This method works only when both indexed Z-sets have ZSet as their inner type.
     */
    fun <V1, V2, R> join(
        other: IndexedZSet<K, W>,
        combineFunc: (V1, V2) -> R
    ): IndexedZSet<K, W> {
        val result = mutableMapOf<K, IZSet<*, W, *>>()

        // Find intersection of keys and perform Cartesian product for each
        for (key in keys()) {
            val leftZSet = getTyped<V1, ZSet<V1, W>>(key)
            val rightZSet = other.getTyped<V2, ZSet<V2, W>>(key)

            if (leftZSet != null && rightZSet != null) {
                val joined = leftZSet.multiply(rightZSet, combineFunc)
                if (!joined.isEmpty()) {
                    result[key] = joined
                }
            }
        }

        return IndexedZSet(result, zero, one)
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is IndexedZSet<*, *>) return false
        return data == other.data
    }

    override fun hashCode(): Int {
        return data.hashCode()
    }

    override fun toString(): String {
        return "IndexedZSet(${data.entries.joinToString(", ") { "${it.key}: ${it.value}" }})"
    }

    override fun flatZSet(): ZSet<ResultTuple, W> {
        val resultMap = mutableMapOf<ResultTuple, W>()
        this.forEachLeaf { resultTuple, weight -> resultMap[resultTuple] = weight }
        return ZSet.fromMap(resultMap, zero)
    }

    // TODO this is not efficient (not a view)
    override fun zSetView(): ZSet<K, W> =
        ZSet.fromMap(
            data.keys.associateWith { one },
            zero
        )

    // TODO probably good to either make the Z-set implementations immutable or find some other solution
    @Suppress("ACCIDENTAL_OVERRIDE")
    override fun containsKey(key: Any?): Boolean = data.containsKey(key)

    override fun entryAt(key: Any?): IMapEntry? {
        val value = data[key] ?: return null
        return object : IMapEntry {
            override fun key(): Any? = key
            override fun `val`(): Any?  = value
            override val key: Any?
                get() = key()
            override val value: Any?
                get() = `val`()

            override fun setValue(newValue: Any?): Any? {
                TODO("Not yet implemented")
            }
        }
    }

    @Suppress("UNCHECKED_CAST")
    override fun assoc(key: Any?, `val`: Any?): IndexedZSet<K, W> = IndexedZSet(data + mapOf(key as K to `val` as IZSet<*, W, *>), zero, one)
    override fun assocEx(key: Any?, `val`: Any?): IPersistentMap {
        TODO("Not yet implemented")
    }

    @Suppress("UNCHECKED_CAST")
    override fun without(key: Any?): IndexedZSet<K,W> {
        val newData = data - key
        return (if (newData.size == data.size) {
            this
        } else {
            IndexedZSet(newData, zero, one)
        }) as IndexedZSet<K, W>
    }

    override fun count(): Int = data.size

    override fun cons(o: Any?): IPersistentCollection? =
        when (o) {
            is IMapEntry -> this.assoc(o.key, o.`val`())
            is IPersistentVector -> {
                if (o.count() != 2) {
                    throw IllegalArgumentException("Can only cons 2-element IPersistentVector to IndexedZSet, found size: ${o.count()}")
                }
                val key = o.nth(0)
                val value = o.nth(1)
                this.assoc(key, value)
            }
            else -> throw IllegalArgumentException("Can only cons IMapEntry or IPersistentVector to IndexedZSet, found: ${o?.let { it::class }}")
        }

    override fun empty(): IPersistentCollection = empty<K, W>(zero, one)

    @Suppress("UNCHECKED_CAST")
    override fun equiv(obj: Any?): Boolean {
        if (this === obj) return true
        if (obj !is IndexedZSet<*, *>) return false
        val zset: IndexedZSet<K, W> = obj as IndexedZSet<K, W>

        for ((key, value) in iterator()) {
            if (key !in zset || zset.valAt(key) != value) {
                return false
            }
        }
        return true
    }

    override fun seq(): ISeq? = PersistentHashMap.create(data as Map<*, *>).seq()

    override fun valAt(key: Any?): Any? = data[key]

    override fun valAt(key: Any?, notFound: Any?): Any? = data[key] ?: notFound

    override fun iterator(): MutableIterator<Map.Entry<K, IZSet<*, W, *>>> = object : MutableIterator<Map.Entry<K, IZSet<*, W, *>>> {
        private val delegate = data.entries.iterator()
        override fun hasNext() = delegate.hasNext()
        override fun next() = delegate.next()
        override fun remove() = throw UnsupportedOperationException("IndexedZSet is immutable")
    }

    companion object {
        /**
         * Create an empty indexed Z-set with integer weights.
         */
        @JvmStatic
        fun <K> empty(): IndexedZSet<K, IntegerWeight> {
            return IndexedZSet(emptyMap(), IntegerWeight.ZERO, IntegerWeight.ONE)
        }

        /**
         * Create an empty indexed Z-set.
         */
        @JvmStatic
        fun <K, W : Weight<W>> empty(
            zero: W,
            one: W
        ): IndexedZSet<K, W> {
            return IndexedZSet(emptyMap(), zero, one)
        }

        /**
         * Create an indexed Z-set from a map of keys to IZSets.
         * Empty IZSets are automatically filtered out.
         */
        @JvmStatic
        fun <K, V, W : Weight<W>, I : IZSet<V, W, I>> fromMap(
            map: Map<K, I>,
            zero: W,
            one: W
        ): IndexedZSet<K, W> {
            val data = map.filterValues { !it.isEmpty() }
            return IndexedZSet(data, zero, one)
        }

        /**
         * Create an indexed Z-set with a single key-IZSet pair.
         */
        @JvmStatic
        fun <K, V, W : Weight<W>, I : IZSet<V, W, I>> singleton(
            key: K,
            inner: I,
            zero: W,
            one: W
        ): IndexedZSet<K, W> {
            if (inner.isEmpty()) {
                return empty(zero, one)
            }
            return IndexedZSet(mapOf(key to inner), zero, one)
        }
    }
}

/**
 * Index a Z-set by grouping values according to a key function.
 * Creates an indexed Z-set where each key maps to a Z-set of values.
 *
 * For ZSets with IntegerWeight.
 */
fun <K, V> ZSet<V, IntegerWeight>.index(keyFunc: (V) -> K): IndexedZSet<K, IntegerWeight> {
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

    return IndexedZSet.fromMap(data, IntegerWeight.ZERO, IntegerWeight.ONE)
}

/**
 * Index a Z-set by grouping values according to a key function (generic weight version).
 * Creates an indexed Z-set where each key maps to a Z-set of values.
 *
 * @param keyFunc Function to extract the grouping key from each value
 * @param zero The zero element of the weight type
 * @param one The one element of the weight type
 */
fun <K, V, W : Weight<W>> ZSet<V, W>.index(keyFunc: (V) -> K, zero: W, one: W): IndexedZSet<K, W> {
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

    return IndexedZSet.fromMap(data, zero, one)
}
