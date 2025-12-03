package org.hooray.incremental

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.assertThrows

class IndexedZSetTest {

    // ========== Core Functionality Tests ==========

    @Test
    fun `test empty IndexedZSet`() {
        val empty = IndexedZSet.empty<String, IntegerWeight>(IntegerWeight.ZERO, IntegerWeight.ONE)
        assertTrue(empty.isEmpty())
        assertEquals(0, empty.size)
        assertNull(empty.getTyped<Int, ZSet<Int, IntegerWeight>>("any"))
    }

    @Test
    fun `test singleton IndexedZSet`() {
        val zset = ZSet.fromMap(mapOf(1 to IntegerWeight(2), 2 to IntegerWeight(3)))
        val indexed = IndexedZSet.singleton("key", zset, IntegerWeight.ZERO, IntegerWeight.ONE)

        assertEquals(1, indexed.size)
        assertFalse(indexed.isEmpty())
        assertEquals(zset, indexed.getTyped<Int, ZSet<Int, IntegerWeight>>("key"))
    }

    @Test
    fun `test singleton with empty ZSet creates empty IndexedZSet`() {
        val emptyZSet = ZSet.empty<Int>()
        val indexed = IndexedZSet.singleton("key", emptyZSet, IntegerWeight.ZERO, IntegerWeight.ONE)

        assertTrue(indexed.isEmpty())
    }

    @Test
    fun `test fromMap filters empty ZSets`() {
        val zset1 = ZSet.fromMap(mapOf(1 to IntegerWeight(1)))
        val zset2 = ZSet.empty<Int>()
        val zset3 = ZSet.fromMap(mapOf(2 to IntegerWeight(2)))

        val map = mapOf("a" to zset1, "b" to zset2, "c" to zset3)
        val indexed = IndexedZSet.fromMap(map, IntegerWeight.ZERO, IntegerWeight.ONE)

        assertEquals(2, indexed.size)
        assertNotNull(indexed.getTyped<Int, ZSet<Int, IntegerWeight>>("a"))
        assertNull(indexed.getTyped<Int, ZSet<Int, IntegerWeight>>("b"))  // Empty ZSet filtered out
        assertNotNull(indexed.getTyped<Int, ZSet<Int, IntegerWeight>>("c"))
    }

    @Test
    fun `test index groups correctly`() {
        val zset = ZSet.fromCollection(listOf("apple", "apricot", "banana", "avocado"))
        val indexed = zset.index { it.first() }

        assertEquals(2, indexed.size)

        val aGroup = indexed.getTyped<String, ZSet<String, IntegerWeight>>('a')
        assertNotNull(aGroup)
        assertEquals(3, aGroup!!.size)
        assertEquals(IntegerWeight(1), aGroup.weight("apple"))
        assertEquals(IntegerWeight(1), aGroup.weight("apricot"))
        assertEquals(IntegerWeight(1), aGroup.weight("avocado"))

        val bGroup = indexed.getTyped<String, ZSet<String, IntegerWeight>>('b')
        assertNotNull(bGroup)
        assertEquals(1, bGroup!!.size)
        assertEquals(IntegerWeight(1), bGroup.weight("banana"))
    }

    @Test
    fun `test index maintains weights`() {
        val zset = ZSet.fromMap(mapOf(
            "a1" to IntegerWeight(2),
            "a2" to IntegerWeight(3),
            "b1" to IntegerWeight(5)
        ))
        val indexed = zset.index { it.first() }

        val aGroup = indexed.getTyped<String, ZSet<String, IntegerWeight>>('a')!!
        assertEquals(IntegerWeight(2), aGroup.weight("a1"))
        assertEquals(IntegerWeight(3), aGroup.weight("a2"))

        val bGroup = indexed.getTyped<String, ZSet<String, IntegerWeight>>('b')!!
        assertEquals(IntegerWeight(5), bGroup.weight("b1"))
    }

    @Test
    fun `test index with all values mapping to same key`() {
        val zset = ZSet.fromCollection(listOf("a", "b", "c"))
        val indexed = zset.index { "same" }

        assertEquals(1, indexed.size)
        val group = indexed.getTyped<String, ZSet<String, IntegerWeight>>("same")!!
        assertEquals(3, group.size)
    }

    @Test
    fun `test index with each value mapping to different key`() {
        val zset = ZSet.fromCollection(listOf("a", "b", "c"))
        val indexed = zset.index { it }

        assertEquals(3, indexed.size)
        assertNotNull(indexed.getTyped<String, ZSet<String, IntegerWeight>>("a"))
        assertNotNull(indexed.getTyped<String, ZSet<String, IntegerWeight>>("b"))
        assertNotNull(indexed.getTyped<String, ZSet<String, IntegerWeight>>("c"))
    }

    @Test
    fun `test flatten reconstructs ZSet`() {
        val zset1 = ZSet.fromMap(mapOf(1 to IntegerWeight(2), 2 to IntegerWeight(3)))
        val zset2 = ZSet.fromMap(mapOf(3 to IntegerWeight(1), 4 to IntegerWeight(4)))

        val indexed = IndexedZSet.fromMap(mapOf("a" to zset1, "b" to zset2), IntegerWeight.ZERO, IntegerWeight.ONE)
        val flattened = indexed.flatten { key, value -> "$key-$value" }

        assertEquals(4, flattened.size)
        assertEquals(IntegerWeight(2), flattened.weight("a-1"))
        assertEquals(IntegerWeight(3), flattened.weight("a-2"))
        assertEquals(IntegerWeight(1), flattened.weight("b-3"))
        assertEquals(IntegerWeight(4), flattened.weight("b-4"))
    }

    @Test
    fun `test deindex concatenates all values`() {
        val zset1 = ZSet.fromMap(mapOf(1 to IntegerWeight(2), 2 to IntegerWeight(3)))
        val zset2 = ZSet.fromMap(mapOf(3 to IntegerWeight(1)))

        val indexed = IndexedZSet.fromMap(mapOf("a" to zset1, "b" to zset2), IntegerWeight.ZERO, IntegerWeight.ONE)
        val deindexed = indexed.deindex<Int>()

        assertEquals(3, deindexed.size)
        assertEquals(IntegerWeight(2), deindexed.weight(1))
        assertEquals(IntegerWeight(3), deindexed.weight(2))
        assertEquals(IntegerWeight(1), deindexed.weight(3))
    }

    @Test
    fun `test deindex with overlapping values adds weights`() {
        val zset1 = ZSet.fromMap(mapOf(1 to IntegerWeight(2)))
        val zset2 = ZSet.fromMap(mapOf(1 to IntegerWeight(3)))

        val indexed = IndexedZSet.fromMap(mapOf("a" to zset1, "b" to zset2), IntegerWeight.ZERO, IntegerWeight.ONE)
        val deindexed = indexed.deindex<Int>()

        assertEquals(1, deindexed.size)
        assertEquals(IntegerWeight(5), deindexed.weight(1))
    }

    @Test
    fun `test join with matching keys`() {
        val left = ZSet.fromCollection(listOf("a1", "a2")).index { it.first() }
        val right = ZSet.fromCollection(listOf("a3", "a4")).index { it.first() }

        val joined = left.join<String, String, String>(right) { l, r -> "$l-$r" }

        assertEquals(1, joined.size)
        val aGroup = joined.getTyped<String, ZSet<String, IntegerWeight>>('a')!!
        assertEquals(4, aGroup.size)  // Cartesian product: 2 * 2 = 4
        assertEquals(IntegerWeight(1), aGroup.weight("a1-a3"))
        assertEquals(IntegerWeight(1), aGroup.weight("a1-a4"))
        assertEquals(IntegerWeight(1), aGroup.weight("a2-a3"))
        assertEquals(IntegerWeight(1), aGroup.weight("a2-a4"))
    }

    @Test
    fun `test join with disjoint keys returns empty`() {
        val left = ZSet.fromCollection(listOf("a1")).index { it.first() }
        val right = ZSet.fromCollection(listOf("b1")).index { it.first() }

        val joined = left.join<String, String, String>(right) { l, r -> "$l-$r" }

        assertTrue(joined.isEmpty())
    }

    @Test
    fun `test join multiplies weights correctly`() {
        val leftZSet = ZSet.fromMap(mapOf("x" to IntegerWeight(2), "y" to IntegerWeight(3)))
        val rightZSet = ZSet.fromMap(mapOf("p" to IntegerWeight(4), "q" to IntegerWeight(5)))

        val left = IndexedZSet.singleton("key", leftZSet, IntegerWeight.ZERO, IntegerWeight.ONE)
        val right = IndexedZSet.singleton("key", rightZSet, IntegerWeight.ZERO, IntegerWeight.ONE)

        val joined = left.join<String, String, String>(right) { l, r -> "$l-$r" }

        val group = joined.getTyped<String, ZSet<String, IntegerWeight>>("key")!!
        assertEquals(IntegerWeight(8), group.weight("x-p"))   // 2 * 4 = 8
        assertEquals(IntegerWeight(10), group.weight("x-q"))  // 2 * 5 = 10
        assertEquals(IntegerWeight(12), group.weight("y-p"))  // 3 * 4 = 12
        assertEquals(IntegerWeight(15), group.weight("y-q"))  // 3 * 5 = 15
    }

    @Test
    fun `test join with some matching and some disjoint keys`() {
        val left = ZSet.fromCollection(listOf("a1", "b1")).index { it.first() }
        val right = ZSet.fromCollection(listOf("a2", "c1")).index { it.first() }

        val joined = left.join<String, String, String>(right) { l, r -> "$l-$r" }

        assertEquals(1, joined.size)
        assertNotNull(joined.getTyped<String, ZSet<String, IntegerWeight>>('a'))
        assertNull(joined.getTyped<String, ZSet<String, IntegerWeight>>('b'))
        assertNull(joined.getTyped<String, ZSet<String, IntegerWeight>>('c'))
    }

    // ========== Algebraic Properties Tests ==========

    @Test
    fun `test add with disjoint keys`() {
        val zset1 = ZSet.fromMap(mapOf(1 to IntegerWeight(1)))
        val zset2 = ZSet.fromMap(mapOf(2 to IntegerWeight(2)))

        val indexed1 = IndexedZSet.singleton("a", zset1, IntegerWeight.ZERO, IntegerWeight.ONE)
        val indexed2 = IndexedZSet.singleton("b", zset2, IntegerWeight.ZERO, IntegerWeight.ONE)

        val result = indexed1.add(indexed2)

        assertEquals(2, result.size)
        assertNotNull(result.getTyped<Int, ZSet<Int, IntegerWeight>>("a"))
        assertNotNull(result.getTyped<Int, ZSet<Int, IntegerWeight>>("b"))
    }

    @Test
    fun `test add with overlapping keys combines ZSets`() {
        val zset1 = ZSet.fromMap(mapOf(1 to IntegerWeight(2)))
        val zset2 = ZSet.fromMap(mapOf(2 to IntegerWeight(3)))

        val indexed1 = IndexedZSet.singleton("a", zset1, IntegerWeight.ZERO, IntegerWeight.ONE)
        val indexed2 = IndexedZSet.singleton("a", zset2, IntegerWeight.ZERO, IntegerWeight.ONE)

        val result = indexed1.add(indexed2)

        assertEquals(1, result.size)
        val aGroup = result.getTyped<Int, ZSet<Int, IntegerWeight>>("a")!!
        assertEquals(2, aGroup.size)
        assertEquals(IntegerWeight(2), aGroup.weight(1))
        assertEquals(IntegerWeight(3), aGroup.weight(2))
    }

    @Test
    fun `test add removes keys with empty ZSets`() {
        val zset1 = ZSet.fromMap(mapOf(1 to IntegerWeight(5)))
        val zset2 = ZSet.fromMap(mapOf(1 to IntegerWeight(-5)))

        val indexed1 = IndexedZSet.singleton("a", zset1, IntegerWeight.ZERO, IntegerWeight.ONE)
        val indexed2 = IndexedZSet.singleton("a", zset2, IntegerWeight.ZERO, IntegerWeight.ONE)

        val result = indexed1.add(indexed2)

        assertTrue(result.isEmpty())
    }

    @Test
    fun `test negate inverts all weights`() {
        val zset = ZSet.fromMap(mapOf(1 to IntegerWeight(3), 2 to IntegerWeight(-2)))
        val indexed = IndexedZSet.singleton("a", zset, IntegerWeight.ZERO, IntegerWeight.ONE)

        val negated = indexed.negate()

        val aGroup = negated.getTyped<Int, ZSet<Int, IntegerWeight>>("a")!!
        assertEquals(IntegerWeight(-3), aGroup.weight(1))
        assertEquals(IntegerWeight(2), aGroup.weight(2))
    }

    @Test
    fun `test double negation returns original`() {
        val zset = ZSet.fromMap(mapOf(1 to IntegerWeight(5), 2 to IntegerWeight(-3)))
        val indexed = IndexedZSet.singleton("a", zset, IntegerWeight.ZERO, IntegerWeight.ONE)

        assertEquals(indexed, indexed.negate().negate())
    }

    @Test
    fun `test subtract uses add and negate`() {
        val zset1 = ZSet.fromMap(mapOf(1 to IntegerWeight(5)))
        val zset2 = ZSet.fromMap(mapOf(1 to IntegerWeight(2)))

        val indexed1 = IndexedZSet.singleton("a", zset1, IntegerWeight.ZERO, IntegerWeight.ONE)
        val indexed2 = IndexedZSet.singleton("a", zset2, IntegerWeight.ZERO, IntegerWeight.ONE)

        val result = indexed1.subtract(indexed2)

        val aGroup = result.getTyped<Int, ZSet<Int, IntegerWeight>>("a")!!
        assertEquals(IntegerWeight(3), aGroup.weight(1))
    }

    @Test
    fun `test addition is commutative`() {
        val zset1 = ZSet.fromMap(mapOf(1 to IntegerWeight(2)))
        val zset2 = ZSet.fromMap(mapOf(2 to IntegerWeight(3)))

        val indexed1 = IndexedZSet.singleton("a", zset1, IntegerWeight.ZERO, IntegerWeight.ONE)
        val indexed2 = IndexedZSet.singleton("b", zset2, IntegerWeight.ZERO, IntegerWeight.ONE)

        assertEquals(indexed1.add(indexed2), indexed2.add(indexed1))
    }

    @Test
    fun `test addition is associative`() {
        val zset1 = ZSet.fromMap(mapOf(1 to IntegerWeight(1)))
        val zset2 = ZSet.fromMap(mapOf(2 to IntegerWeight(2)))
        val zset3 = ZSet.fromMap(mapOf(3 to IntegerWeight(3)))

        val indexed1 = IndexedZSet.singleton("a", zset1, IntegerWeight.ZERO, IntegerWeight.ONE)
        val indexed2 = IndexedZSet.singleton("b", zset2, IntegerWeight.ZERO, IntegerWeight.ONE)
        val indexed3 = IndexedZSet.singleton("c", zset3, IntegerWeight.ZERO, IntegerWeight.ONE)

        assertEquals(
            indexed1.add(indexed2).add(indexed3),
            indexed1.add(indexed2.add(indexed3))
        )
    }

    @Test
    fun `test empty is identity for addition`() {
        val zset = ZSet.fromMap(mapOf(1 to IntegerWeight(5)))
        val indexed = IndexedZSet.singleton("a", zset, IntegerWeight.ZERO, IntegerWeight.ONE)
        val empty = IndexedZSet.empty<String, IntegerWeight>(IntegerWeight.ZERO, IntegerWeight.ONE)

        assertEquals(indexed, indexed.add(empty))
        assertEquals(indexed, empty.add(indexed))
    }

    // ========== Edge Cases Tests ==========

    @Test
    fun `test index on empty ZSet`() {
        val empty = ZSet.empty<String>()
        val indexed = empty.index { it.first() }

        assertTrue(indexed.isEmpty())
    }

    @Test
    fun `test flatten on empty IndexedZSet`() {
        val empty = IndexedZSet.empty<String, IntegerWeight>(IntegerWeight.ZERO, IntegerWeight.ONE)
        val flattened = empty.flatten { k, v -> "$k-$v" }

        assertTrue(flattened.isEmpty())
    }

    @Test
    fun `test deindex on empty IndexedZSet`() {
        val empty = IndexedZSet.empty<String, IntegerWeight>(IntegerWeight.ZERO, IntegerWeight.ONE)
        val deindexed = empty.deindex<Int>()

        assertTrue(deindexed.isEmpty())
    }

    @Test
    fun `test join on empty IndexedZSets`() {
        val empty1 = IndexedZSet.empty<String, IntegerWeight>(IntegerWeight.ZERO, IntegerWeight.ONE)
        val empty2 = IndexedZSet.empty<String, IntegerWeight>(IntegerWeight.ZERO, IntegerWeight.ONE)

        val joined = empty1.join<Int, Int, Int>(empty2) { l, r -> l + r }

        assertTrue(joined.isEmpty())
    }

    @Test
    fun `test join with weights that cancel to zero`() {
        val leftZSet = ZSet.fromMap(mapOf("x" to IntegerWeight(2), "y" to IntegerWeight(-2)))
        val rightZSet = ZSet.fromMap(mapOf("p" to IntegerWeight(3)))

        val left = IndexedZSet.singleton("key", leftZSet, IntegerWeight.ZERO, IntegerWeight.ONE)
        val right = IndexedZSet.singleton("key", rightZSet, IntegerWeight.ZERO, IntegerWeight.ONE)

        val joined = left.join<String, String, String>(right) { l, _ -> l }

        // x*3 = 6, y*3 = -6, they map to different combined values so don't cancel
        val group = joined.getTyped<String, ZSet<String, IntegerWeight>>("key")!!
        assertEquals(IntegerWeight(6), group.weight("x"))
        assertEquals(IntegerWeight(-6), group.weight("y"))
    }

    @Test
    fun `test entries returns all key-ZSet pairs`() {
        val zset1 = ZSet.fromMap(mapOf(1 to IntegerWeight(1)))
        val zset2 = ZSet.fromMap(mapOf(2 to IntegerWeight(2)))

        val indexed = IndexedZSet.fromMap(mapOf("a" to zset1, "b" to zset2), IntegerWeight.ZERO, IntegerWeight.ONE)
        val entries = indexed.entries()

        assertEquals(2, entries.size)
        assertTrue(entries.any { it.key == "a" })
        assertTrue(entries.any { it.key == "b" })
    }

    @Test
    fun `test keys returns all keys`() {
        val zset1 = ZSet.fromMap(mapOf(1 to IntegerWeight(1)))
        val zset2 = ZSet.fromMap(mapOf(2 to IntegerWeight(2)))

        val indexed = IndexedZSet.fromMap(mapOf("a" to zset1, "b" to zset2), IntegerWeight.ZERO, IntegerWeight.ONE)
        val keys = indexed.keys()

        assertEquals(2, keys.size)
        assertTrue(keys.contains("a"))
        assertTrue(keys.contains("b"))
    }

    @Test
    fun `test weight returns ONE for present keys`() {
        val zset1 = ZSet.fromMap(mapOf(1 to IntegerWeight(2)))
        val zset2 = ZSet.fromMap(mapOf(2 to IntegerWeight(3)))

        val indexed = IndexedZSet.fromMap(mapOf("a" to zset1, "b" to zset2), IntegerWeight.ZERO, IntegerWeight.ONE)

        assertEquals(IntegerWeight.ONE, indexed.weight("a"))
        assertEquals(IntegerWeight.ONE, indexed.weight("b"))
    }

    @Test
    fun `test weight returns ZERO for absent keys`() {
        val zset = ZSet.fromMap(mapOf(1 to IntegerWeight(5)))
        val indexed = IndexedZSet.singleton("a", zset, IntegerWeight.ZERO, IntegerWeight.ONE)

        assertEquals(IntegerWeight.ZERO, indexed.weight("b"))
        assertEquals(IntegerWeight.ZERO, indexed.weight("c"))
    }

    @Test
    fun `test weight on empty IndexedZSet returns ZERO`() {
        val empty = IndexedZSet.empty<String, IntegerWeight>(IntegerWeight.ZERO, IntegerWeight.ONE)

        assertEquals(IntegerWeight.ZERO, empty.weight("any"))
    }

    @Test
    fun `test depth returns 2 for simple IndexedZSet`() {
        // IndexedZSet contains ZSet<Int, IntegerWeight>
        // ZSet has depth 1, so IndexedZSet should have depth 2
        val zset = ZSet.fromCollection(listOf(1, 2, 3))
        val indexed = IndexedZSet.singleton("key", zset, IntegerWeight.ZERO, IntegerWeight.ONE)

        assertEquals(2, indexed.depth())
    }

    @Test
    fun `test depth with nested IndexedZSet`() {
        // Create a nested structure: IndexedZSet<String, ZSet<ZSet<Int, IntegerWeight>, IntegerWeight>, IntegerWeight>
        // The values are ZSets containing ZSets
        // So the depth should be 1 + 1 + 1 = 3

        // First level: ZSet<Int, IntegerWeight>
        val innerZSet = ZSet.fromCollection(listOf(1, 2, 3))

        // Second level: IndexedZSet containing ZSets as keys (creates nested structure)
        val outerZSet = ZSet.singleton(innerZSet, IntegerWeight.ONE)
        val indexed = IndexedZSet.singleton("key", outerZSet, IntegerWeight.ZERO, IntegerWeight.ONE)

        // The indexed ZSet contains ZSet<ZSet<Int, IntegerWeight>, IntegerWeight>
        // The first value is a ZSet (with depth 1)
        // So depth should be 1 + 1 = 2
        assertEquals(2, indexed.depth())
    }

    @Test
    fun `test getByPrefix with single level`() {
        val zset = ZSet.fromCollection(listOf(1, 2, 3))
        val indexed : IndexedZSet<Any, IntegerWeight> = IndexedZSet.singleton("key", zset, IntegerWeight.ZERO, IntegerWeight.ONE)

        val result = indexed.getByPrefix(listOf("key"))

        assertNotNull(result)
        assertEquals(zset, result)
    }

    @Test
    fun `test getByPrefix with empty prefix returns null`() {
        val zset = ZSet.fromCollection(listOf(1, 2, 3))
        val indexed = IndexedZSet.singleton("key", zset, IntegerWeight.ZERO, IntegerWeight.ONE)

        val result = indexed.getByPrefix(emptyList())

        val indexedView = ZSet.fromCollection(listOf("key"))

        assertEquals(indexedView, result)
    }

    @Test
    fun `test getByPrefix with non-existent key returns null`() {
        val zset = ZSet.fromCollection(listOf(1, 2, 3))
        val indexed = IndexedZSet.singleton("key", zset, IntegerWeight.ZERO, IntegerWeight.ONE)

        val result = indexed.getByPrefix(listOf("nonexistent"))

        assertEquals(ZSet.empty<IntegerWeight>(), result)
    }

    @Test
    fun `test getByPrefix with nested IndexedZSet`() {
        val level1 = ZSet.fromCollection(listOf("a", "b"))
        val level2 : IndexedZSet<Any, IntegerWeight> = IndexedZSet.singleton("top", level1, IntegerWeight.ZERO, IntegerWeight.ONE)

        val result1 = level2.getByPrefix(listOf("top"))

        assertEquals(level1, result1)

        assertThrows<IllegalArgumentException> { level2.getByPrefix(listOf("top", "a")) }
    }

    @Test
    fun `test getByPrefix with wrong type in prefix returns null`() {
        val zset = ZSet.fromCollection(listOf(1, 2, 3))
        val indexed = IndexedZSet.singleton("key", zset, IntegerWeight.ZERO, IntegerWeight.ONE)

        // Using an Int when a String is expected
        val result = indexed.getByPrefix(listOf(123))

        assertEquals(ZSet.empty<IntegerWeight>() ,result)
    }

    // ========== Integration Tests ==========

    @Test
    fun `test round trip index then deindex preserves multiset`() {
        val original = ZSet.fromCollection(listOf("apple", "apricot", "banana", "apple"))
        val indexed = original.index { it.first() }
        val deindexed = indexed.deindex<String>()

        // Should preserve weights (apple appears twice)
        assertEquals(original, deindexed)
    }

    @Test
    fun `test complex join with multiple groups`() {
        val orders = ZSet.fromCollection(listOf(
            "customer1:item1",
            "customer1:item2",
            "customer2:item3"
        ))

        val customers = ZSet.fromCollection(listOf(
            "customer1:name1",
            "customer2:name2"
        ))

        val ordersIndexed = orders.index { it.substringBefore(':') }
        val customersIndexed = customers.index { it.substringBefore(':') }

        val joined = ordersIndexed.join<String, String, String>(customersIndexed) { order, customer ->
            "$order-$customer"
        }

        assertEquals(2, joined.size)

        val customer1Group = joined.getTyped<String, ZSet<String, IntegerWeight>>("customer1")!!
        assertEquals(2, customer1Group.size)  // 2 orders * 1 customer = 2

        val customer2Group = joined.getTyped<String, ZSet<String, IntegerWeight>>("customer2")!!
        assertEquals(1, customer2Group.size)  // 1 order * 1 customer = 1
    }

    @Test
    fun `test chained operations`() {
        val zset = ZSet.fromCollection(listOf("a1", "a2", "b1", "b2", "b3"))

        val indexed = zset.index { it.first() }
        val negated = indexed.negate()
        val doubleNegated = negated.negate()
        val deindexed = doubleNegated.deindex<String>()

        // After double negation and deindexing, should equal original
        assertEquals(zset, deindexed)
    }

    @Test
    fun `test equals and hashCode`() {
        val zset = ZSet.fromMap(mapOf(1 to IntegerWeight(1)))

        val indexed1 = IndexedZSet.singleton("a", zset, IntegerWeight.ZERO, IntegerWeight.ONE)
        val indexed2 = IndexedZSet.singleton("a", zset, IntegerWeight.ZERO, IntegerWeight.ONE)
        val indexed3 = IndexedZSet.singleton("b", zset, IntegerWeight.ZERO, IntegerWeight.ONE)

        assertEquals(indexed1, indexed2)
        assertEquals(indexed1.hashCode(), indexed2.hashCode())
        assertNotEquals(indexed1, indexed3)
    }

    @Test
    fun `test toString`() {
        val zset = ZSet.fromMap(mapOf(1 to IntegerWeight(1)))
        val indexed = IndexedZSet.singleton("a", zset, IntegerWeight.ZERO, IntegerWeight.ONE)
        val str = indexed.toString()

        assertTrue(str.contains("IndexedZSet"))
        assertTrue(str.contains("a"))
    }

    // ========== forEachLeaf Tests ==========

    @Test
    fun `test forEachLeaf on simple IndexedZSet`() {
        val zset = ZSet.fromMap(mapOf("a" to IntegerWeight(1), "b" to IntegerWeight(2)))
        val indexed = IndexedZSet.singleton("key", zset, IntegerWeight.ZERO, IntegerWeight.ONE)

        val collected = mutableListOf<Pair<List<Any?>, IntegerWeight>>()
        indexed.forEachLeaf { prefix, weight -> collected.add(prefix to weight) }

        assertEquals(2, collected.size)
        assertTrue(collected.any { it.first == listOf("key", "a") && it.second == IntegerWeight(1) })
        assertTrue(collected.any { it.first == listOf("key", "b") && it.second == IntegerWeight(2) })
    }

    @Test
    fun `test forEachLeaf on nested IndexedZSet`() {
        // Create 3-level structure: {1 -> {:name -> {"John" -> 1, "Jane" -> 2}}}
        val nameValues = ZSet.fromMap(mapOf("John" to IntegerWeight(1), "Jane" to IntegerWeight(2)))
        val attributes = IndexedZSet.singleton(":name", nameValues, IntegerWeight.ZERO, IntegerWeight.ONE)
        val entities = IndexedZSet.singleton(1, attributes, IntegerWeight.ZERO, IntegerWeight.ONE)

        val collected = mutableListOf<Pair<List<Any?>, IntegerWeight>>()
        entities.forEachLeaf { prefix, weight -> collected.add(prefix to weight) }

        assertEquals(2, collected.size)
        assertTrue(collected.any { it.first == listOf(1, ":name", "John") && it.second == IntegerWeight(1) })
        assertTrue(collected.any { it.first == listOf(1, ":name", "Jane") && it.second == IntegerWeight(2) })
    }

    @Test
    fun `test forEachLeaf on empty IndexedZSet`() {
        val empty = IndexedZSet.empty<String, IntegerWeight>(IntegerWeight.ZERO, IntegerWeight.ONE)

        val collected = mutableListOf<Pair<List<Any?>, IntegerWeight>>()
        empty.forEachLeaf { prefix, weight -> collected.add(prefix to weight) }

        assertTrue(collected.isEmpty())
    }

    @Test
    fun `test forEachLeaf with multiple keys at each level`() {
        val zset1 = ZSet.fromMap(mapOf(1 to IntegerWeight(1)))
        val zset2 = ZSet.fromMap(mapOf(2 to IntegerWeight(2)))
        val indexed = IndexedZSet.fromMap(mapOf("a" to zset1, "b" to zset2), IntegerWeight.ZERO, IntegerWeight.ONE)

        val collected = mutableListOf<Pair<List<Any?>, IntegerWeight>>()
        indexed.forEachLeaf { prefix, weight -> collected.add(prefix to weight) }

        assertEquals(2, collected.size)
        assertTrue(collected.any { it.first == listOf("a", 1) && it.second == IntegerWeight(1) })
        assertTrue(collected.any { it.first == listOf("b", 2) && it.second == IntegerWeight(2) })
    }

    // ========== extendLeaves Tests ==========

    @Test
    fun `test extendLeaves on simple IndexedZSet`() {
        val zset = ZSet.fromMap(mapOf("x" to IntegerWeight(1)))
        val indexed = IndexedZSet.singleton("key", zset, IntegerWeight.ZERO, IntegerWeight.ONE)

        val extended = indexed.extendLeaves { prefix, weight ->
            // Extend each leaf with a new ZSet based on the prefix
            ZSet.singleton("extended-${prefix.last()}", weight)
        }

        // The result should have depth 3 now (key -> x -> extended-x)
        assertEquals(3, extended.depth())

        val collected = mutableListOf<Pair<List<Any?>, IntegerWeight>>()
        extended.forEachLeaf { prefix, weight -> collected.add(prefix to weight) }

        assertEquals(1, collected.size)
        assertEquals(listOf("key", "x", "extended-x"), collected[0].first)
        assertEquals(IntegerWeight(1), collected[0].second)
    }

    @Test
    fun `test extendLeaves with multiple leaves`() {
        val zset = ZSet.fromMap(mapOf("a" to IntegerWeight(1), "b" to IntegerWeight(2)))
        val indexed = IndexedZSet.singleton("key", zset, IntegerWeight.ZERO, IntegerWeight.ONE)

        val extended = indexed.extendLeaves { prefix, weight ->
            // Each leaf gets extended with its own value
            ZSet.singleton("child-${prefix.last()}", weight)
        }

        val collected = mutableListOf<Pair<List<Any?>, IntegerWeight>>()
        extended.forEachLeaf { prefix, weight -> collected.add(prefix to weight) }

        assertEquals(2, collected.size)
        assertTrue(collected.any { it.first == listOf("key", "a", "child-a") && it.second == IntegerWeight(1) })
        assertTrue(collected.any { it.first == listOf("key", "b", "child-b") && it.second == IntegerWeight(2) })
    }

    @Test
    fun `test extendLeaves on nested IndexedZSet`() {
        // Create 3-level structure: {"entity" -> {":name" -> {"John" -> 1}}}
        val nameValues = ZSet.singleton("John", IntegerWeight.ONE)
        val attributes = IndexedZSet.singleton(":name", nameValues, IntegerWeight.ZERO, IntegerWeight.ONE)
        val entities = IndexedZSet.singleton("entity", attributes, IntegerWeight.ZERO, IntegerWeight.ONE)

        val extended = entities.extendLeaves { _, weight ->
            ZSet.singleton("extended", weight)
        }

        // Should now be 4 levels deep
        assertEquals(4, extended.depth())

        val collected = mutableListOf<Pair<List<Any?>, IntegerWeight>>()
        extended.forEachLeaf { prefix, weight -> collected.add(prefix to weight) }

        assertEquals(1, collected.size)
        assertEquals(listOf("entity", ":name", "John", "extended"), collected[0].first)
    }

    @Test
    fun `test extendLeaves with empty extension filters out`() {
        val zset = ZSet.fromMap(mapOf("keep" to IntegerWeight(1), "remove" to IntegerWeight(2)))
        val indexed = IndexedZSet.singleton("key", zset, IntegerWeight.ZERO, IntegerWeight.ONE)

        val extended = indexed.extendLeaves { prefix, weight ->
            if (prefix.last() == "keep") {
                ZSet.singleton("child", weight)
            } else {
                ZSet.empty()  // Empty extension should be filtered
            }
        }

        val collected = mutableListOf<Pair<List<Any?>, IntegerWeight>>()
        extended.forEachLeaf { prefix, weight -> collected.add(prefix to weight) }

        assertEquals(1, collected.size)
        assertEquals(listOf("key", "keep", "child"), collected[0].first)
    }

    @Test
    fun `test extendLeaves on empty IndexedZSet`() {
        val empty = IndexedZSet.empty<String, IntegerWeight>(IntegerWeight.ZERO, IntegerWeight.ONE)

        val extended = empty.extendLeaves { _, weight ->
            ZSet.singleton("child", weight)
        }

        assertTrue(extended.isEmpty())
    }

    @Test
    fun `test extendLeaves preserves weights`() {
        val zset = ZSet.fromMap(mapOf("a" to IntegerWeight(5), "b" to IntegerWeight(-3)))
        val indexed = IndexedZSet.singleton("key", zset, IntegerWeight.ZERO, IntegerWeight.ONE)

        val extended = indexed.extendLeaves { _, weight ->
            // Pass through the weight
            ZSet.singleton("child", weight)
        }

        val collected = mutableListOf<Pair<List<Any?>, IntegerWeight>>()
        extended.forEachLeaf { prefix, weight -> collected.add(prefix to weight) }

        assertEquals(2, collected.size)
        assertTrue(collected.any { it.first == listOf("key", "a", "child") && it.second == IntegerWeight(5) })
        assertTrue(collected.any { it.first == listOf("key", "b", "child") && it.second == IntegerWeight(-3) })
    }

    @Test
    fun `test extendLeaves with multiple children per leaf`() {
        val zset = ZSet.singleton("parent", IntegerWeight.ONE)
        val indexed = IndexedZSet.singleton("key", zset, IntegerWeight.ZERO, IntegerWeight.ONE)

        val extended = indexed.extendLeaves { _, _ ->
            // Each leaf gets multiple children
            ZSet.fromMap(mapOf("child1" to IntegerWeight(1), "child2" to IntegerWeight(2)))
        }

        val collected = mutableListOf<Pair<List<Any?>, IntegerWeight>>()
        extended.forEachLeaf { prefix, weight -> collected.add(prefix to weight) }

        assertEquals(2, collected.size)
        assertTrue(collected.any { it.first == listOf("key", "parent", "child1") && it.second == IntegerWeight(1) })
        assertTrue(collected.any { it.first == listOf("key", "parent", "child2") && it.second == IntegerWeight(2) })
    }

    // ========== Arbitrary Depth Nesting Tests ==========

    @Test
    fun `test 3-level nested structure`() {
        // Level 1: ZSet<String, IntegerWeight>
        val nameValues = ZSet.fromMap(mapOf("John" to IntegerWeight(1), "Jane" to IntegerWeight(1)))

        // Level 2: IndexedZSet<String, IntegerWeight> - attributes indexed by attribute name
        val attributes = IndexedZSet.singleton(":name", nameValues, IntegerWeight.ZERO, IntegerWeight.ONE)

        // Level 3: IndexedZSet<Int, IntegerWeight> - entities indexed by entity ID
        val entities = IndexedZSet.singleton(1, attributes, IntegerWeight.ZERO, IntegerWeight.ONE)

        // Verify structure
        assertEquals(1, entities.size)
        assertEquals(3, entities.depth())

        // Access nested data
        val attrs = entities.getTyped<String, IndexedZSet<String, IntegerWeight>>(1)
        assertNotNull(attrs)
        assertEquals(1, attrs!!.size)

        val names = attrs.getTyped<String, ZSet<String, IntegerWeight>>(":name")
        assertNotNull(names)
        assertEquals(2, names!!.size)
        assertEquals(IntegerWeight(1), names.weight("John"))
        assertEquals(IntegerWeight(1), names.weight("Jane"))
    }

    @Test
    fun `test 3-level nested addition`() {
        // Create first 3-level structure: {1 -> {:name -> {"John" -> 1}}}
        val nameValues1 = ZSet.singleton("John", IntegerWeight.ONE)
        val attributes1 = IndexedZSet.singleton(":name", nameValues1, IntegerWeight.ZERO, IntegerWeight.ONE)
        val entities1 = IndexedZSet.singleton(1, attributes1, IntegerWeight.ZERO, IntegerWeight.ONE)

        // Create second 3-level structure: {1 -> {:age -> {30 -> 1}}}
        val ageValues = ZSet.singleton(30, IntegerWeight.ONE)
        val attributes2 = IndexedZSet.singleton(":age", ageValues, IntegerWeight.ZERO, IntegerWeight.ONE)
        val entities2 = IndexedZSet.singleton(1, attributes2, IntegerWeight.ZERO, IntegerWeight.ONE)

        // Add them
        val result = entities1.add(entities2)

        // Should have one entity with two attributes
        assertEquals(1, result.size)
        val attrs = result.getTyped<String, IndexedZSet<String, IntegerWeight>>(1)!!
        assertEquals(2, attrs.size)

        val names = attrs.getTyped<String, ZSet<String, IntegerWeight>>(":name")!!
        assertEquals(IntegerWeight(1), names.weight("John"))

        val ages = attrs.getTyped<Any, ZSet<Any, IntegerWeight>>(":age")!!
        assertEquals(IntegerWeight(1), ages.weight(30))
    }
}