package org.hooray.incremental

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

class IndexedZSetTest {

    // ========== Core Functionality Tests ==========

    @Test
    fun `test empty IndexedZSet`() {
        val empty = IndexedZSet.empty<String, Int>()
        assertTrue(empty.isEmpty())
        assertEquals(0, empty.size)
        assertNull(empty.get("any"))
    }

    @Test
    fun `test singleton IndexedZSet`() {
        val zset = ZSet.fromMap(mapOf(1 to IntegerWeight(2), 2 to IntegerWeight(3)))
        val indexed = IndexedZSet.singleton("key", zset)

        assertEquals(1, indexed.size)
        assertFalse(indexed.isEmpty())
        assertEquals(zset, indexed.get("key"))
    }

    @Test
    fun `test singleton with empty ZSet creates empty IndexedZSet`() {
        val emptyZSet = ZSet.empty<Int>()
        val indexed = IndexedZSet.singleton("key", emptyZSet)

        assertTrue(indexed.isEmpty())
    }

    @Test
    fun `test fromMap filters empty ZSets`() {
        val zset1 = ZSet.fromMap(mapOf(1 to IntegerWeight(1)))
        val zset2 = ZSet.empty<Int>()
        val zset3 = ZSet.fromMap(mapOf(2 to IntegerWeight(2)))

        val map = mapOf("a" to zset1, "b" to zset2, "c" to zset3)
        val indexed = IndexedZSet.fromMap(map, IntegerWeight.ZERO)

        assertEquals(2, indexed.size)
        assertNotNull(indexed.get("a"))
        assertNull(indexed.get("b"))  // Empty ZSet filtered out
        assertNotNull(indexed.get("c"))
    }

    @Test
    fun `test index groups correctly`() {
        val zset = ZSet.fromCollection(listOf("apple", "apricot", "banana", "avocado"))
        val indexed = zset.index { it.first() }

        assertEquals(2, indexed.size)

        val aGroup = indexed.get('a')
        assertNotNull(aGroup)
        assertEquals(3, aGroup!!.size)
        assertEquals(IntegerWeight(1), aGroup.weight("apple"))
        assertEquals(IntegerWeight(1), aGroup.weight("apricot"))
        assertEquals(IntegerWeight(1), aGroup.weight("avocado"))

        val bGroup = indexed.get('b')
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

        val aGroup = indexed.get('a')!!
        assertEquals(IntegerWeight(2), aGroup.weight("a1"))
        assertEquals(IntegerWeight(3), aGroup.weight("a2"))

        val bGroup = indexed.get('b')!!
        assertEquals(IntegerWeight(5), bGroup.weight("b1"))
    }

    @Test
    fun `test index with all values mapping to same key`() {
        val zset = ZSet.fromCollection(listOf("a", "b", "c"))
        val indexed = zset.index { "same" }

        assertEquals(1, indexed.size)
        val group = indexed.get("same")!!
        assertEquals(3, group.size)
    }

    @Test
    fun `test index with each value mapping to different key`() {
        val zset = ZSet.fromCollection(listOf("a", "b", "c"))
        val indexed = zset.index { it }

        assertEquals(3, indexed.size)
        assertNotNull(indexed.get("a"))
        assertNotNull(indexed.get("b"))
        assertNotNull(indexed.get("c"))
    }

    @Test
    fun `test flatten reconstructs ZSet`() {
        val zset1 = ZSet.fromMap(mapOf(1 to IntegerWeight(2), 2 to IntegerWeight(3)))
        val zset2 = ZSet.fromMap(mapOf(3 to IntegerWeight(1), 4 to IntegerWeight(4)))

        val indexed = IndexedZSet.fromMap(mapOf("a" to zset1, "b" to zset2), IntegerWeight.ZERO)
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

        val indexed = IndexedZSet.fromMap(mapOf("a" to zset1, "b" to zset2), IntegerWeight.ZERO)
        val deindexed = indexed.deindex()

        assertEquals(3, deindexed.size)
        assertEquals(IntegerWeight(2), deindexed.weight(1))
        assertEquals(IntegerWeight(3), deindexed.weight(2))
        assertEquals(IntegerWeight(1), deindexed.weight(3))
    }

    @Test
    fun `test deindex with overlapping values adds weights`() {
        val zset1 = ZSet.fromMap(mapOf(1 to IntegerWeight(2)))
        val zset2 = ZSet.fromMap(mapOf(1 to IntegerWeight(3)))

        val indexed = IndexedZSet.fromMap(mapOf("a" to zset1, "b" to zset2), IntegerWeight.ZERO)
        val deindexed = indexed.deindex()

        assertEquals(1, deindexed.size)
        assertEquals(IntegerWeight(5), deindexed.weight(1))
    }

    @Test
    fun `test join with matching keys`() {
        val left = ZSet.fromCollection(listOf("a1", "a2")).index { it.first() }
        val right = ZSet.fromCollection(listOf("a3", "a4")).index { it.first() }

        val joined = left.join(right) { l, r -> "$l-$r" }

        assertEquals(1, joined.size)
        val aGroup = joined.get('a')!!
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

        val joined = left.join(right) { l, r -> "$l-$r" }

        assertTrue(joined.isEmpty())
    }

    @Test
    fun `test join multiplies weights correctly`() {
        val leftZSet = ZSet.fromMap(mapOf("x" to IntegerWeight(2), "y" to IntegerWeight(3)))
        val rightZSet = ZSet.fromMap(mapOf("p" to IntegerWeight(4), "q" to IntegerWeight(5)))

        val left = IndexedZSet.singleton("key", leftZSet)
        val right = IndexedZSet.singleton("key", rightZSet)

        val joined = left.join(right) { l, r -> "$l-$r" }

        val group = joined.get("key")!!
        assertEquals(IntegerWeight(8), group.weight("x-p"))   // 2 * 4 = 8
        assertEquals(IntegerWeight(10), group.weight("x-q"))  // 2 * 5 = 10
        assertEquals(IntegerWeight(12), group.weight("y-p"))  // 3 * 4 = 12
        assertEquals(IntegerWeight(15), group.weight("y-q"))  // 3 * 5 = 15
    }

    @Test
    fun `test join with some matching and some disjoint keys`() {
        val left = ZSet.fromCollection(listOf("a1", "b1")).index { it.first() }
        val right = ZSet.fromCollection(listOf("a2", "c1")).index { it.first() }

        val joined = left.join(right) { l, r -> "$l-$r" }

        assertEquals(1, joined.size)
        assertNotNull(joined.get('a'))
        assertNull(joined.get('b'))
        assertNull(joined.get('c'))
    }

    // ========== Algebraic Properties Tests ==========

    @Test
    fun `test add with disjoint keys`() {
        val zset1 = ZSet.fromMap(mapOf(1 to IntegerWeight(1)))
        val zset2 = ZSet.fromMap(mapOf(2 to IntegerWeight(2)))

        val indexed1 = IndexedZSet.singleton("a", zset1)
        val indexed2 = IndexedZSet.singleton("b", zset2)

        val result = indexed1.add(indexed2)

        assertEquals(2, result.size)
        assertNotNull(result.get("a"))
        assertNotNull(result.get("b"))
    }

    @Test
    fun `test add with overlapping keys combines ZSets`() {
        val zset1 = ZSet.fromMap(mapOf(1 to IntegerWeight(2)))
        val zset2 = ZSet.fromMap(mapOf(2 to IntegerWeight(3)))

        val indexed1 = IndexedZSet.singleton("a", zset1)
        val indexed2 = IndexedZSet.singleton("a", zset2)

        val result = indexed1.add(indexed2)

        assertEquals(1, result.size)
        val aGroup = result.get("a")!!
        assertEquals(2, aGroup.size)
        assertEquals(IntegerWeight(2), aGroup.weight(1))
        assertEquals(IntegerWeight(3), aGroup.weight(2))
    }

    @Test
    fun `test add removes keys with empty ZSets`() {
        val zset1 = ZSet.fromMap(mapOf(1 to IntegerWeight(5)))
        val zset2 = ZSet.fromMap(mapOf(1 to IntegerWeight(-5)))

        val indexed1 = IndexedZSet.singleton("a", zset1)
        val indexed2 = IndexedZSet.singleton("a", zset2)

        val result = indexed1.add(indexed2)

        assertTrue(result.isEmpty())
    }

    @Test
    fun `test negate inverts all weights`() {
        val zset = ZSet.fromMap(mapOf(1 to IntegerWeight(3), 2 to IntegerWeight(-2)))
        val indexed = IndexedZSet.singleton("a", zset)

        val negated = indexed.negate()

        val aGroup = negated.get("a")!!
        assertEquals(IntegerWeight(-3), aGroup.weight(1))
        assertEquals(IntegerWeight(2), aGroup.weight(2))
    }

    @Test
    fun `test double negation returns original`() {
        val zset = ZSet.fromMap(mapOf(1 to IntegerWeight(5), 2 to IntegerWeight(-3)))
        val indexed = IndexedZSet.singleton("a", zset)

        assertEquals(indexed, indexed.negate().negate())
    }

    @Test
    fun `test subtract uses add and negate`() {
        val zset1 = ZSet.fromMap(mapOf(1 to IntegerWeight(5)))
        val zset2 = ZSet.fromMap(mapOf(1 to IntegerWeight(2)))

        val indexed1 = IndexedZSet.singleton("a", zset1)
        val indexed2 = IndexedZSet.singleton("a", zset2)

        val result = indexed1.subtract(indexed2)

        val aGroup = result.get("a")!!
        assertEquals(IntegerWeight(3), aGroup.weight(1))
    }

    @Test
    fun `test addition is commutative`() {
        val zset1 = ZSet.fromMap(mapOf(1 to IntegerWeight(2)))
        val zset2 = ZSet.fromMap(mapOf(2 to IntegerWeight(3)))

        val indexed1 = IndexedZSet.singleton("a", zset1)
        val indexed2 = IndexedZSet.singleton("b", zset2)

        assertEquals(indexed1.add(indexed2), indexed2.add(indexed1))
    }

    @Test
    fun `test addition is associative`() {
        val zset1 = ZSet.fromMap(mapOf(1 to IntegerWeight(1)))
        val zset2 = ZSet.fromMap(mapOf(2 to IntegerWeight(2)))
        val zset3 = ZSet.fromMap(mapOf(3 to IntegerWeight(3)))

        val indexed1 = IndexedZSet.singleton("a", zset1)
        val indexed2 = IndexedZSet.singleton("b", zset2)
        val indexed3 = IndexedZSet.singleton("c", zset3)

        assertEquals(
            indexed1.add(indexed2).add(indexed3),
            indexed1.add(indexed2.add(indexed3))
        )
    }

    @Test
    fun `test empty is identity for addition`() {
        val zset = ZSet.fromMap(mapOf(1 to IntegerWeight(5)))
        val indexed = IndexedZSet.singleton("a", zset)
        val empty = IndexedZSet.empty<String, Int>()

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
        val empty = IndexedZSet.empty<String, Int>()
        val flattened = empty.flatten { k, v -> "$k-$v" }

        assertTrue(flattened.isEmpty())
    }

    @Test
    fun `test deindex on empty IndexedZSet`() {
        val empty = IndexedZSet.empty<String, Int>()
        val deindexed = empty.deindex()

        assertTrue(deindexed.isEmpty())
    }

    @Test
    fun `test join on empty IndexedZSets`() {
        val empty1 = IndexedZSet.empty<String, Int>()
        val empty2 = IndexedZSet.empty<String, Int>()

        val joined = empty1.join(empty2) { l, r -> l + r }

        assertTrue(joined.isEmpty())
    }

    @Test
    fun `test join with weights that cancel to zero`() {
        val leftZSet = ZSet.fromMap(mapOf("x" to IntegerWeight(2), "y" to IntegerWeight(-2)))
        val rightZSet = ZSet.fromMap(mapOf("p" to IntegerWeight(3)))

        val left = IndexedZSet.singleton("key", leftZSet)
        val right = IndexedZSet.singleton("key", rightZSet)

        val joined = left.join(right) { l, _ -> l }

        // x*3 = 6, y*3 = -6, they map to different combined values so don't cancel
        val group = joined.get("key")!!
        assertEquals(IntegerWeight(6), group.weight("x"))
        assertEquals(IntegerWeight(-6), group.weight("y"))
    }

    @Test
    fun `test entries returns all key-ZSet pairs`() {
        val zset1 = ZSet.fromMap(mapOf(1 to IntegerWeight(1)))
        val zset2 = ZSet.fromMap(mapOf(2 to IntegerWeight(2)))

        val indexed = IndexedZSet.fromMap(mapOf("a" to zset1, "b" to zset2), IntegerWeight.ZERO)
        val entries = indexed.entries()

        assertEquals(2, entries.size)
        assertTrue(entries.any { it.key == "a" })
        assertTrue(entries.any { it.key == "b" })
    }

    @Test
    fun `test keys returns all keys`() {
        val zset1 = ZSet.fromMap(mapOf(1 to IntegerWeight(1)))
        val zset2 = ZSet.fromMap(mapOf(2 to IntegerWeight(2)))

        val indexed = IndexedZSet.fromMap(mapOf("a" to zset1, "b" to zset2), IntegerWeight.ZERO)
        val keys = indexed.keys()

        assertEquals(2, keys.size)
        assertTrue(keys.contains("a"))
        assertTrue(keys.contains("b"))
    }

    // ========== Integration Tests ==========

    @Test
    fun `test round trip index then deindex preserves multiset`() {
        val original = ZSet.fromCollection(listOf("apple", "apricot", "banana", "apple"))
        val indexed = original.index { it.first() }
        val deindexed = indexed.deindex()

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

        val joined = ordersIndexed.join(customersIndexed) { order, customer ->
            order + "-" + customer
        }

        assertEquals(2, joined.size)

        val customer1Group = joined.get("customer1")!!
        assertEquals(2, customer1Group.size)  // 2 orders * 1 customer = 2

        val customer2Group = joined.get("customer2")!!
        assertEquals(1, customer2Group.size)  // 1 order * 1 customer = 1
    }

    @Test
    fun `test chained operations`() {
        val zset = ZSet.fromCollection(listOf("a1", "a2", "b1", "b2", "b3"))

        val indexed = zset.index { it.first() }
        val negated = indexed.negate()
        val doubleNegated = negated.negate()
        val deindexed = doubleNegated.deindex()

        // After double negation and deindexing, should equal original
        assertEquals(zset, deindexed)
    }

    @Test
    fun `test equals and hashCode`() {
        val zset = ZSet.fromMap(mapOf(1 to IntegerWeight(1)))

        val indexed1 = IndexedZSet.singleton("a", zset)
        val indexed2 = IndexedZSet.singleton("a", zset)
        val indexed3 = IndexedZSet.singleton("b", zset)

        assertEquals(indexed1, indexed2)
        assertEquals(indexed1.hashCode(), indexed2.hashCode())
        assertNotEquals(indexed1, indexed3)
    }

    @Test
    fun `test toString`() {
        val zset = ZSet.fromMap(mapOf(1 to IntegerWeight(1)))
        val indexed = IndexedZSet.singleton("a", zset)
        val str = indexed.toString()

        assertTrue(str.contains("IndexedZSet"))
        assertTrue(str.contains("a"))
    }
}
