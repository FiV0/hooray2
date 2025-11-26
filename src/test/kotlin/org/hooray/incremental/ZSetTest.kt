package org.hooray.incremental

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.assertThrows

class ZSetTest {

    @Test
    fun `test empty ZSet`() {
        val empty = ZSet.empty<String>()
        assertTrue(empty.isEmpty())
        assertEquals(0, empty.size())
        assertEquals(IntegerWeight.ZERO, empty.weight("any"))
    }

    @Test
    fun `test singleton ZSet`() {
        val single = ZSet.singleton("hello", IntegerWeight(5))
        assertFalse(single.isEmpty())
        assertEquals(1, single.size())
        assertEquals(IntegerWeight(5), single.weight("hello"))
        assertEquals(IntegerWeight.ZERO, single.weight("world"))
    }

    @Test
    fun `test singleton with zero weight creates empty ZSet`() {
        val single = ZSet.singleton("hello", IntegerWeight.ZERO)
        assertTrue(single.isEmpty())
    }

    @Test
    fun `test fromCollection with unique elements`() {
        val zset = ZSet.fromCollection(listOf("a", "b", "c"))
        assertEquals(3, zset.size())
        assertEquals(IntegerWeight.ONE, zset.weight("a"))
        assertEquals(IntegerWeight.ONE, zset.weight("b"))
        assertEquals(IntegerWeight.ONE, zset.weight("c"))
    }

    @Test
    fun `test fromCollection with duplicates`() {
        val zset = ZSet.fromCollection(listOf("a", "b", "a", "c", "a"))
        assertEquals(3, zset.size())
        assertEquals(IntegerWeight(3), zset.weight("a"))
        assertEquals(IntegerWeight(1), zset.weight("b"))
        assertEquals(IntegerWeight(1), zset.weight("c"))
    }

    @Test
    fun `test fromMap filters zero weights`() {
        val map = mapOf(
            "a" to IntegerWeight(1),
            "b" to IntegerWeight(0),
            "c" to IntegerWeight(2)
        )
        val zset = ZSet.fromMap(map)
        assertEquals(2, zset.size())
        assertEquals(IntegerWeight(1), zset.weight("a"))
        assertEquals(IntegerWeight.ZERO, zset.weight("b"))
        assertEquals(IntegerWeight(2), zset.weight("c"))
    }

    @Test
    fun `test add ZSets with disjoint keys`() {
        val zset1 = ZSet.fromCollection(listOf("a", "b"))
        val zset2 = ZSet.fromCollection(listOf("c", "d"))
        val result = zset1.add(zset2)

        assertEquals(4, result.size())
        assertEquals(IntegerWeight(1), result.weight("a"))
        assertEquals(IntegerWeight(1), result.weight("b"))
        assertEquals(IntegerWeight(1), result.weight("c"))
        assertEquals(IntegerWeight(1), result.weight("d"))
    }

    @Test
    fun `test add ZSets with overlapping keys`() {
        val zset1 = ZSet.fromMap(mapOf("a" to IntegerWeight(2), "b" to IntegerWeight(3)))
        val zset2 = ZSet.fromMap(mapOf("b" to IntegerWeight(1), "c" to IntegerWeight(4)))
        val result = zset1.add(zset2)

        assertEquals(3, result.size())
        assertEquals(IntegerWeight(2), result.weight("a"))
        assertEquals(IntegerWeight(4), result.weight("b"))
        assertEquals(IntegerWeight(4), result.weight("c"))
    }

    @Test
    fun `test add removes zero weights`() {
        val zset1 = ZSet.fromMap(mapOf("a" to IntegerWeight(5)))
        val zset2 = ZSet.fromMap(mapOf("a" to IntegerWeight(-5)))
        val result = zset1.add(zset2)

        assertTrue(result.isEmpty())
        assertEquals(IntegerWeight.ZERO, result.weight("a"))
    }

    @Test
    fun `test negate`() {
        val zset = ZSet.fromMap(mapOf("a" to IntegerWeight(3), "b" to IntegerWeight(-2)))
        val negated = zset.negate()

        assertEquals(2, negated.size())
        assertEquals(IntegerWeight(-3), negated.weight("a"))
        assertEquals(IntegerWeight(2), negated.weight("b"))
    }

    @Test
    fun `test subtract`() {
        val zset1 = ZSet.fromMap(mapOf("a" to IntegerWeight(5), "b" to IntegerWeight(3)))
        val zset2 = ZSet.fromMap(mapOf("a" to IntegerWeight(2), "c" to IntegerWeight(1)))
        val result = zset1.subtract(zset2)

        assertEquals(3, result.size())
        assertEquals(IntegerWeight(3), result.weight("a"))
        assertEquals(IntegerWeight(3), result.weight("b"))
        assertEquals(IntegerWeight(-1), result.weight("c"))
    }

    @Test
    fun `test positive filters to positive weights only`() {
        val zset = ZSet.fromMap(mapOf(
            "a" to IntegerWeight(5),
            "b" to IntegerWeight(-3),
            "c" to IntegerWeight(1)
        ))
        val positive = zset.positive()

        assertEquals(2, positive.size())
        assertEquals(IntegerWeight(5), positive.weight("a"))
        assertEquals(IntegerWeight.ZERO, positive.weight("b"))
        assertEquals(IntegerWeight(1), positive.weight("c"))
    }

    @Test
    fun `test distinct creates set with weight 1`() {
        val zset = ZSet.fromMap(mapOf("a" to IntegerWeight(5), "b" to IntegerWeight(3)))
        val distinct = zset.distinct()

        assertEquals(2, distinct.size())
        assertEquals(IntegerWeight.ONE, distinct.weight("a"))
        assertEquals(IntegerWeight.ONE, distinct.weight("b"))
    }

    @Test
    fun `test multiply`() {
        val zset = ZSet.fromMap(mapOf("a" to IntegerWeight(2), "b" to IntegerWeight(3)))
        val multiplied = zset.multiply(3)

        assertEquals(2, multiplied.size())
        assertEquals(IntegerWeight(6), multiplied.weight("a"))
        assertEquals(IntegerWeight(9), multiplied.weight("b"))
    }

    @Test
    fun `test multiply by zero creates empty ZSet`() {
        val zset = ZSet.fromMap(mapOf("a" to IntegerWeight(2), "b" to IntegerWeight(3)))
        val multiplied = zset.multiply(0)

        assertTrue(multiplied.isEmpty())
    }

    @Test
    fun `test multiply by negative`() {
        val zset = ZSet.fromMap(mapOf("a" to IntegerWeight(2)))
        val multiplied = zset.multiply(-1)

        assertEquals(IntegerWeight(-2), multiplied.weight("a"))
    }

    @Test
    fun `test entries returns all non-zero weighted values`() {
        val zset = ZSet.fromMap(mapOf("a" to IntegerWeight(1), "b" to IntegerWeight(2)))
        val entries = zset.entries()

        assertEquals(2, entries.size)
        assertTrue(entries.any { it.key == "a" && it.value == IntegerWeight(1) })
        assertTrue(entries.any { it.key == "b" && it.value == IntegerWeight(2) })
    }

    @Test
    fun `test keys returns all values`() {
        val zset = ZSet.fromMap(mapOf("a" to IntegerWeight(1), "b" to IntegerWeight(2)))
        val keys = zset.keys()

        assertEquals(2, keys.size)
        assertTrue(keys.contains("a"))
        assertTrue(keys.contains("b"))
    }

    @Test
    fun `test equals and hashCode`() {
        val zset1 = ZSet.fromMap(mapOf("a" to IntegerWeight(1), "b" to IntegerWeight(2)))
        val zset2 = ZSet.fromMap(mapOf("a" to IntegerWeight(1), "b" to IntegerWeight(2)))
        val zset3 = ZSet.fromMap(mapOf("a" to IntegerWeight(1), "b" to IntegerWeight(3)))

        assertEquals(zset1, zset2)
        assertEquals(zset1.hashCode(), zset2.hashCode())
        assertNotEquals(zset1, zset3)
    }

    @Test
    fun `test toString`() {
        val zset = ZSet.fromMap(mapOf("a" to IntegerWeight(1)))
        val str = zset.toString()

        assertTrue(str.contains("ZSet"))
        assertTrue(str.contains("a"))
    }

    @Test
    fun `test IntegerWeight add with overflow detection`() {
        val weight1 = IntegerWeight(Integer.MAX_VALUE)
        val weight2 = IntegerWeight(1)

        assertThrows<ArithmeticException> {
            weight1.add(weight2)
        }
    }

    @Test
    fun `test IntegerWeight multiply with overflow detection`() {
        val weight = IntegerWeight(Integer.MAX_VALUE)

        assertThrows<ArithmeticException> {
            weight.multiply(2)
        }
    }

    @Test
    fun `test IntegerWeight operations`() {
        val w1 = IntegerWeight(5)
        val w2 = IntegerWeight(3)

        assertEquals(IntegerWeight(8), w1.add(w2))
        assertEquals(IntegerWeight(-5), w1.negate())
        assertEquals(IntegerWeight(15), w1.multiply(3))
        assertFalse(w1.isZero())
        assertTrue(IntegerWeight.ZERO.isZero())
    }

    @Test
    fun `test ZSet addition is commutative`() {
        val zset1 = ZSet.fromMap(mapOf("a" to IntegerWeight(2), "b" to IntegerWeight(3)))
        val zset2 = ZSet.fromMap(mapOf("b" to IntegerWeight(1), "c" to IntegerWeight(4)))

        assertEquals(zset1.add(zset2), zset2.add(zset1))
    }

    @Test
    fun `test ZSet addition is associative`() {
        val zset1 = ZSet.fromMap(mapOf("a" to IntegerWeight(1)))
        val zset2 = ZSet.fromMap(mapOf("b" to IntegerWeight(2)))
        val zset3 = ZSet.fromMap(mapOf("c" to IntegerWeight(3)))

        assertEquals(
            zset1.add(zset2).add(zset3),
            zset1.add(zset2.add(zset3))
        )
    }

    @Test
    fun `test ZSet empty is identity for addition`() {
        val zset = ZSet.fromMap(mapOf("a" to IntegerWeight(5)))
        val empty = ZSet.empty<String>()

        assertEquals(zset, zset.add(empty))
        assertEquals(zset, empty.add(zset))
    }

    @Test
    fun `test double negation returns original`() {
        val zset = ZSet.fromMap(mapOf("a" to IntegerWeight(5), "b" to IntegerWeight(-3)))
        assertEquals(zset, zset.negate().negate())
    }

    @Test
    fun `test IntegerWeight multiply(other) basic operation`() {
        val w1 = IntegerWeight(5)
        val w2 = IntegerWeight(3)

        assertEquals(IntegerWeight(15), w1.multiply(w2))
        assertEquals(IntegerWeight(15), w2.multiply(w1))
    }

    @Test
    fun `test IntegerWeight multiply(other) with zero`() {
        val w1 = IntegerWeight(5)
        val zero = IntegerWeight.ZERO

        assertEquals(IntegerWeight.ZERO, w1.multiply(zero))
        assertEquals(IntegerWeight.ZERO, zero.multiply(w1))
    }

    @Test
    fun `test IntegerWeight multiply(other) with negative values`() {
        val w1 = IntegerWeight(5)
        val w2 = IntegerWeight(-3)

        assertEquals(IntegerWeight(-15), w1.multiply(w2))
        assertEquals(IntegerWeight(-15), w2.multiply(w1))
    }

    @Test
    fun `test IntegerWeight multiply(other) with overflow detection`() {
        val w1 = IntegerWeight(Integer.MAX_VALUE)
        val w2 = IntegerWeight(2)

        assertThrows<ArithmeticException> {
            w1.multiply(w2)
        }
    }

    @Test
    fun `test IntegerWeight multiply(other) is commutative`() {
        val w1 = IntegerWeight(7)
        val w2 = IntegerWeight(11)

        assertEquals(w1.multiply(w2), w2.multiply(w1))
    }

    @Test
    fun `test generic ZSet empty with custom zero`() {
        val customZero = IntegerWeight(0)
        val empty = ZSet.empty<String, IntegerWeight>(customZero)

        assertTrue(empty.isEmpty())
        assertEquals(0, empty.size())
        assertEquals(customZero, empty.weight("any"))
    }

    @Test
    fun `test generic ZSet fromMap with custom zero`() {
        val customZero = IntegerWeight(0)
        val map = mapOf(
            "a" to IntegerWeight(1),
            "b" to IntegerWeight(0),
            "c" to IntegerWeight(2)
        )
        val zset = ZSet.fromMap(map, customZero)

        assertEquals(2, zset.size())
        assertEquals(IntegerWeight(1), zset.weight("a"))
        assertEquals(customZero, zset.weight("b"))
        assertEquals(IntegerWeight(2), zset.weight("c"))
    }
}
