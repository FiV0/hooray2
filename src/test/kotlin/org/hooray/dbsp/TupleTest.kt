package org.hooray.dbsp

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

class TupleTest {

    @Test
    fun `equal tuples are equal and hash equal`() {
        val a = Tuple.of(1, "x", listOf("nested"))
        val b = Tuple.of(1, "x", listOf("nested"))
        assertEquals(a, b)
        assertEquals(a.hashCode(), b.hashCode())
    }

    @Test
    fun `distinct tuples are not equal`() {
        assertNotEquals(Tuple.of(1, "x"), Tuple.of(1, "y"))
        assertNotEquals(Tuple.of(1, "x"), Tuple.of(1, "x", "z"))
    }

    @Test
    fun `tuple usable as a map key by value`() {
        val map = mutableMapOf(Tuple.of(1, 2) to "hit")
        assertEquals("hit", map[Tuple.of(1, 2)])
        assertNull(map[Tuple.of(2, 1)])
    }

    @Test
    fun `arity and indexed access`() {
        val t = Tuple.of("a", "b", "c")
        assertEquals(3, t.arity)
        assertEquals("a", t[0])
        assertEquals("c", t[2])
    }

    @Test
    fun `permute reorders columns`() {
        val t = Tuple.of("e", "v")
        assertEquals(Tuple.of("v", "e"), t.permute(intArrayOf(1, 0)))
    }

    @Test
    fun `permute projects and duplicates columns`() {
        val t = Tuple.of("a", "b", "c")
        assertEquals(Tuple.of("c"), t.permute(intArrayOf(2)))
        assertEquals(Tuple.of("b", "b"), t.permute(intArrayOf(1, 1)))
    }

    @Test
    fun `prefix and drop split a tuple`() {
        val t = Tuple.of("k1", "k2", "x", "y")
        assertEquals(Tuple.of("k1", "k2"), t.prefix(2))
        assertEquals(Tuple.of("x", "y"), t.drop(2))
    }

    @Test
    fun `concat joins two tuples`() {
        assertEquals(Tuple.of(1, 2, 3, 4), Tuple.of(1, 2).concat(Tuple.of(3, 4)))
    }

    @Test
    fun `empty tuple is the cartesian join key`() {
        assertEquals(0, Tuple.EMPTY.arity)
        assertEquals(Tuple.EMPTY, Tuple.of(1, 2).prefix(0))
        assertEquals(Tuple.of(1, 2), Tuple.EMPTY.concat(Tuple.of(1, 2)))
    }

    @Test
    fun `tuples hold nulls`() {
        val t = Tuple.of(null, "x")
        assertEquals(Tuple.of(null, "x"), t)
        assertNull(t[0])
    }
}
