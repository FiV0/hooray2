package org.hooray.dbsp

import org.hooray.incremental.IntegerWeight
import org.hooray.incremental.ZSet
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

class GroupOpsTest {

    private fun zset(vararg pairs: Pair<Tuple, Int>): TupleZSet {
        val m = HashMap<Tuple, IntegerWeight>()
        for ((t, w) in pairs) m.merge(t, IntegerWeight(w)) { a, b -> a.add(b) }
        return ZSet.fromMap(m)
    }

    private val a = Tuple.of("a")
    private val b = Tuple.of("b")

    // --- Plus / Minus ---

    @Test
    fun `plus adds two z-sets`() {
        val out = PlusOp().eval(zset(a to 1, b to 2), zset(a to 3))
        assertEquals(zset(a to 4, b to 2), out)
    }

    @Test
    fun `plus with the empty z-set is the identity`() {
        val z = zset(a to 1, b to -2)
        assertEquals(z, PlusOp().eval(z, emptyTupleZSet()))
        assertEquals(z, PlusOp().eval(emptyTupleZSet(), z))
    }

    @Test
    fun `plus is commutative`() {
        val x = zset(a to 1, b to 5)
        val y = zset(a to -3, b to 2)
        assertEquals(PlusOp().eval(x, y), PlusOp().eval(y, x))
    }

    @Test
    fun `minus subtracts two z-sets`() {
        val out = MinusOp().eval(zset(a to 5, b to 2), zset(a to 3, b to 2))
        assertEquals(zset(a to 2), out)
    }

    @Test
    fun `a minus a is empty`() {
        val z = zset(a to 1, b to -2)
        assertTrue(MinusOp().eval(z, z).isEmpty())
    }

    // --- Distinct ---

    @Test
    fun `distinct emits plus one when a tuple becomes present`() {
        assertEquals(zset(a to 1), DistinctOp().eval(zset(a to 1)))
    }

    @Test
    fun `distinct stays silent while a tuple remains present`() {
        val d = DistinctOp()
        assertEquals(zset(a to 1), d.eval(zset(a to 1)))
        assertTrue(d.eval(zset(a to 1)).isEmpty())   // weight 1 -> 2, still present
        assertTrue(d.eval(zset(a to 3)).isEmpty())   // weight 2 -> 5, still present
    }

    @Test
    fun `distinct emits minus one when a tuple becomes absent`() {
        val d = DistinctOp()
        d.eval(zset(a to 2))                          // present, weight 2
        assertTrue(d.eval(zset(a to -1)).isEmpty())   // weight 2 -> 1, still present
        assertEquals(zset(a to -1), d.eval(zset(a to -1)))  // weight 1 -> 0, now absent
    }

    @Test
    fun `distinct threshold crossing over a full sequence`() {
        val d = DistinctOp()
        assertEquals(zset(a to 1), d.eval(zset(a to 1)))   // 0 -> 1
        assertTrue(d.eval(zset(a to 1)).isEmpty())          // 1 -> 2
        assertTrue(d.eval(zset(a to -1)).isEmpty())         // 2 -> 1
        assertEquals(zset(a to -1), d.eval(zset(a to -1)))  // 1 -> 0
    }

    @Test
    fun `distinct treats non-positive accumulated weight as absent`() {
        val d = DistinctOp()
        assertTrue(d.eval(zset(a to -2)).isEmpty())         // 0 -> -2, still absent
        assertEquals(zset(a to 1), d.eval(zset(a to 3)))    // -2 -> 1, now present
    }
}
