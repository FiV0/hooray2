package org.hooray.dbsp

import org.hooray.incremental.IntegerWeight
import org.hooray.incremental.ZSet
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

class PrimitiveOpsTest {

    private fun zset(vararg pairs: Pair<Tuple, Int>): TupleZSet {
        val m = HashMap<Tuple, IntegerWeight>()
        for ((t, w) in pairs) m.merge(t, IntegerWeight(w)) { a, b -> a.add(b) }
        return ZSet.fromMap(m)
    }

    private val a = Tuple.of("a")
    private val b = Tuple.of("b")
    private val c = Tuple.of("c")

    @Test
    fun `integrate accumulates the running sum`() {
        val i = IntegrateOp()
        assertEquals(zset(a to 1), i.eval(zset(a to 1)))
        assertEquals(zset(a to 1, b to 1), i.eval(zset(b to 1)))
        assertEquals(zset(b to 1), i.eval(zset(a to -1)))   // a cancels out
    }

    @Test
    fun `differentiate yields the change between states`() {
        val d = DifferentiateOp()
        assertEquals(zset(a to 1), d.eval(zset(a to 1)))
        assertEquals(zset(b to 1), d.eval(zset(a to 1, b to 1)))
        assertEquals(zset(b to -1), d.eval(zset(a to 1)))
    }

    @Test
    fun `z1 delays the stream by one step`() {
        val z = Z1Op()
        assertTrue(z.eval(zset(a to 1)).isEmpty())          // first step: empty
        assertEquals(zset(a to 1), z.eval(zset(b to 1)))
        assertEquals(zset(b to 1), z.eval(zset(c to 1)))
    }

    @Test
    fun `difference subtracts pointwise`() {
        val difference = DifferenceOp()
        assertEquals(
            zset(a to 1, b to 1, c to -1),
            difference.eval(zset(a to 1, b to 2), zset(b to 1, c to 1)),
        )
    }

    @Test
    fun `D after I is the identity`() {
        val i = IntegrateOp()
        val d = DifferentiateOp()
        val deltas = listOf(
            zset(a to 1, b to 2),
            zset(a to -1, c to 3),
            zset(b to -2),
            zset(c to -3, a to 5),
        )
        for (delta in deltas) {
            assertEquals(delta, d.eval(i.eval(delta)))
        }
    }

    @Test
    fun `I after D is the identity`() {
        val d = DifferentiateOp()
        val i = IntegrateOp()
        val states = listOf(
            zset(a to 1),
            zset(a to 1, b to 1),
            zset(a to 1, b to 1, c to 4),
            zset(c to 4),
        )
        for (state in states) {
            assertEquals(state, i.eval(d.eval(state)))
        }
    }
}
