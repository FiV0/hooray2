package org.hooray.dbsp

import org.hooray.incremental.IntegerWeight
import org.hooray.incremental.ZSet
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

class StreamJoinTest {

    private fun zset(vararg pairs: Pair<Tuple, Int>): TupleZSet {
        val m = HashMap<Tuple, IntegerWeight>()
        for ((t, w) in pairs) m.merge(t, IntegerWeight(w)) { a, b -> a.add(b) }
        return ZSet.fromMap(m)
    }

    @Test
    fun `join on a single key column`() {
        val left = zset(Tuple.of("k1", "x") to 1)
        val right = zset(Tuple.of("k1", "y") to 1)
        val out = StreamJoinOp(keyArity = 1).eval(left, right)
        assertEquals(zset(Tuple.of("k1", "x", "y") to 1), out)
    }

    @Test
    fun `no matching key yields empty`() {
        val left = zset(Tuple.of("k1", "x") to 1)
        val right = zset(Tuple.of("k2", "y") to 1)
        assertTrue(StreamJoinOp(keyArity = 1).eval(left, right).isEmpty())
    }

    @Test
    fun `join multiplies weights`() {
        val left = zset(Tuple.of("k", "x") to 2)
        val right = zset(Tuple.of("k", "y") to 3)
        val out = StreamJoinOp(keyArity = 1).eval(left, right)
        assertEquals(zset(Tuple.of("k", "x", "y") to 6), out)
    }

    @Test
    fun `negative weights multiply through the join`() {
        val left = zset(Tuple.of("k", "x") to -1)
        val right = zset(Tuple.of("k", "y") to 4)
        val out = StreamJoinOp(keyArity = 1).eval(left, right)
        assertEquals(zset(Tuple.of("k", "x", "y") to -4), out)
    }

    @Test
    fun `one left tuple joins every matching right tuple`() {
        val left = zset(Tuple.of("k", "x") to 1)
        val right = zset(Tuple.of("k", "y1") to 1, Tuple.of("k", "y2") to 1)
        val out = StreamJoinOp(keyArity = 1).eval(left, right)
        assertEquals(
            zset(Tuple.of("k", "x", "y1") to 1, Tuple.of("k", "x", "y2") to 1),
            out,
        )
    }

    @Test
    fun `distinct keys do not cross-join`() {
        val left = zset(Tuple.of("k1", "x") to 1, Tuple.of("k2", "x") to 1)
        val right = zset(Tuple.of("k1", "z") to 1, Tuple.of("k2", "z") to 1)
        val out = StreamJoinOp(keyArity = 1).eval(left, right)
        assertEquals(
            zset(Tuple.of("k1", "x", "z") to 1, Tuple.of("k2", "x", "z") to 1),
            out,
        )
    }

    @Test
    fun `key arity zero is the cartesian product`() {
        val left = zset(Tuple.of("x") to 1, Tuple.of("y") to 1)
        val right = zset(Tuple.of("p") to 1)
        val out = StreamJoinOp(keyArity = 0).eval(left, right)
        assertEquals(zset(Tuple.of("x", "p") to 1, Tuple.of("y", "p") to 1), out)
    }

    @Test
    fun `join on a two-column key`() {
        val left = zset(Tuple.of("a", "b", "x") to 1)
        val right = zset(Tuple.of("a", "b", "y") to 1, Tuple.of("a", "c", "y") to 1)
        val out = StreamJoinOp(keyArity = 2).eval(left, right)
        assertEquals(zset(Tuple.of("a", "b", "x", "y") to 1), out)
    }

    @Test
    fun `joining with an empty side yields empty`() {
        val left = zset(Tuple.of("k", "x") to 1)
        assertTrue(StreamJoinOp(keyArity = 1).eval(left, emptyTupleZSet()).isEmpty())
        assertTrue(StreamJoinOp(keyArity = 1).eval(emptyTupleZSet(), left).isEmpty())
    }
}
