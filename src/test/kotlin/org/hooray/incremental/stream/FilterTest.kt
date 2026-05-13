package org.hooray.incremental.stream

import org.hooray.incremental.IntegerWeight
import org.hooray.incremental.ZSet
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class FilterTest {

    @Test
    fun `predicate keeps matching rows and drops the rest`() {
        // 10-row fixture: keep even-valued entries
        val zset = ZSet.fromMap(
            mapOf(
                1 to IntegerWeight(1),
                2 to IntegerWeight(2),
                3 to IntegerWeight(-1),
                4 to IntegerWeight(3),
                5 to IntegerWeight(1),
                6 to IntegerWeight(-2),
                7 to IntegerWeight(1),
                8 to IntegerWeight(4),
                9 to IntegerWeight(-1),
                10 to IntegerWeight(5)
            )
        )
        val out = computeFilter(zset) { it % 2 == 0 }
        assertEquals(5, out.size)
        assertEquals(IntegerWeight(2), out.weight(2))
        assertEquals(IntegerWeight(3), out.weight(4))
        assertEquals(IntegerWeight(-2), out.weight(6))
        assertEquals(IntegerWeight(4), out.weight(8))
        assertEquals(IntegerWeight(5), out.weight(10))
        // odd entries removed
        assertEquals(IntegerWeight.ZERO, out.weight(1))
        assertEquals(IntegerWeight.ZERO, out.weight(3))
    }

    @Test
    fun `predicate that rejects everything yields empty`() {
        val zset = ZSet.fromMap(mapOf(1 to IntegerWeight(1), 2 to IntegerWeight(1)))
        val out = computeFilter(zset) { false }
        assertTrue(out.isEmpty())
    }

    @Test
    fun `predicate that accepts everything returns equivalent zset`() {
        val zset = ZSet.fromMap(mapOf(1 to IntegerWeight(1), 2 to IntegerWeight(-3)))
        val out = computeFilter(zset) { true }
        assertEquals(zset, out)
    }

    @Test
    fun `filter builder produces a FilterNode rooted output`() {
        val input: ZSetStream<Int> = SimpleStream(SimpleNode(NodeId(0), "in"))
        val out = filter(NodeId(1), "evens", input) { it % 2 == 0 }
        assertTrue(out.node is FilterNode<*>)
        val fn = out.node as FilterNode<*>
        assertEquals(NodeId(1), fn.id)
        assertSame(input, fn.input)
    }
}
