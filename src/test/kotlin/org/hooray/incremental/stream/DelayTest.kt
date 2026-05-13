package org.hooray.incremental.stream

import org.hooray.incremental.IntegerWeight
import org.hooray.incremental.ZSet
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class DelayTest {

    private fun zset(vararg pairs: Pair<String, Int>): ZSet<String, IntegerWeight> =
        ZSet.fromMap(pairs.associate { (k, w) -> k to IntegerWeight(w) })

    @Test
    fun `delta delay returns empty at tick 0`() {
        val node = DelayNode<ZSet<String, IntegerWeight>>(
            NodeId(1), "delay",
            SimpleStream(SimpleNode(NodeId(0), "in")),
            ZSet.empty()
        )
        val out = node.eval(zset("a" to 1))
        assertTrue(out.isEmpty())
    }

    @Test
    fun `delta delay at tick N returns the input from tick N-1`() {
        val node = DelayNode<ZSet<String, IntegerWeight>>(
            NodeId(1), "delay",
            SimpleStream(SimpleNode(NodeId(0), "in")),
            ZSet.empty()
        )
        val outs = listOf(
            zset("alice" to 1),
            zset("bob" to 2),
            zset("eve" to -1)
        ).map { delta ->
            val out = node.eval(delta)
            node.commit()
            out
        }
        // Tick 0 -> empty
        assertTrue(outs[0].isEmpty())
        // Tick 1 -> tick-0 input
        assertEquals(IntegerWeight(1), outs[1].weight("alice"))
        assertEquals(1, outs[1].size)
        // Tick 2 -> tick-1 input
        assertEquals(IntegerWeight(2), outs[2].weight("bob"))
        assertEquals(1, outs[2].size)
    }

    @Test
    fun `delay zset builder produces a DelayNode rooted output`() {
        val input: ZSetStream<String> = SimpleStream(SimpleNode(NodeId(0), "in"))
        val out = delay(NodeId(2), "delay", input)
        assertTrue(out.node is DelayNode<*>)
        assertEquals(NodeId(2), (out.node as DelayNode<*>).id)
    }

    @Test
    fun `accumulated delay returns empty accumulated at tick 0`() {
        val input: AccumulatedStream<String> = SimpleStream(SimpleNode(NodeId(0), "in"))
        val out = delayAccumulated(NodeId(3), "delay", input)
        val n = out.node as DelayNode<AccumulatedZSet<String>>
        val result = n.eval(AccumulatedZSet(zset("a" to 5)))
        assertTrue(result.zset.isEmpty())
    }

    @Test
    fun `accumulated delay at tick N returns tick N-1's accumulated state`() {
        val input: AccumulatedStream<String> = SimpleStream(SimpleNode(NodeId(0), "in"))
        val out = delayAccumulated(NodeId(3), "delay", input)
        @Suppress("UNCHECKED_CAST")
        val n = out.node as DelayNode<AccumulatedZSet<String>>
        val outs = listOf(
            AccumulatedZSet(zset("alice" to 5)),
            AccumulatedZSet(zset("alice" to 5, "bob" to 1))
        ).map { acc ->
            val r = n.eval(acc)
            n.commit()
            r
        }
        assertTrue(outs[0].zset.isEmpty())
        assertEquals(IntegerWeight(5), outs[1].zset.weight("alice"))
    }

    @Test
    fun `eval without commit does not advance state`() {
        val node = DelayNode<ZSet<String, IntegerWeight>>(
            NodeId(1), "delay",
            SimpleStream(SimpleNode(NodeId(0), "in")),
            ZSet.empty()
        )
        node.eval(zset("alice" to 1))
        // No commit; next eval should still see the initial empty state
        val out = node.eval(zset("bob" to 1))
        assertTrue(out.isEmpty())
    }
}
