package org.hooray.incremental.stream

import org.hooray.incremental.IntegerWeight
import org.hooray.incremental.ZSet
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class IntegrateTest {

    private fun zset(vararg pairs: Pair<String, Int>): ZSet<String, IntegerWeight> =
        ZSet.fromMap(pairs.associate { (k, w) -> k to IntegerWeight(w) })

    private fun integrateNode(): IntegrateNode<String> =
        IntegrateNode(NodeId(1), "int", SimpleStream(SimpleNode(NodeId(0), "in")))

    @Test
    fun `first tick eval returns the first tick's input`() {
        val node = integrateNode()
        val out = node.eval(zset("alice" to 1))
        assertEquals(IntegerWeight(1), out.zset.weight("alice"))
    }

    @Test
    fun `second tick eval exposes the cumulative sum through this tick`() {
        val node = integrateNode()
        node.eval(zset("alice" to 2))
        node.commit()
        val out = node.eval(zset("alice" to 3))
        assertEquals(IntegerWeight(5), out.zset.weight("alice"))
    }

    @Test
    fun `tick N eval exposes the sum of inputs through tick N inclusive`() {
        val node = integrateNode()
        val ticks = listOf(
            zset("alice" to 1, "bob" to 1),
            zset("alice" to 2),
            zset("bob" to -1),
            zset("eve" to 4)
        )
        val outs = ticks.map { delta ->
            val out = node.eval(delta)
            node.commit()
            out
        }

        // Tick 0: sum = tick-0 input
        assertEquals(IntegerWeight(1), outs[0].zset.weight("alice"))
        assertEquals(IntegerWeight(1), outs[0].zset.weight("bob"))
        // Tick 1: sum = ticks 0..1
        assertEquals(IntegerWeight(3), outs[1].zset.weight("alice"))
        assertEquals(IntegerWeight(1), outs[1].zset.weight("bob"))
        // Tick 2: sum = ticks 0..2 — bob cancels
        assertEquals(IntegerWeight(3), outs[2].zset.weight("alice"))
        assertEquals(IntegerWeight.ZERO, outs[2].zset.weight("bob"))
        // Tick 3: sum = ticks 0..3
        assertEquals(IntegerWeight(3), outs[3].zset.weight("alice"))
        assertEquals(IntegerWeight(4), outs[3].zset.weight("eve"))
    }

    @Test
    fun `eval without commit does not advance state`() {
        val node = integrateNode()
        // First eval and commit so accumulated holds something to compare against.
        node.eval(zset("alice" to 5))
        node.commit()
        // Now eval again without commit; next eval should see the same held state.
        node.eval(zset("bob" to 1))
        val out = node.eval(zset("bob" to 1))
        // accumulated is still {alice: 5}, plus current input {bob: 1}
        assertEquals(IntegerWeight(5), out.zset.weight("alice"))
        assertEquals(IntegerWeight(1), out.zset.weight("bob"))
    }

    @Test
    fun `integrate builder produces an IntegrateNode rooted output`() {
        val input: ZSetStream<String> = SimpleStream(SimpleNode(NodeId(0), "in"))
        val out = integrate(NodeId(2), "int", input)
        assertTrue(out.node is IntegrateNode<*>)
        val n = out.node as IntegrateNode<*>
        assertEquals(NodeId(2), n.id)
        assertSame(input, n.input)
    }
}
