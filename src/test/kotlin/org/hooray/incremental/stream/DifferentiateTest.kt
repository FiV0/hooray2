package org.hooray.incremental.stream

import org.hooray.incremental.IntegerWeight
import org.hooray.incremental.ZSet
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import kotlin.random.Random

class DifferentiateTest {

    private fun zset(vararg pairs: Pair<String, Int>): ZSet<String, IntegerWeight> =
        ZSet.fromMap(pairs.associate { (k, w) -> k to IntegerWeight(w) })

    private fun differentiateNode(): DifferentiateNode<String> =
        DifferentiateNode(NodeId(1), "diff", SimpleStream(SimpleNode(NodeId(0), "in")))

    @Test
    fun `first tick eval returns the input itself (since prior held is empty)`() {
        val node = differentiateNode()
        val out = node.eval(AccumulatedZSet(zset("a" to 3)))
        assertEquals(IntegerWeight(3), out.weight("a"))
    }

    @Test
    fun `eval at tick N returns input N minus input N-1`() {
        val node = differentiateNode()
        val ticks = listOf(
            AccumulatedZSet(zset("a" to 1)),
            AccumulatedZSet(zset("a" to 3)),
            AccumulatedZSet(zset("a" to 3, "b" to 2)),
            AccumulatedZSet(zset("b" to 2))
        )
        val outs = ticks.map { acc ->
            val out = node.eval(acc)
            node.commit()
            out
        }
        // Tick 0: input - empty = {a:1}
        assertEquals(IntegerWeight(1), outs[0].weight("a"))
        // Tick 1: {a:3} - {a:1} = {a:2}
        assertEquals(IntegerWeight(2), outs[1].weight("a"))
        // Tick 2: {a:3, b:2} - {a:3} = {b:2}
        assertEquals(IntegerWeight.ZERO, outs[2].weight("a"))
        assertEquals(IntegerWeight(2), outs[2].weight("b"))
        // Tick 3: {b:2} - {a:3, b:2} = {a:-3}
        assertEquals(IntegerWeight(-3), outs[3].weight("a"))
        assertEquals(IntegerWeight.ZERO, outs[3].weight("b"))
    }

    @Test
    fun `differentiate of integrate is the identity on the input stream`() {
        val rng = Random(42)
        // Generate 5 ticks of random delta inputs
        val deltas: List<ZSet<String, IntegerWeight>> = (0 until 5).map {
            val keys = listOf("a", "b", "c").filter { rng.nextBoolean() }
            zset(*keys.map { it to rng.nextInt(-3, 4) }.filter { it.second != 0 }.toTypedArray())
        }

        val integ = IntegrateNode<String>(NodeId(10), "int", SimpleStream(SimpleNode(NodeId(0), "in")))
        val diff = DifferentiateNode<String>(NodeId(11), "diff", integ.output)

        for ((i, delta) in deltas.withIndex()) {
            val accumulated = integ.eval(delta)
            val differenced = diff.eval(accumulated)
            integ.commit()
            diff.commit()
            assertEquals(delta, differenced, "tick $i: differentiate(integrate(s)) != s")
        }
    }

    @Test
    fun `differentiate builder produces a DifferentiateNode rooted output`() {
        val input: AccumulatedStream<String> = SimpleStream(SimpleNode(NodeId(0), "in"))
        val out = differentiate(NodeId(2), "diff", input)
        assertTrue(out.node is DifferentiateNode<*>)
        val n = out.node as DifferentiateNode<*>
        assertEquals(NodeId(2), n.id)
        assertSame(input, n.input)
    }
}
