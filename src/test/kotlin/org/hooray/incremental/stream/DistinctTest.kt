package org.hooray.incremental.stream

import org.hooray.incremental.IncrementalDistinct
import org.hooray.incremental.IntegerWeight
import org.hooray.incremental.ResultZSet
import org.hooray.incremental.ZSet
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class DistinctTest {

    private fun tup(s: String): List<Any> = listOf<Any>(s)

    private fun zset(vararg pairs: Pair<String, Int>): ResultZSet =
        ZSet.fromMap(pairs.associate { (k, w) -> tup(k) to IntegerWeight(w) })

    private fun runStream(
        node: DistinctNode<List<Any>>,
        ticks: List<ResultZSet>
    ): List<ResultZSet> = ticks.map { delta ->
        val out = node.eval(delta)
        node.commit()
        out
    }

    private fun runLegacy(
        legacy: IncrementalDistinct,
        ticks: List<ResultZSet>
    ): List<ResultZSet> = ticks.map { delta ->
        val out = legacy.eval(delta)
        legacy.commit()
        out
    }

    private fun streamNode(): DistinctNode<List<Any>> =
        DistinctNode(NodeId(1), "d", SimpleStream(SimpleNode(NodeId(0), "in")))

    @Test
    fun `fresh tuple emits +1`() {
        val node = streamNode()
        val out = node.eval(zset("alice" to 1))
        node.commit()
        assertEquals(IntegerWeight.ONE, out.weight(tup("alice")))
        assertEquals(1, out.size)
    }

    @Test
    fun `re-adding a present tuple emits nothing`() {
        val node = streamNode()
        val outs = runStream(node, listOf(zset("alice" to 1), zset("alice" to 1)))
        assertEquals(IntegerWeight.ONE, outs[0].weight(tup("alice")))
        assertTrue(outs[1].isEmpty())
    }

    @Test
    fun `tuple crossing positive to zero emits -1`() {
        val node = streamNode()
        val outs = runStream(node, listOf(zset("alice" to 1), zset("alice" to -1)))
        assertEquals(IntegerWeight.ONE, outs[0].weight(tup("alice")))
        assertEquals(IntegerWeight.MINUS_ONE, outs[1].weight(tup("alice")))
    }

    @Test
    fun `parity with IncrementalDistinct across many tick patterns`() {
        val ticks: List<ResultZSet> = listOf(
            zset("alice" to 1, "bob" to 1),
            zset("alice" to 1),                       // alice stays present
            zset("alice" to -1),                      // alice still present
            zset("alice" to -1),                      // alice crosses to absent
            zset("alice" to 1, "bob" to -1),          // alice back, bob gone
            zset("eve" to 2, "alice" to -1)           // eve appears (>0), alice stays present
        )
        val streamOuts = runStream(streamNode(), ticks)
        val legacyOuts = runLegacy(IncrementalDistinct(), ticks)
        assertEquals(legacyOuts.size, streamOuts.size)
        for (i in streamOuts.indices) {
            assertEquals(legacyOuts[i], streamOuts[i], "tick $i diverged")
        }
    }

    @Test
    fun `distinct builder produces a DistinctNode rooted output`() {
        val input: ZSetStream<List<Any>> = SimpleStream(SimpleNode(NodeId(0), "in"))
        val out = distinct(NodeId(2), "d", input)
        assertTrue(out.node is DistinctNode<*>)
        val dn = out.node as DistinctNode<*>
        assertEquals(NodeId(2), dn.id)
        assertSame(input, dn.input)
    }
}
