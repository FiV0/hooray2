package org.hooray.incremental.stream

import org.hooray.incremental.IntegerWeight
import org.hooray.incremental.ZSet
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class ProjectTest {

    @Test
    fun `empty input projects to empty`() {
        val zset = ZSet.empty<List<Any>>()
        val result = computeProject<List<Any>, Any>(zset) { it[0] }
        assertTrue(result.isEmpty())
    }

    @Test
    fun `identity projection preserves the ZSet`() {
        val zset = ZSet.fromMap(
            mapOf("a" to IntegerWeight(2), "b" to IntegerWeight(-1))
        )
        val result = computeProject<String, String>(zset) { it }
        assertEquals(IntegerWeight(2), result.weight("a"))
        assertEquals(IntegerWeight(-1), result.weight("b"))
        assertEquals(2, result.size)
    }

    @Test
    fun `overlapping projections sum weights`() {
        // 3 rows project to 2 keys; the two rows mapping to "x" sum to 1+(-2)=-1
        val zset = ZSet.fromMap(
            mapOf(
                listOf<Any>("x", 1) to IntegerWeight(1),
                listOf<Any>("x", 2) to IntegerWeight(-2),
                listOf<Any>("y", 3) to IntegerWeight(3)
            )
        )
        val result = computeProject<List<Any>, Any>(zset) { it[0] }
        assertEquals(IntegerWeight(-1), result.weight("x"))
        assertEquals(IntegerWeight(3), result.weight("y"))
        assertEquals(2, result.size)
    }

    @Test
    fun `projection whose weights cancel drops the entry`() {
        val zset = ZSet.fromMap(
            mapOf(
                listOf<Any>("x", 1) to IntegerWeight(2),
                listOf<Any>("x", 2) to IntegerWeight(-2)
            )
        )
        val result = computeProject<List<Any>, Any>(zset) { it[0] }
        assertTrue(result.isEmpty())
    }

    @Test
    fun `project builder produces a ProjectNode whose output is rooted at it`() {
        val inputNode = SimpleNode(NodeId(20), "input")
        val input: ZSetStream<String> = SimpleStream(inputNode)
        val out = project<String, Int>(NodeId(21), "proj", input) { it.length }
        assertTrue(out.node is ProjectNode<*, *>)
        val pn = out.node as ProjectNode<*, *>
        assertEquals(NodeId(21), pn.id)
        assertSame(input, pn.input)
    }
}
