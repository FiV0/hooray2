package org.hooray.incremental.stream

import org.hooray.incremental.IntegerWeight
import org.hooray.incremental.ZSet
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class MapIndexTest {

    private fun triple(e: String, a: String, v: Any): List<Any> = listOf(e, a, v)

    @Test
    fun `empty input produces empty index`() {
        val zset = ZSet.empty<List<Any>>()
        val spec = IndexSpec<List<Any>, Any, List<Any>>("aev", keyLevels = listOf(1), valueLevels = listOf(0, 2))
        val indexed = computeMapIndex(zset, spec)
        assertTrue(indexed.isEmpty())
    }

    @Test
    fun `single-position key groups tuples by that position`() {
        // 5 tuples, mixed positive and negative weights, hand-computed reference
        val zset = ZSet.fromMap(
            mapOf(
                triple("alice", "name", "Alice") to IntegerWeight(1),
                triple("alice", "age", 30) to IntegerWeight(1),
                triple("bob", "name", "Bob") to IntegerWeight(1),
                triple("bob", "age", 25) to IntegerWeight(2),
                triple("eve", "name", "Eve") to IntegerWeight(-1)
            )
        )
        val spec = IndexSpec<List<Any>, Any, List<Any>>(
            name = "byAttr",
            keyLevels = listOf(1),
            valueLevels = listOf(0, 2)
        )
        val indexed = computeMapIndex(zset, spec)

        // Expected groups: "name" -> 3 tuples (alice/Alice +1, bob/Bob +1, eve/Eve -1)
        //                  "age"  -> 2 tuples (alice/30 +1, bob/25 +2)
        assertEquals(setOf<Any>("name", "age"), indexed.keys())

        val byName = indexed.getTyped<List<Any>, ZSet<List<Any>, IntegerWeight>>("name")!!
        assertEquals(IntegerWeight(1), byName.weight(triple("alice", "name", "Alice")))
        assertEquals(IntegerWeight(1), byName.weight(triple("bob", "name", "Bob")))
        assertEquals(IntegerWeight(-1), byName.weight(triple("eve", "name", "Eve")))
        assertEquals(3, byName.size)

        val byAge = indexed.getTyped<List<Any>, ZSet<List<Any>, IntegerWeight>>("age")!!
        assertEquals(IntegerWeight(1), byAge.weight(triple("alice", "age", 30)))
        assertEquals(IntegerWeight(2), byAge.weight(triple("bob", "age", 25)))
        assertEquals(2, byAge.size)
    }

    @Test
    fun `multi-position key uses a list as the group key`() {
        val zset = ZSet.fromMap(
            mapOf(
                triple("a", "x", 1) to IntegerWeight(1),
                triple("a", "y", 2) to IntegerWeight(1),
                triple("b", "x", 3) to IntegerWeight(1)
            )
        )
        // key = (entity, attribute)
        val spec = IndexSpec<List<Any>, Any, List<Any>>(
            name = "byEA",
            keyLevels = listOf(0, 1),
            valueLevels = listOf(2)
        )
        val indexed = computeMapIndex(zset, spec)
        assertEquals(
            setOf<Any>(listOf<Any>("a", "x"), listOf<Any>("a", "y"), listOf<Any>("b", "x")),
            indexed.keys()
        )
    }

    @Test
    fun `fixedPrefix filters out tuples whose leading positions do not match`() {
        val zset = ZSet.fromMap(
            mapOf(
                triple("alice", "name", "Alice") to IntegerWeight(1),
                triple("bob", "name", "Bob") to IntegerWeight(1),
                triple("alice", "age", 30) to IntegerWeight(1)
            )
        )
        // fixedPrefix pins position 0 to "alice"
        val spec = IndexSpec<List<Any>, Any, List<Any>>(
            name = "alicePinned",
            keyLevels = listOf(1),
            valueLevels = listOf(2),
            fixedPrefix = listOf("alice")
        )
        val indexed = computeMapIndex(zset, spec)
        assertEquals(setOf<Any>("name", "age"), indexed.keys())
        // bob row excluded
        val byName = indexed.getTyped<List<Any>, ZSet<List<Any>, IntegerWeight>>("name")!!
        assertEquals(1, byName.size)
        assertEquals(IntegerWeight(1), byName.weight(triple("alice", "name", "Alice")))
        assertEquals(IntegerWeight.ZERO, byName.weight(triple("bob", "name", "Bob")))
    }

    @Test
    fun `identical inputs produce identical outputs (purity)`() {
        val zset = ZSet.fromMap(
            mapOf(
                triple("a", "x", 1) to IntegerWeight(1),
                triple("b", "x", 2) to IntegerWeight(-1)
            )
        )
        val spec = IndexSpec<List<Any>, Any, List<Any>>(
            name = "byAttr",
            keyLevels = listOf(1),
            valueLevels = listOf(0, 2)
        )
        val a = computeMapIndex(zset, spec)
        val b = computeMapIndex(zset, spec)
        assertEquals(a, b)
    }

    @Test
    fun `empty keyLevels is rejected`() {
        val zset = ZSet.empty<List<Any>>()
        val spec = IndexSpec<List<Any>, Any, List<Any>>(
            name = "bad",
            keyLevels = emptyList(),
            valueLevels = listOf(0)
        )
        assertThrows<IllegalArgumentException> { computeMapIndex(zset, spec) }
    }

    @Test
    fun `mapIndex builder produces a node whose output stream is rooted at itself`() {
        val inputNode = SimpleNode(NodeId(10), "input")
        val inputStream: ZSetStream<List<Any>> = SimpleStream(inputNode)
        val spec = IndexSpec<List<Any>, Any, List<Any>>(
            name = "byAttr",
            keyLevels = listOf(1),
            valueLevels = listOf(0, 2)
        )
        val outStream = mapIndex(NodeId(11), "ix", inputStream, spec)
        // The node behind the output stream is a MapIndex node
        assertTrue(outStream.node is MapIndexNode<*, *, *>)
        val mi = outStream.node as MapIndexNode<*, *, *>
        assertEquals(NodeId(11), mi.id)
        assertSame(inputStream, mi.input)
        assertSame(spec, mi.spec)
    }
}
