package org.hooray.incremental.stream

import org.hooray.incremental.IndexedZSet
import org.hooray.incremental.IntegerWeight
import org.hooray.incremental.ZSet
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Test

class StreamTypesTest {
    @Test
    fun `NodeId is a value-class wrapper that compares by value`() {
        val a = NodeId(1)
        val b = NodeId(1)
        val c = NodeId(2)
        assertEquals(a, b)
        assertEquals(a.hashCode(), b.hashCode())
        assertNotNull(c)
        assertEquals(false, a == c)
    }

    @Test
    fun `concrete Node carries id and label`() {
        val node: Node = SimpleNode(NodeId(42), "src")
        assertEquals(NodeId(42), node.id)
        assertEquals("src", node.label)
    }

    @Test
    fun `Stream exposes its node`() {
        val node = SimpleNode(NodeId(1), "n")
        val stream: Stream<String> = SimpleStream(node)
        assertSame(node, stream.node)
    }

    @Test
    fun `ZSetStream alias is Stream over ZSet with IntegerWeight`() {
        val node = SimpleNode(NodeId(1), "z")
        val stream: ZSetStream<String> = SimpleStream(node)
        val underlying: Stream<ZSet<String, IntegerWeight>> = stream
        assertSame(node, underlying.node)
    }

    @Test
    fun `IndexedZSetStream alias is Stream over IndexedZSet with IntegerWeight`() {
        val node = SimpleNode(NodeId(2), "ix")
        val stream: IndexedZSetStream<String> = SimpleStream(node)
        val underlying: Stream<IndexedZSet<String, IntegerWeight>> = stream
        assertSame(node, underlying.node)
    }

    @Test
    fun `AccumulatedStream alias is Stream over AccumulatedZSet`() {
        val node = SimpleNode(NodeId(3), "acc")
        val stream: AccumulatedStream<String> = SimpleStream(node)
        val underlying: Stream<AccumulatedZSet<String>> = stream
        assertSame(node, underlying.node)
    }

    @Test
    fun `DerivedNode exposes a ZSetStream output rooted at itself`() {
        val derived: DerivedNode<String> = SimpleDerivedNode(NodeId(4), "d") { owner ->
            SimpleStream(owner)
        }
        assertSame(derived, derived.output.node)
    }

    @Test
    fun `AccumulatedZSet wraps an underlying ZSet`() {
        val z = ZSet.singleton("a", IntegerWeight(3))
        val acc = AccumulatedZSet(z)
        assertSame(z, acc.zset)
    }
}
