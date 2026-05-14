package org.hooray.incremental.stream.ops

import org.hooray.incremental.IntegerWeight
import org.hooray.incremental.ZSet
import org.hooray.incremental.stream.IndexSpec
import org.hooray.incremental.stream.Node
import org.hooray.incremental.stream.NodeId
import org.hooray.incremental.stream.StreamRef
import org.hooray.incremental.stream.ZSetStream
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Test

class StreamOperatorTest {
    private data class SourceNode(
        override val id: NodeId = NodeId(0),
        override val label: String = "source"
    ) : Node

    private fun sourceStream(): ZSetStream<String> =
        StreamRef<ZSet<String, IntegerWeight>>(SourceNode())

    @Test
    fun `mapIndex records the requested index spec on the graph node`() {
        val input = sourceStream()
        val spec = IndexSpec<String, String, String>(
            name = "name-by-value",
            keyLevels = listOf(1),
            valueLevels = listOf(0)
        )

        val output = mapIndex(input, spec)
        val node = assertInstanceOf(MapIndexNode::class.java, output.node)

        assertEquals(input, node.input)
        assertEquals(spec, node.spec)
    }

    @Test
    fun `state operators are distinct graph nodes`() {
        val input = sourceStream()

        val integrated = integrate(input)
        val delayed = delay(input)
        val differentiated = differentiate(input)

        assertInstanceOf(IntegrateNode::class.java, integrated.node)
        assertInstanceOf(DelayNode::class.java, delayed.node)
        assertInstanceOf(DifferentiateNode::class.java, differentiated.node)
    }

    @Test
    fun `differentiate accepts any stream and records its input`() {
        val input = sourceStream()

        val differentiated = differentiate(input)
        val node = assertInstanceOf(DifferentiateNode::class.java, differentiated.node)

        assertEquals(input, node.input)
    }

    @Test
    fun `differentiate delay records the delayed stream as input`() {
        val input = sourceStream()

        val delayed = delay(input)
        val differentiated = differentiate(delayed)
        val node = assertInstanceOf(DifferentiateNode::class.java, differentiated.node)

        assertEquals(delayed, node.input)
    }

    @Test
    fun `differentiate integrate keeps the intermediate stream explicit`() {
        val input = sourceStream()

        val integrated = integrate(input)
        val differentiated = differentiate(integrated)
        val node = assertInstanceOf(DifferentiateNode::class.java, differentiated.node)

        assertEquals(integrated, node.input)
    }
}
