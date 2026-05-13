package org.hooray.incremental.stream

@JvmInline
value class NodeId(val value: Int)

sealed interface Node {
    val id: NodeId
    val label: String
}

/**
 * A node that produces a single output stream. The type parameter T is
 * the *underlying value type* of the stream (e.g., `ZSet<Row, IntegerWeight>`
 * for a join, `IndexedZSet<K, IntegerWeight>` for a mapIndex). This is
 * Stream<T>, not ZSetStream<T>, so operators that emit indexed Z-sets can
 * implement it too.
 */
interface DerivedNode<T> : Node {
    val output: Stream<T>
}

internal data class SimpleNode(
    override val id: NodeId,
    override val label: String
) : Node

internal class SimpleDerivedNode<T>(
    override val id: NodeId,
    override val label: String,
    outputFactory: (Node) -> Stream<T>
) : DerivedNode<T> {
    override val output: Stream<T> = outputFactory(this)
}
