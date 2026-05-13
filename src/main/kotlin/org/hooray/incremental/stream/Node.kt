package org.hooray.incremental.stream

@JvmInline
value class NodeId(val value: Int)

sealed interface Node {
    val id: NodeId
    val label: String
}

interface DerivedNode<T> : Node {
    val output: ZSetStream<T>
}

internal data class SimpleNode(
    override val id: NodeId,
    override val label: String
) : Node

internal class SimpleDerivedNode<T>(
    override val id: NodeId,
    override val label: String,
    outputFactory: (Node) -> ZSetStream<T>
) : DerivedNode<T> {
    override val output: ZSetStream<T> = outputFactory(this)
}
