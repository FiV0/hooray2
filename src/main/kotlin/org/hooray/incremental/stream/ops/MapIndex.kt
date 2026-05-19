package org.hooray.incremental.stream.ops

import org.hooray.incremental.stream.IndexSpec
import org.hooray.incremental.stream.IndexedZSetStream
import org.hooray.incremental.stream.Node
import org.hooray.incremental.stream.NodeId
import org.hooray.incremental.stream.StreamRef
import org.hooray.incremental.stream.ZSetStream

data class MapIndexNode<T, K, V>(
    val input: ZSetStream<T>,
    val spec: IndexSpec<T, K, V>,
    override val id: NodeId = NodeId.next(),
    override val label: String = "mapIndex(${spec.name})"
) : Node

fun <T, K, V> mapIndex(
    input: ZSetStream<T>,
    spec: IndexSpec<T, K, V>
): IndexedZSetStream<K, V> =
    StreamRef(MapIndexNode(input, spec))
