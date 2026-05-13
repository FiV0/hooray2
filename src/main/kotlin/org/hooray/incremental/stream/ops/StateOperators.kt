package org.hooray.incremental.stream.ops

import org.hooray.incremental.stream.AccumulatedStream
import org.hooray.incremental.stream.Node
import org.hooray.incremental.stream.NodeId
import org.hooray.incremental.stream.Stream
import org.hooray.incremental.stream.StreamRef
import org.hooray.incremental.stream.ZSetStream

data class IntegrateNode<T>(
    val input: ZSetStream<T>,
    override val id: NodeId = NodeId.next(),
    override val label: String = "integrate"
) : Node

data class DelayNode<T>(
    val input: Stream<T>,
    override val id: NodeId = NodeId.next(),
    override val label: String = "delay"
) : Node

data class DifferentiateNode<T>(
    val input: AccumulatedStream<T>,
    override val id: NodeId = NodeId.next(),
    override val label: String = "differentiate"
) : Node

fun <T> integrate(input: ZSetStream<T>): AccumulatedStream<T> =
    StreamRef(IntegrateNode(input))

fun <T> delay(input: Stream<T>): Stream<T> =
    StreamRef(DelayNode(input))

fun <T> differentiate(input: AccumulatedStream<T>): ZSetStream<T> =
    StreamRef(DifferentiateNode(input))
