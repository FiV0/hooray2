package org.hooray.incremental.stream

import org.hooray.incremental.IntegerWeight
import org.hooray.incremental.IndexedZSet
import org.hooray.incremental.ZSet
import java.util.concurrent.atomic.AtomicInteger

@JvmInline
value class NodeId(val value: Int) {
    companion object {
        private val nextValue = AtomicInteger()

        fun next(): NodeId = NodeId(nextValue.getAndIncrement())
    }
}

interface Node {
    val id: NodeId
    val label: String
}

interface Stream<out T> {
    val node: Node
}

data class StreamRef<out T>(override val node: Node) : Stream<T>

typealias ZSetStream<T> = Stream<ZSet<T, IntegerWeight>>

data class IndexedZSetPayload<K, V>(
    val index: IndexedZSet<K, IntegerWeight>
)

typealias IndexedZSetStream<K, V> = Stream<IndexedZSetPayload<K, V>>
