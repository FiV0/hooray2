package org.hooray.incremental.stream

import org.hooray.incremental.IndexedZSet
import org.hooray.incremental.IntegerWeight
import org.hooray.incremental.ZSet

sealed interface Stream<out T> {
    val node: Node
}

internal class SimpleStream<out T>(override val node: Node) : Stream<T>

class AccumulatedZSet<T>(val zset: ZSet<T, IntegerWeight>)

typealias ZSetStream<T> = Stream<ZSet<T, IntegerWeight>>

// Deviation from spec: existing IndexedZSet is parameterized as IndexedZSet<K, W>;
// the nested value type lives inside the inner IZSet, not as a top-level parameter.
typealias IndexedZSetStream<K> = Stream<IndexedZSet<K, IntegerWeight>>

typealias AccumulatedStream<T> = Stream<AccumulatedZSet<T>>
