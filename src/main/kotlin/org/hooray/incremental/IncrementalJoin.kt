package org.hooray.incremental

import org.hooray.algo.Extension

interface IncrementalJoin<T> {
    fun join(deltas: List<IndexedZSet<Extension, *, IntegerWeight>>): ZSet<T, IntegerWeight>
}