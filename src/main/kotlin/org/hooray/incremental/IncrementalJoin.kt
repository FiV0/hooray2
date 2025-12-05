package org.hooray.incremental

interface IncrementalJoin<T> {
    fun join(deltas: ZSetIndices): ZSet<T, IntegerWeight>
}