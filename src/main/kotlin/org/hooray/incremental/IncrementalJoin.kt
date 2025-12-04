package org.hooray.incremental

import org.hooray.algo.Extension

interface IncrementalJoin<T> {
    fun join(deltas: ZSetIndices): ZSet<T, IntegerWeight>
}