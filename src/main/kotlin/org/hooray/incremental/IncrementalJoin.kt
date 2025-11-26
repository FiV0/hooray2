package org.hooray.incremental

interface IncrementalJoin<T> {
    fun join(): ZSet<T, IntegerWeight>
}