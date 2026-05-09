package org.hooray.incremental

data class ZSetIndices(
    val aev: IndexedZSet<Any, IntegerWeight>,
    val ave: IndexedZSet<Any, IntegerWeight>,
)