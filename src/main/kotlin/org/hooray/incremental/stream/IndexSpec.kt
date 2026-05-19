package org.hooray.incremental.stream

data class IndexSpec<T, K, V>(
    val name: String,
    val keyLevels: List<Int>,
    val valueLevels: List<Int>,
    val fixedPrefix: List<Any?> = emptyList()
)
