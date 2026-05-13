package org.hooray.incremental.stream

import org.hooray.incremental.CompiledTriplePattern

data class IncrementalWcojJoinSpec(
    val patterns: List<CompiledTriplePattern>,
    val levels: Int,
    val canonicalOrder: List<Int> = (0 until levels).toList()
) {
    init {
        require(levels >= 0) { "levels must be non-negative" }
        require(canonicalOrder.size == levels) {
            "canonicalOrder size ${canonicalOrder.size} must match levels $levels"
        }
    }
}
