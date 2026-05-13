package org.hooray.incremental.stream

sealed interface TransformSpec

data class ProjectSpec(
    val outputLevels: List<Int>
) : TransformSpec {
    init {
        require(outputLevels.all { it >= 0 }) { "outputLevels must be non-negative" }
    }
}
