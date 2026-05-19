package org.hooray.incremental.stream

import org.hooray.incremental.ZSetIndices

data class CircuitSpec(
    val input: InputHandle<ZSetIndices>,
    val source: IncrementalWcojJoinSpec? = null,
    val transforms: List<TransformSpec> = emptyList()
)
