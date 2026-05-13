package org.hooray.incremental.stream

import org.hooray.incremental.IncrementalWcojJoinEngine
import org.hooray.incremental.ResultZSet
import org.hooray.incremental.ZSetIndices

class IncrementalWcojSource(spec: IncrementalWcojJoinSpec) {
    private val engine = IncrementalWcojJoinEngine(spec.patterns, spec.levels)

    fun step(input: ZSetIndices): ResultZSet =
        engine.eval(input)
}
