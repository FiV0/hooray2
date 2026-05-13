package org.hooray.incremental.stream

import org.hooray.incremental.ResultZSet
import org.hooray.incremental.IndexedZSet
import org.hooray.incremental.IntegerWeight
import org.hooray.incremental.ZSet
import org.hooray.incremental.ZSetIndices
import org.hooray.incremental.stream.ops.project

class Circuit(private val spec: CircuitSpec) {
    val input: InputHandle<ZSetIndices> = spec.input
    private val source: IncrementalWcojSource? = spec.source?.let(::IncrementalWcojSource)

    fun step(): ResultZSet {
        val nextInput = input.take()
        val sourceResult = when {
            source == null -> ZSet.empty()
            nextInput == null -> source.step(emptyInput())
            else -> source.step(nextInput)
        }
        return spec.transforms.fold(sourceResult) { result, transform ->
            when (transform) {
                is ProjectSpec -> project(result, transform)
            }
        }
    }

    fun step(input: ZSetIndices): ResultZSet {
        this.input.set(input)
        return step()
    }

    private fun emptyInput(): ZSetIndices =
        ZSetIndices(
            IndexedZSet.empty(IntegerWeight.ZERO, IntegerWeight.ONE),
            IndexedZSet.empty(IntegerWeight.ZERO, IntegerWeight.ONE)
        )
}
