package org.hooray.incremental.stream

import org.hooray.incremental.IndexedZSet
import org.hooray.incremental.IntegerWeight
import org.hooray.incremental.ResultZSet
import org.hooray.incremental.ZSetIndices

/**
 * A source sub-circuit: consumes a `ZSetIndices` delta and emits a
 * `ResultZSet`. `commit` advances any internal state to the next tick.
 *
 * In v1 the source is built by analysis from compiled patterns
 * (Task 15). Defining it as an interface lets the Circuit skeleton
 * land first without depending on the WCOJ expansion.
 */
fun interface CircuitSource {
    fun eval(input: ZSetIndices): ResultZSet
    fun commit() {}
}

/**
 * A post-source transform applied to the running ResultZSet (Project,
 * Distinct, Filter, etc.). Has its own eval/commit pair.
 */
interface CircuitTransform {
    fun eval(input: ResultZSet): ResultZSet
    fun commit() {}
}

data class CircuitSpec(
    val source: CircuitSource,
    val transforms: List<CircuitTransform> = emptyList()
)

/**
 * Runtime entry point. `input.set(delta); step()` advances the circuit
 * by one tick; `step(delta)` is the migration wrapper.
 *
 * On each step: every eval runs first (in source-then-transforms order),
 * then every commit runs. This mirrors `IncrementalPipeline.step`: eval
 * reads the previous-tick state; commit folds the current tick into
 * state for the next tick.
 */
class Circuit(private val spec: CircuitSpec) {
    val input: InputHandle<ZSetIndices> = InputHandle()

    private val emptyIndices = ZSetIndices(
        aev = IndexedZSet.empty(IntegerWeight.ZERO, IntegerWeight.ONE),
        ave = IndexedZSet.empty(IntegerWeight.ZERO, IntegerWeight.ONE)
    )

    fun step(): ResultZSet {
        val delta = input.takeOrEmpty(emptyIndices)
        var result = spec.source.eval(delta)
        for (t in spec.transforms) {
            result = t.eval(result)
        }
        spec.source.commit()
        spec.transforms.forEach { it.commit() }
        return result
    }

    fun step(input: ZSetIndices): ResultZSet {
        this.input.set(input)
        return step()
    }
}
