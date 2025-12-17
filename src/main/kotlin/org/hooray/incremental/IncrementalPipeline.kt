package org.hooray.incremental

/**
 * A pipeline that chains incremental operators together.
 *
 * ## Execution Model (eval/commit pattern)
 *
 * Each call to step() executes two phases:
 * 1. **Evaluate phase**: All operators are evaluated in order, each seeing
 *    "delayed" state (accumulated from previous steps, not current)
 * 2. **Commit phase**: All operators commit their pending deltas to state
 *
 * This two-phase approach ensures correct incremental semantics - operators
 * compute against consistent historical state, not partially-updated state.
 *
 * ## Data Flow
 *
 * ```
 * ZSetIndices ──→ SourceOperator ──→ TransformOperator(s) ──→ ResultZSet
 *                     (Join)            (Distinct, etc.)
 * ```
 *
 * @param source The source operator that consumes ZSetIndices
 * @param transforms List of transform operators applied in order
 */
class IncrementalPipeline(
    private val source: SourceOperator,
    private val transforms: List<TransformOperator> = emptyList()
) {
    /**
     * Process a delta through the pipeline.
     *
     * @param delta The input delta (changes to the indexed relations)
     * @return The output delta after all operators have processed
     */
    fun step(delta: ZSetIndices): ResultZSet {
        // Phase 1: Evaluate all operators in order
        var result = source.eval(delta)
        for (transform in transforms) {
            result = transform.eval(result)
        }

        // Phase 2: Commit all operators
        source.commit()
        transforms.forEach { it.commit() }

        return result
    }

    /**
     * Get the names of all operators in the pipeline (for debugging).
     */
    fun operatorNames(): List<String> {
        return listOf(source.name) + transforms.map { it.name }
    }
}
