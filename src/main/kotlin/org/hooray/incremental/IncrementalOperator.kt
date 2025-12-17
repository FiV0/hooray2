package org.hooray.incremental

import org.hooray.algo.ResultTuple

/**
 * Type alias for result Z-sets commonly used in pipelines.
 */
typealias ResultZSet = ZSet<ResultTuple, IntegerWeight>

/**
 * Base interface for incremental operators using the eval/commit pattern.
 *
 * The eval/commit pattern separates computation from state updates:
 * - eval(): Computes output using current internal state (the "delayed integral")
 *           but does NOT modify state
 * - commit(): Advances internal state by incorporating the last input
 *
 * This separation ensures correct incremental semantics where operators
 * see the accumulated state from all *previous* time steps, not the current one.
 *
 * @param I Input type
 * @param O Output type
 */
interface IncrementalOperator<I, O> {
    /**
     * Human-readable name for debugging and profiling.
     */
    val name: String

    /**
     * Process input delta and produce output delta.
     *
     * Internal state represents the "delayed integral" - accumulated values
     * from all previous steps. State is NOT modified during eval.
     *
     * @param input The input delta for this time step
     * @return The output delta
     */
    fun eval(input: I): O

    /**
     * Advance internal state by incorporating the input from the last eval.
     *
     * Called after all operators in the pipeline have been evaluated.
     * This ensures that during eval, all operators see consistent "delayed" state.
     */
    fun commit()
}

/**
 * Operator that takes indexed deltas (ZSetIndices) and produces result tuples.
 * Used for source operators like WCOJ joins.
 */
interface SourceOperator : IncrementalOperator<ZSetIndices, ResultZSet>

/**
 * Operator that transforms result Z-sets.
 * Used for operators like distinct, filter, map that transform join results.
 */
interface TransformOperator : IncrementalOperator<ResultZSet, ResultZSet>
