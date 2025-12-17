package org.hooray.incremental

import org.hooray.algo.ResultTuple

/**
 * Incremental Distinct operator using the eval/commit pattern.
 *
 * ## Algorithm
 *
 * For each result tuple, we track the accumulated weight across all time steps.
 * The distinct output should contain each unique tuple exactly once (weight = 1)
 * if it's "present" (accumulated weight > 0).
 *
 * The incremental version only emits changes when tuples cross the zero threshold:
 * - ≤0 → >0: Emit +1 (tuple becomes present)
 * - >0 → ≤0: Emit -1 (tuple becomes absent)
 * - Otherwise: Emit nothing (stayed present or stayed absent)
 *
 * ## Example
 *
 * | Time | Input Delta     | Accumulated | Output Delta |
 * |------|-----------------|-------------|--------------|
 * | t=0  | (Alice, +1)     | Alice: 1    | (Alice, +1)  |
 * | t=1  | (Alice, +1)     | Alice: 2    | (nothing)    |
 * | t=2  | (Alice, -1)     | Alice: 1    | (nothing)    |
 * | t=3  | (Alice, -1)     | Alice: 0    | (Alice, -1)  |
 * | t=4  | (Alice, +1)     | Alice: 1    | (Alice, +1)  |
 *
 * ## State
 *
 * The `state` map represents the "delayed integral" - accumulated weights from
 * all previous steps. During eval(), we read from this state but don't modify it.
 * During commit(), we update the state with the pending delta.
 */
class IncrementalDistinct : TransformOperator {
    override val name: String = "IncrementalDistinct"

    /**
     * Accumulated weights from all previous steps (the "delayed integral").
     * Maps each result tuple to its total accumulated weight.
     */
    private var state: MutableMap<ResultTuple, Int> = mutableMapOf()

    /**
     * Pending delta to be committed after all operators have evaluated.
     */
    private var pendingDelta: ResultZSet? = null

    override fun eval(input: ResultZSet): ResultZSet {
        pendingDelta = input

        if (input.isEmpty()) {
            return ZSet.empty()
        }

        val outputMap = mutableMapOf<ResultTuple, IntegerWeight>()

        for ((tuple, weight) in input.entries()) {
            val deltaWeight = weight.value
            val oldWeight = state[tuple] ?: 0
            val newWeight = oldWeight + deltaWeight

            // Detect threshold crossing
            when {
                oldWeight <= 0 && newWeight > 0 -> {
                    // absent → present: emit +1
                    outputMap[tuple] = IntegerWeight.ONE
                }
                oldWeight > 0 && newWeight <= 0 -> {
                    // present → absent: emit -1
                    outputMap[tuple] = IntegerWeight.MINUS_ONE
                }
                // No threshold crossing: emit nothing
            }
        }

        return ZSet.fromMap(outputMap)
    }

    override fun commit() {
        val delta = pendingDelta ?: return

        // Update state with the delta
        for ((tuple, weight) in delta.entries()) {
            val newWeight = (state[tuple] ?: 0) + weight.value
            if (newWeight == 0) {
                state.remove(tuple)
            } else {
                state[tuple] = newWeight
            }
        }

        pendingDelta = null
    }

    /**
     * Get the current accumulated state size (for debugging/profiling).
     */
    fun stateSize(): Int = state.size

    /**
     * Check if a tuple is currently in the distinct set (weight > 0).
     * Useful for testing.
     */
    fun contains(tuple: ResultTuple): Boolean = (state[tuple] ?: 0) > 0
}
