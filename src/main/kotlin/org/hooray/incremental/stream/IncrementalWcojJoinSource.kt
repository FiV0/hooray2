package org.hooray.incremental.stream

import org.hooray.algo.ResultTuple
import org.hooray.incremental.ArrangementState
import org.hooray.incremental.CompiledTriplePattern
import org.hooray.incremental.ResultZSet
import org.hooray.incremental.ZSet
import org.hooray.incremental.ZSetIndices

/**
 * Runtime CircuitSource for the incremental WCOJ. For each tick it
 * distributes the input delta to every compiled pattern, then iterates
 * once per pattern as the "delta term": the active pattern reads its
 * DELTA arrangement, others read CURRENT. The per-branch result is
 * canonicalized back to the global variable order and summed into the
 * final delta.
 *
 * Mirrors `IncrementalWcojJoinEngine.eval` line-for-line, but built on
 * the new stream-package helpers (`variableOrderForDeltaTerm`,
 * `permuteToCanonical`, `computeZSetGenericJoin`).
 *
 * `commit()` is a no-op: the existing engine commits each pattern
 * inline after its branch finishes, and this source preserves that.
 */
class IncrementalWcojJoinSource(
    private val patterns: List<CompiledTriplePattern>,
    private val levels: Int
) : CircuitSource {
    override fun eval(input: ZSetIndices): ResultZSet {
        patterns.forEach { it.receiveDelta(input) }

        var result: ResultZSet = ZSet.empty<ResultTuple>()
        for (deltaIndex in patterns.indices) {
            val variableOrder = variableOrderForDeltaTerm(patterns[deltaIndex], levels)
            val extenders = patterns.mapIndexed { patternIndex, pattern ->
                val state = if (patternIndex == deltaIndex) {
                    ArrangementState.DELTA
                } else {
                    ArrangementState.CURRENT
                }
                pattern.view(variableOrder, state).toExtender()
            }
            val branchResult = computeZSetGenericJoin(extenders, levels)
            val canonical = permuteToCanonical(branchResult, variableOrder, levels)
            patterns[deltaIndex].commit()
            result = result.add(canonical)
        }
        return result
    }

    override fun commit() {
        // Intentional no-op — commits happen inline per-branch in eval()
        // to match IncrementalWcojJoinEngine semantics.
    }
}
