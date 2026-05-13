package org.hooray.incremental.stream

import org.hooray.algo.ResultTuple
import org.hooray.incremental.CompiledTriplePattern
import org.hooray.incremental.IntegerWeight
import org.hooray.incremental.ResultZSet
import org.hooray.incremental.ZSet

/**
 * Per-branch variable order: the delta pattern's variables come first
 * (so its arrangement can drive the join), then the remaining canonical
 * variables in ascending order.
 *
 * Direct extraction of `IncrementalWcojJoinEngine.variableOrderForDeltaTerm`.
 */
fun variableOrderForDeltaTerm(
    deltaPattern: CompiledTriplePattern,
    levels: Int
): List<Int> {
    val deltaVariables = deltaPattern.variableIndexes()
    val remaining = (0 until levels).filterNot { it in deltaVariables }
    return deltaVariables + remaining
}

/**
 * Tuple permutation from branch order to canonical order. If
 * `variableOrder[i] == j`, then position i in the branch tuple holds
 * the value of canonical variable j.
 */
fun canonicalTuplePermutation(
    variableOrder: List<Int>,
    levels: Int
): (ResultTuple) -> ResultTuple {
    val identity = (0 until levels).toList()
    if (variableOrder == identity) return { it }
    return { tuple ->
        val canonical = MutableList<Any?>(levels) { null }
        for ((branchIndex, canonicalIndex) in variableOrder.withIndex()) {
            canonical[canonicalIndex] = tuple[branchIndex]
        }
        @Suppress("UNCHECKED_CAST")
        canonical as ResultTuple
    }
}

/**
 * Reindex a branch-local result ZSet into the canonical variable order,
 * summing weights for tuples that permute to the same canonical tuple
 * and dropping zero-weight entries.
 */
fun permuteToCanonical(
    termResult: ResultZSet,
    variableOrder: List<Int>,
    levels: Int
): ResultZSet {
    val identity = (0 until levels).toList()
    if (variableOrder == identity) return termResult
    val permute = canonicalTuplePermutation(variableOrder, levels)

    val accumulator = HashMap<ResultTuple, IntegerWeight>()
    for ((tuple, weight) in termResult.entries()) {
        accumulator.merge(permute(tuple), weight) { left, right ->
            val sum = left.add(right)
            if (sum.isZero()) null else sum
        }
    }
    return ZSet.fromMap(accumulator)
}
