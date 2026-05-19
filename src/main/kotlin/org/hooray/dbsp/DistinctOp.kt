package org.hooray.dbsp

import org.hooray.incremental.IntegerWeight
import org.hooray.incremental.ZSet

/**
 * Incremental `distinct`: projects a multiset onto a set, emitting changes only
 * when a tuple crosses the presence threshold.
 *
 * The operator keeps the accumulated weight of every tuple. For each tuple in
 * the input delta it compares the weight before and after:
 *  - absent → present (`≤0` then `>0`): emit `+1`
 *  - present → absent (`>0` then `≤0`): emit `-1`
 *  - otherwise: emit nothing
 *
 * This is the set-semantics operator; the v1 standard circuit keeps bag
 * semantics and does not wire it, but it is provided for completeness and
 * future operators.
 */
class DistinctOp(override val name: String = "distinct") :
    UnaryOperator<TupleZSet, TupleZSet> {

    /** Accumulated weight per tuple (the integral of all inputs seen). */
    private val state = HashMap<Tuple, Int>()

    override fun eval(input: TupleZSet): TupleZSet {
        val output = HashMap<Tuple, IntegerWeight>()
        for (entry in input.entries()) {
            val tuple = entry.key
            val before = state.getOrDefault(tuple, 0)
            val after = before + entry.value.value
            if (before <= 0 && after > 0) {
                output[tuple] = IntegerWeight.ONE
            } else if (before > 0 && after <= 0) {
                output[tuple] = IntegerWeight.MINUS_ONE
            }
            if (after == 0) state.remove(tuple) else state[tuple] = after
        }
        return ZSet.fromMap(output)
    }
}
