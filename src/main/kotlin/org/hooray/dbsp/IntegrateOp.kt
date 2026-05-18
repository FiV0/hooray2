package org.hooray.dbsp

/**
 * Integration (∫ / I): the running sum of every input z-set seen so far.
 *
 * `I(s)[t] = s[0] + s[1] + ... + s[t]`. Converts a stream of deltas into a
 * stream of accumulated states. Implemented as a stateful accumulator rather
 * than a `plus`+`z1` feedback loop, so the circuit stays a pure DAG.
 */
class IntegrateOp(override val name: String = "integrate") :
    UnaryOperator<TupleZSet, TupleZSet> {

    private var accumulator: TupleZSet = emptyTupleZSet()

    override fun eval(input: TupleZSet): TupleZSet {
        accumulator = accumulator.add(input)
        return accumulator
    }
}
