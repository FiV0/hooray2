package org.hooray.dbsp

/**
 * Differentiation (D): the change between the current input and the previous.
 *
 * `D(s)[t] = s[t] - s[t-1]` (with `s[-1]` the empty z-set). Converts a stream
 * of accumulated states into a stream of deltas. `D` is the inverse of
 * [IntegrateOp]: `D ∘ I = I ∘ D = identity`.
 */
class DifferentiateOp(override val name: String = "differentiate") :
    UnaryOperator<TupleZSet, TupleZSet> {

    private var previous: TupleZSet = emptyTupleZSet()

    override fun eval(input: TupleZSet): TupleZSet {
        val delta = input.subtract(previous)
        previous = input
        return delta
    }
}
