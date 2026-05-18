package org.hooray.dbsp

/**
 * The incremental join built by *composing* primitive operators, following the
 * textbook DBSP form:
 *
 * ```
 * ΔO = Δa ⋈ I(Δb)  +  z⁻¹(I(Δa)) ⋈ Δb
 * ```
 *
 * This exists only as a reference oracle: it is differential-tested against the
 * fused [IncrementalJoinOp] in `IncrementalJoinTest`. If the two ever disagree,
 * one of them is wrong.
 */
class ComposedJoin(keyArity: Int) {
    private val integralA = IntegrateOp()
    private val delayA = Z1Op()
    private val integralB = IntegrateOp()
    private val joinLeft = StreamJoinOp(keyArity)
    private val joinRight = StreamJoinOp(keyArity)
    private val plus = PlusOp()

    fun eval(left: TupleZSet, right: TupleZSet): TupleZSet {
        val bNew = integralB.eval(right)                  // B[t]
        val aOld = delayA.eval(integralA.eval(left))      // A[t-1] = z⁻¹(A[t])
        return plus.eval(
            joinLeft.eval(left, bNew),                    // Δa ⋈ B[t]
            joinRight.eval(aOld, right),                  // A[t-1] ⋈ Δb
        )
    }
}
