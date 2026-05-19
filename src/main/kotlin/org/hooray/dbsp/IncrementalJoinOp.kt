package org.hooray.dbsp

/**
 * Incremental equi-join: given the per-step *deltas* of two relations, emits the
 * delta of their join — without recomputing the whole join.
 *
 * **Derivation.** Let `A[t]`, `B[t]` be the full relations (the integrals of the
 * delta streams) and `O[t] = A[t] ⋈ B[t]`. With `A[t] = A[t-1] + Δa` and
 * `B[t] = B[t-1] + Δb`, bilinearity of `⋈` gives
 *
 * ```
 * O[t] - O[t-1] = A[t-1]⋈Δb + Δa⋈B[t-1] + Δa⋈Δb
 *               = Δa ⋈ (B[t-1] + Δb)  +  A[t-1] ⋈ Δb
 *               = Δa ⋈ B[t]           +  A[t-1] ⋈ Δb
 * ```
 *
 * so the operator keeps the integral of each input and computes
 * `ΔO = Δa ⋈ Bₙₑw + Aₒₗd ⋈ Δb`, where `Bₙₑw` includes the current `Δb` and
 * `Aₒₗd` excludes the current `Δa`. The `Δa⋈Δb` cross term is absorbed into the
 * first product. This is the fused trace-style join (cf. Feldera's `join`); the
 * composed `Integrate`/`Z1`/`StreamJoin`/`Plus` form is the test oracle.
 *
 * Join key, matching, and output layout are exactly [StreamJoinOp]'s.
 */
class IncrementalJoinOp(
    keyArity: Int,
    override val name: String = "incremental-join",
) : BinaryOperator<TupleZSet, TupleZSet, TupleZSet> {

    private val join = StreamJoinOp(keyArity)

    // TODO: This is incremental in output semantics, but not in indexing cost.
    // StreamJoinOp rebuilds a hash index over the full accumulated relation on
    // every step. A trace-backed join should maintain keyed indexes for A and B
    // incrementally and probe those indexes with the incoming deltas.

    /** `A[t-1]` — integral of the left input, excluding the current step. */
    private var integralA: TupleZSet = emptyTupleZSet()

    /** `B[t-1]` — integral of the right input, excluding the current step. */
    private var integralB: TupleZSet = emptyTupleZSet()

    override fun eval(left: TupleZSet, right: TupleZSet): TupleZSet {
        val integralBNew = integralB.add(right)                // B[t]
        val delta = join.eval(left, integralBNew)              // Δa ⋈ B[t]
            .add(join.eval(integralA, right))                  // + A[t-1] ⋈ Δb

        integralA = integralA.add(left)                        // advance to A[t]
        integralB = integralBNew                               // advance to B[t]
        return delta
    }
}
