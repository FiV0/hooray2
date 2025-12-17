package org.hooray.incremental

import org.hooray.algo.Extension
import org.hooray.algo.Prefix

/**
 * Wrapper that adapts IncrementalGenericJoin-style join computation to the SourceOperator interface.
 *
 * This class implements the eval/commit pattern:
 * - eval(): Distributes deltas and computes the join result
 * - commit(): Advances the state of all relations
 *
 * The computation logic mirrors IncrementalGenericJoin but separates
 * the state update from the computation.
 */
class IncrementalJoinOperator(
    private val relations: List<IncrementalIndex>,
    private val levels: Int
) : SourceOperator {
    override val name: String = "IncrementalJoin"

    /**
     * Compute delta extensions for one level using the DBSP incremental formula:
     *
     * Δ_{1..n} builds up as: Δ_{1..i} = Δ_{1..i-1} ⋈ Δᵢ
     *                                 + Δ_{1..i-1} ⋈ z⁻¹(i)
     *                                 + Δᵢ ⋈ z⁻¹(processed relations)
     */
    private fun computeLevelDelta(prefix: Prefix, relations: List<IncrementalIndex>): ZSet<Extension, IntegerWeight> {
        if (relations.isEmpty()) return ZSet.empty()

        // Start with smallest delta (WCOJ optimization)
        val minIndex = relations.indices.minBy { relations[it].delta.count(prefix) }
        var runningDelta = relations[minIndex].delta.propose(prefix)

        for (j in relations.indices) {
            if (j == minIndex) continue

            val deltaJ = relations[j].delta.propose(prefix)
            var term3 = deltaJ

            // Intersect with z⁻¹ of all relations processed before j
            for (k in 0 until j) {
                term3 = relations[k].accumulated.intersect(prefix, term3)
            }

            // Don't forget minIndex if it comes after j!
            if (minIndex > j) {
                term3 = relations[minIndex].accumulated.intersect(prefix, term3)
            }

            runningDelta = runningDelta.equiJoin(deltaJ) +
                relations[j].accumulated.intersect(prefix, runningDelta) +
                term3
        }

        return runningDelta
    }

    override fun eval(input: ZSetIndices): ResultZSet {
        // 1. Distribute deltas to relations
        relations.forEach { rel -> rel.receiveDelta(input) }

        // 2. Compute join level by level
        val participatingLevel1 = relations.filter { it.participatesInLevel(0) }
        var result: IZSet<Any, IntegerWeight, *> = computeLevelDelta(emptyList(), participatingLevel1)

        for (level in 1 until levels) {
            val participating = relations.filter { it.participatesInLevel(level) }
            result = result.extendLeaves { prefix, weight ->
                require(prefix.size == level) { "Prefix size ${prefix.size} does not match current level $level" }
                computeLevelDelta(prefix, participating).multiply(weight)
            }
        }

        return result.flatZSet()
    }

    override fun commit() {
        // Commit deltas to all relations
        relations.forEach { it.commit() }
    }
}
