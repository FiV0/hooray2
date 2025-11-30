package org.hooray.incremental

import org.hooray.algo.Extension
import org.hooray.algo.Prefix
import org.hooray.algo.ResultTuple
import org.hooray.iterator.LevelParticipation

/**
 * A prefix extender over a ZSet - mirrors the non-incremental interface.
 */
interface ZSetPrefixExtender {
    fun count(prefix: Prefix): Int
    fun propose(prefix: Prefix): ZSet<Extension, IntegerWeight>
    fun intersect(prefix: Prefix, extensions: ZSet<Extension, IntegerWeight>): ZSet<Extension, IntegerWeight>
}

/**
 * Manages the incremental state for a single indexed relation.
 * Provides separate extenders for delta and accumulated state.
 */
interface IncrementalRelation : LevelParticipation {
    /** Receive the delta for this transaction */
    fun receiveDelta(delta: IndexedZSet<*, IntegerWeight>)

    /** Merge current delta into accumulated state (call after join completes) */
    fun commit()

    /** Extender over the current delta */
    val delta: ZSetPrefixExtender

    /** Extender over z⁻¹ (accumulated previous state) */
    val accumulated: ZSetPrefixExtender
}

class IncrementalGenericJoin2(private val relations: List<IncrementalRelation>, private val levels: Int) : IncrementalJoin<ResultTuple> {

    override fun join(deltas: List<IndexedZSet<Any, IntegerWeight>>): ZSet<ResultTuple, IntegerWeight> {
        // 1. Distribute deltas
        relations.forEachIndexed { i, rel -> rel.receiveDelta(deltas[i]) }

        // 2. Compute join level by level
        var result: IndexedZSet<Any, IntegerWeight> = IndexedZSet.empty(IntegerWeight.ZERO, IntegerWeight.ONE)

        for (level in 0 until levels) {
            val participating = relations.filter { it.participatesInLevel(level) }
            result = result.extendLeaves { prefix, weight ->
                computeLevelDelta(prefix, participating).multiply(weight)
            }
        }

        // 3. Commit deltas
        relations.forEach { it.commit() }

        return result.toFlatZSet()
    }

    /**
     * Compute delta extensions for one level using the DBSP incremental formula:
     *
     * Δ_{1..n} builds up as: Δ_{1..i} = Δ_{1..i-1} ⋈ Δᵢ
     *                                 + Δ_{1..i-1} ⋈ z⁻¹(i)
     *                                 + Δᵢ ⋈ z⁻¹(processed relations)
     */
    private fun computeLevelDelta(prefix: Prefix, relations: List<IncrementalRelation>): ZSet<Extension, IntegerWeight> {
        if (relations.isEmpty()) return ZSet.empty()

        // Start with smallest delta (WCOJ optimization)
        val minIndex = relations.indices.minBy { relations[it].delta.count(prefix) }
        var runningDelta = relations[minIndex].delta.propose(prefix)

        for (j in relations.indices) {
            if (j == minIndex) continue

            val deltaJ = relations[j].delta.propose(prefix)

            // Term 3: Δⱼ ⋈ z⁻¹(all relations whose deltas are in runningDelta)
            val term3 = computeAnchoredTerm(prefix, relations, j, minIndex, deltaJ)

            runningDelta =
                runningDelta.naturalJoin(deltaJ) +                          // Term 1
                        relations[j].accumulated.intersect(prefix, runningDelta) +  // Term 2
                        term3                                                        // Term 3
        }

        return runningDelta
    }

    /**
     * Compute Δⱼ ⋈ z⁻¹(all relations that have contributed deltas so far).
     * This is "anchored" on relation j's delta.
     */
    private fun computeAnchoredTerm(
        prefix: Prefix,
        relations: List<IncrementalRelation>,
        j: Int,
        minIndex: Int,
        deltaJ: ZSet<Extension, IntegerWeight>
    ): ZSet<Extension, IntegerWeight> {
        var result = deltaJ

        // Intersect with z⁻¹ of all relations processed before j
        for (k in 0 until j) {
            result = relations[k].accumulated.intersect(prefix, result)
        }

        // Don't forget minIndex if it comes after j!
        if (minIndex > j) {
            result = relations[minIndex].accumulated.intersect(prefix, result)
        }

        return result
    }
}
