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

    companion object {
        fun fromZSet(zset: ZSet<Extension, IntegerWeight>): ZSetPrefixExtender {
            return object : ZSetPrefixExtender {
                override fun count(prefix: Prefix): Int = zset.size

                // Here we assume that if we get a simple ZSet the prefix is not relevant for constraining the index.
                override fun propose(prefix: Prefix): ZSet<Extension, IntegerWeight> = zset

                override fun intersect(prefix: Prefix, extensions: ZSet<Extension, IntegerWeight>): ZSet<Extension, IntegerWeight> =
                    zset.naturalJoin(extensions)
            }
        }

        @Suppress("UNCHECKED_CAST")
        fun fromIndexedZSet(indexedZSet: IndexedZSet<*, IntegerWeight>): ZSetPrefixExtender {
            return object : ZSetPrefixExtender {
                override fun count(prefix: Prefix): Int = indexedZSet.getByPrefix(prefix).size

                override fun propose(prefix: Prefix): ZSet<Extension, IntegerWeight> = indexedZSet.getByPrefix(prefix) as ZSet<Extension, IntegerWeight>

                override fun intersect(prefix: Prefix, extensions: ZSet<Extension, IntegerWeight>): ZSet<Extension, IntegerWeight> =
                    (indexedZSet.getByPrefix(prefix) as ZSet<Extension, IntegerWeight>).naturalJoin(extensions)
            }
        }
    }
}

/**
 * Manages the incremental state for a single indexed relation.
 * Provides separate extenders for delta and accumulated state.
 */
interface IncrementalIndex : LevelParticipation {
    /** Receive the delta for this transaction */
    fun receiveDelta(delta: ZSetIndices)

    /** Merge current delta into accumulated state (call after join completes) */
    fun commit()

    /** Extender over the current delta */
    val delta: ZSetPrefixExtender

    /** Extender over z⁻¹ (accumulated previous state) */
    val accumulated: ZSetPrefixExtender
}

class IncrementalGenericJoin(private val relations: List<IncrementalIndex>, private val levels: Int) : IncrementalJoin<ResultTuple> {
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

        // TODO if at any point runningDelta becomes empty, only the term3 remains to be computed
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

                           // Δ_{1..i-1} ⋈ Δᵢ
            runningDelta = runningDelta.naturalJoin(deltaJ) +
                           // + Δ_{1..i-1} ⋈ z⁻¹(i)
                           relations[j].accumulated.intersect(prefix, runningDelta) +
                           // + Δᵢ ⋈ z⁻¹(1) ⋈ z⁻¹(2) ⋈ ... ⋈ z⁻¹(i-1)
                           term3
        }

        return runningDelta
    }


    override fun join(deltas: ZSetIndices): ZSet<ResultTuple, IntegerWeight> {
        // 1. Distribute deltas
        relations.forEach { rel -> rel.receiveDelta(deltas) }

        // 2. Compute join level by level
        // TODO make extendLeaves work on empty IndexedZSet
        val participatingLevel1 = relations.filter { it.participatesInLevel(0) }
        var result: IZSet<Any, IntegerWeight, *> = computeLevelDelta(emptyList(), participatingLevel1)

        for (level in 1 until levels) {
            val participating = relations.filter { it.participatesInLevel(level) }
            result = result.extendLeaves { prefix, weight ->
                computeLevelDelta(prefix, participating).multiply(weight)
            }
        }

        // 3. Commit deltas
        relations.forEach { it.commit() }

        return result.flatZSet()
    }
}