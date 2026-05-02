package org.hooray.incremental

import org.hooray.algo.Extension
import org.hooray.algo.Prefix
import org.hooray.algo.ResultTuple
import org.hooray.iterator.LevelParticipation
import kotlinx.collections.immutable.persistentListOf

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
                    zset.equiJoin(extensions)
            }
        }

        @Suppress("UNCHECKED_CAST")
        // The prefix might contain more variables than the index participates in, so we need a way to extract the relevant part.
        // TODO This constant prefix extraction will definitely cost us here.
        fun fromIndexedZSet(indexedZSet: IndexedZSet<*, IntegerWeight>, prefixExtracter: (Prefix) -> Prefix): ZSetPrefixExtender {
            return object : ZSetPrefixExtender {
                override fun count(prefix: Prefix): Int = indexedZSet.getByPrefix(prefixExtracter(prefix))?.size ?: 0

                override fun propose(prefix: Prefix): ZSet<Extension, IntegerWeight> =
                    (indexedZSet.getByPrefix(prefixExtracter(prefix))?.asZSetView() ?: ZSet.empty()) as ZSet<Extension, IntegerWeight>

                override fun intersect(prefix: Prefix, extensions: ZSet<Extension, IntegerWeight>): ZSet<Extension, IntegerWeight> =
                    ((indexedZSet.getByPrefix(prefixExtracter(prefix))?.asZSetView() ?: ZSet.empty()) as ZSet<Extension, IntegerWeight>).equiJoin(extensions)
            }
        }

        fun difference(newExtender: ZSetPrefixExtender, oldExtender: ZSetPrefixExtender): ZSetPrefixExtender {
            return object : ZSetPrefixExtender {
                override fun count(prefix: Prefix): Int =
                    maxOf(newExtender.count(prefix), oldExtender.count(prefix))

                override fun propose(prefix: Prefix): ZSet<Extension, IntegerWeight> =
                    oldExtender.propose(prefix).negate().add(newExtender.propose(prefix))

                override fun intersect(prefix: Prefix, extensions: ZSet<Extension, IntegerWeight>): ZSet<Extension, IntegerWeight> =
                    oldExtender.intersect(prefix, extensions).negate().add(newExtender.intersect(prefix, extensions))
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

    /** Extender over z⁻¹ + Δz (the relation state after the current transaction) */
    val current: ZSetPrefixExtender
}

/**
 * Shared implementation of the corrected level-wise incremental GenericJoin algorithm.
 *
 * In the math, P_i is the materialized Z-set of prefixes after binding i
 * variables, with P_0 being the singleton empty prefix. In this zero-based
 * implementation, oldPrefixStates[level] stores P_{level + 1}.
 *
 * ΔP_i is the Z-set of changed prefixes for the current transaction.
 *
 * For every level i we compute:
 *
 * ΔP_i = Extend_i(ΔP_{i-1}, E_i_new)
 *      + Extend_i(P_{i-1_old}, ΔE_i)
 *
 * The first term carries prefixes that changed at the previous level through the
 * new extension relation. The second term is the important corrected part: it
 * handles old prefixes whose valid next extensions changed even though the
 * prefix itself did not appear in ΔP_{i-1}.
 */
internal class IncrementalGenericJoinEngine(
    private val relations: List<IncrementalIndex>,
    private val levels: Int
) {
    private val relationSets: List<List<IncrementalIndex>> = List(levels) { level ->
        relations.filter { it.participatesInLevel(level) }
    }

    private var oldPrefixStates: List<IZSet<Any, IntegerWeight, *>> = List(levels) { level ->
        emptyPrefixState(level)
    }

    private var pendingPrefixStates: List<IZSet<Any, IntegerWeight, *>>? = null

    private fun emptyPrefixState(level: Int): IZSet<Any, IntegerWeight, *> =
        if (level == 0) ZSet.empty<Any>() else IndexedZSet.empty<Any, IntegerWeight>(IntegerWeight.ZERO, IntegerWeight.ONE)

    @Suppress("UNCHECKED_CAST")
    private fun addPrefixStates(
        left: IZSet<Any, IntegerWeight, *>,
        right: IZSet<Any, IntegerWeight, *>
    ): IZSet<Any, IntegerWeight, *> {
        if (left.isEmpty()) return right
        if (right.isEmpty()) return left
        return when {
            left is ZSet<*, *> && right is ZSet<*, *> ->
                (left as ZSet<Any, IntegerWeight>).add(right as ZSet<Any, IntegerWeight>)
            left is IndexedZSet<*, *> && right is IndexedZSet<*, *> ->
                (left as IndexedZSet<Any, IntegerWeight>).add(right as IndexedZSet<Any, IntegerWeight>)
            else -> throw IllegalStateException("Cannot add prefix states ${left::class} and ${right::class}")
        }
    }

    private fun currentExtensions(prefix: Prefix, relations: List<IncrementalIndex>): ZSet<Extension, IntegerWeight> =
        intersectExtensions(prefix, relations.map { it.current })

    /**
     * Intersect a fixed list of extenders for one prefix.
     *
     * This is the standard GenericJoin level step: start with the smallest
     * candidate set and intersect all other participating relations into it.
     */
    private fun intersectExtensions(prefix: Prefix, extenders: List<ZSetPrefixExtender>): ZSet<Extension, IntegerWeight> {
        if (extenders.isEmpty()) return ZSet.empty()

        // Start with the smallest candidate set, preserving the WCOJ selection heuristic.
        val minIndex = extenders.indices.minBy { extenders[it].count(prefix) }
        var extensions = extenders[minIndex].propose(prefix)

        // Intersect the proposed candidates with every other relation at this level.
        for (i in extenders.indices) {
            if (i != minIndex) {
                extensions = extenders[i].intersect(prefix, extensions)
            }
        }

        return extensions
    }

    /**
     * Compute ΔE_i for one prefix using the telescoping expansion:
     *
     * ΔE_i = Σ_t old_1 ⋈ ... ⋈ old_{t-1} ⋈ Δ_t ⋈ new_{t+1} ⋈ ... ⋈ new_n
     *
     * Δ_t is not the raw relation delta. It is the change in that relation's
     * extension set for this level: E_t_new - E_t_old. This distinction matters
     * for non-leaf prefixes, where adding or removing a tuple under an existing
     * prefix may leave the projected extension set unchanged.
     */
    private fun changedExtensions(prefix: Prefix, relations: List<IncrementalIndex>): ZSet<Extension, IntegerWeight> {
        if (relations.isEmpty()) return ZSet.empty()

        // Any deterministic order is correct for the telescoping sum. Use the
        // current candidate count so small extension sets tend to appear early.
        val orderedRelations = relations.sortedBy { it.current.count(prefix) }
        var result = ZSet.empty<Extension>()

        for (deltaIndex in orderedRelations.indices) {
            val deltaExtender = ZSetPrefixExtender.difference(
                orderedRelations[deltaIndex].current,
                orderedRelations[deltaIndex].accumulated
            )

            // Start the term at Δ_t: the changed extension set of one relation.
            var term = deltaExtender.propose(prefix)

            // Intersect old relations before Δ_t.
            for (oldIndex in 0 until deltaIndex) {
                term = orderedRelations[oldIndex].accumulated.intersect(prefix, term)
            }

            // Intersect new relations after Δ_t.
            for (newIndex in (deltaIndex + 1) until orderedRelations.size) {
                term = orderedRelations[newIndex].current.intersect(prefix, term)
            }

            // Add this telescoping term into ΔE_i.
            result = result.add(term)
        }

        return result
    }

    private fun extensionDeltaExtender(relations: List<IncrementalIndex>): ZSetPrefixExtender {
        return object : ZSetPrefixExtender {
            override fun count(prefix: Prefix): Int = changedExtensions(prefix, relations).size

            override fun propose(prefix: Prefix): ZSet<Extension, IntegerWeight> =
                changedExtensions(prefix, relations)

            override fun intersect(prefix: Prefix, extensions: ZSet<Extension, IntegerWeight>): ZSet<Extension, IntegerWeight> =
                changedExtensions(prefix, relations).equiJoin(extensions)
        }
    }

    private fun currentExtender(relations: List<IncrementalIndex>): ZSetPrefixExtender {
        return object : ZSetPrefixExtender {
            override fun count(prefix: Prefix): Int = currentExtensions(prefix, relations).size

            override fun propose(prefix: Prefix): ZSet<Extension, IntegerWeight> =
                currentExtensions(prefix, relations)

            override fun intersect(prefix: Prefix, extensions: ZSet<Extension, IntegerWeight>): ZSet<Extension, IntegerWeight> =
                currentExtensions(prefix, relations).equiJoin(extensions)
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun extendRoot(extensions: ZSet<Extension, IntegerWeight>): IZSet<Any, IntegerWeight, *> =
        extensions as ZSet<Any, IntegerWeight>

    @Suppress("UNCHECKED_CAST")
    private fun extendPrefixes(
        prefixes: IZSet<Any, IntegerWeight, *>,
        prefixSize: Int,
        extender: ZSetPrefixExtender
    ): IZSet<Any, IntegerWeight, *> {
        if (prefixes.isEmpty()) return emptyPrefixState(prefixSize)

        return prefixes.extendLeaves { prefix, weight ->
            require(prefix.size == prefixSize) { "Prefix size ${prefix.size} does not match current level $prefixSize" }
            extender.propose(prefix).multiply(weight)
        } as IZSet<Any, IntegerWeight, *>
    }

    fun eval(input: ZSetIndices): ResultZSet {
        // 1. Distribute relation deltas. Relations now expose both E_old and E_new.
        relations.forEach { rel -> rel.receiveDelta(input) }

        val newPrefixStates = MutableList(levels) { level -> emptyPrefixState(level) }
        var previousDelta: IZSet<Any, IntegerWeight, *> = ZSet.empty<Any>()

        for (level in 0 until levels) {
            // The code-level `level` computes the math-level ΔP_{level + 1}.
            val participating = relationSets[level]

            // 2. Build changed extensions ΔE_i for this level.
            val deltaExtensions = extensionDeltaExtender(participating)

            // 3. Extend changed prefixes through the new extension relation:
            //    Extend_i(ΔP_{i-1}, E_i_new).
            val changedPrefixTerm =
                if (level == 0)
                    emptyPrefixState(level)
                else
                    extendPrefixes(previousDelta, level, currentExtender(participating))

            // 4. Extend old prefixes through changed extensions:
            //    Extend_i(P_{i-1_old}, ΔE_i).
            val oldPrefixTerm =
                if (level == 0)
                    extendRoot(deltaExtensions.propose(persistentListOf()))
                else
                    extendPrefixes(oldPrefixStates[level - 1], level, deltaExtensions)

            // 5. Add both terms to obtain ΔP_i.
            val levelDelta = addPrefixStates(changedPrefixTerm, oldPrefixTerm)

            // 6. Materialize pending P_new_i = P_old_i + ΔP_i.
            newPrefixStates[level] = addPrefixStates(oldPrefixStates[level], levelDelta)

            previousDelta = levelDelta
        }

        pendingPrefixStates = newPrefixStates
        return previousDelta.flatZSet()
    }

    fun commit() {
        // Commit relation states first, so their z⁻¹ matches the prefix states below.
        relations.forEach { it.commit() }

        // Advance the materialized prefix states from P_old to P_new.
        pendingPrefixStates?.let { oldPrefixStates = it }
        pendingPrefixStates = null
    }
}

