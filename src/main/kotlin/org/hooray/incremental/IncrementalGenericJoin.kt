package org.hooray.incremental

import kotlinx.collections.immutable.persistentListOf
import org.hooray.algo.Extension
import org.hooray.algo.Prefix
import org.hooray.algo.ResultTuple
import org.hooray.iterator.LevelParticipation

/**
 * A weighted prefix extender used by the WCOJ term evaluator.
 */
interface ZSetPrefixExtender : LevelParticipation {
    fun count(prefix: Prefix): Int
    fun propose(prefix: Prefix): ZSet<Extension, IntegerWeight>
    fun intersect(prefix: Prefix, extensions: ZSet<Extension, IntegerWeight>): ZSet<Extension, IntegerWeight>

    companion object {
        @Suppress("UNCHECKED_CAST")
        fun fromIndexedZSet(
            indexedZSet: IZSet<*, IntegerWeight, *>,
            participatingLevels: List<Int>
        ): ZSetPrefixExtender {
            val levelSet = participatingLevels.toSet()
            val prefixExtractor: (Prefix) -> Prefix = { prefix ->
                prefix.filterIndexed { index, _ -> levelSet.contains(index) }
            }

            return object : ZSetPrefixExtender {
                private fun candidates(prefix: Prefix): ZSet<Extension, IntegerWeight> {
                    val localPrefix = prefixExtractor(prefix)
                    return when (indexedZSet) {
                        is IndexedZSet<*, IntegerWeight> ->
                            (indexedZSet.getByPrefix(localPrefix)?.asZSetView() ?: ZSet.empty()) as ZSet<Extension, IntegerWeight>
                        is ZSet<*, IntegerWeight> ->
                            if (localPrefix.isEmpty()) indexedZSet as ZSet<Extension, IntegerWeight> else ZSet.empty()
                        else -> throw IllegalArgumentException("Unsupported IZSet type ${indexedZSet::class}")
                    }
                }

                override fun count(prefix: Prefix): Int = candidates(prefix).size

                override fun propose(prefix: Prefix): ZSet<Extension, IntegerWeight> = candidates(prefix)

                override fun intersect(prefix: Prefix, extensions: ZSet<Extension, IntegerWeight>): ZSet<Extension, IntegerWeight> =
                    candidates(prefix).equiJoin(extensions)

                override fun participatesInLevel(level: Int): Boolean = levelSet.contains(level)
            }
        }
    }
}

enum class ArrangementState {
    OLD,
    DELTA,
    CURRENT
}

data class ArrangedView(
    val index: IZSet<Any, IntegerWeight, *>,
    val participatingLevels: List<Int>
) {
    fun toExtender(): ZSetPrefixExtender =
        ZSetPrefixExtender.fromIndexedZSet(index, participatingLevels)
}

private enum class ArrangementKind {
    AEV,
    AVE
}

/**
 * Compiled metadata for one supported incremental triple pattern.
 *
 * Missing variable positions are represented as -1 so Clojure can call this
 * constructor without nullable integer interop surprises.
 */
data class CompiledTriplePattern(
    val entityConstant: Any?,
    val attribute: Any,
    val valueConstant: Any?,
    val entityVarCanonicalIndex: Int,
    val valueVarCanonicalIndex: Int
) {
    private var oldAev: IZSet<Any, IntegerWeight, *>? = null
    private var oldAve: IZSet<Any, IntegerWeight, *>? = null
    private var pendingDeltaAev: IZSet<Any, IntegerWeight, *>? = null
    private var pendingDeltaAve: IZSet<Any, IntegerWeight, *>? = null

    fun variableIndexes(): List<Int> =
        listOf(entityVarCanonicalIndex, valueVarCanonicalIndex).filter { it >= 0 }.distinct()

    fun participatesInCanonicalLevel(level: Int): Boolean =
        entityVarCanonicalIndex == level || valueVarCanonicalIndex == level

    fun receiveDelta(indices: ZSetIndices) {
        pendingDeltaAev = extractArrangement(indices, ArrangementKind.AEV)
        pendingDeltaAve = extractArrangement(indices, ArrangementKind.AVE)
    }

    fun view(variableOrder: List<Int>, state: ArrangementState): ArrangedView {
        val entityLevel = if (entityVarCanonicalIndex >= 0) variableOrder.indexOf(entityVarCanonicalIndex) else -1
        val valueLevel = if (valueVarCanonicalIndex >= 0) variableOrder.indexOf(valueVarCanonicalIndex) else -1

        val (arrangementKind, participatingLevels) = when {
            entityConstant != null && valueVarCanonicalIndex >= 0 ->
                ArrangementKind.AEV to listOf(valueLevel)
            valueConstant != null && entityVarCanonicalIndex >= 0 ->
                ArrangementKind.AVE to listOf(entityLevel)
            entityVarCanonicalIndex >= 0 && valueVarCanonicalIndex >= 0 && entityLevel < valueLevel ->
                ArrangementKind.AEV to listOf(entityLevel, valueLevel)
            entityVarCanonicalIndex >= 0 && valueVarCanonicalIndex >= 0 ->
                ArrangementKind.AVE to listOf(valueLevel, entityLevel)
            else -> throw IllegalArgumentException("Unsupported compiled triple pattern $this")
        }

        return ArrangedView(arrangement(arrangementKind, state), participatingLevels)
    }

    fun commit() {
        pendingDeltaAev?.let { oldAev = addArrangements(oldAev, it) }
        pendingDeltaAve?.let { oldAve = addArrangements(oldAve, it) }
        pendingDeltaAev = null
        pendingDeltaAve = null
    }

    private fun arrangement(kind: ArrangementKind, state: ArrangementState): IZSet<Any, IntegerWeight, *> {
        val old = oldArrangement(kind)
        val delta = deltaArrangement(kind)
        return when (state) {
            ArrangementState.OLD -> old
            ArrangementState.DELTA -> delta
            ArrangementState.CURRENT -> addArrangements(old, delta)
        }
    }

    private fun oldArrangement(kind: ArrangementKind): IZSet<Any, IntegerWeight, *> =
        when (kind) {
            ArrangementKind.AEV -> oldAev
            ArrangementKind.AVE -> oldAve
        } ?: emptyArrangement(kind)

    private fun deltaArrangement(kind: ArrangementKind): IZSet<Any, IntegerWeight, *> =
        when (kind) {
            ArrangementKind.AEV -> pendingDeltaAev
            ArrangementKind.AVE -> pendingDeltaAve
        } ?: emptyArrangement(kind)

    private fun extractArrangement(indices: ZSetIndices, kind: ArrangementKind): IZSet<Any, IntegerWeight, *>? {
        val (indexedZSet, fixedPrefix) = when (kind) {
            ArrangementKind.AEV -> indices.aev to aevFixedPrefix()
            ArrangementKind.AVE -> indices.ave to aveFixedPrefix()
        }
        if (fixedPrefix == null) return null
        @Suppress("UNCHECKED_CAST")
        return indexedZSet.getByPrefix(fixedPrefix) as IZSet<Any, IntegerWeight, *>?
    }

    private fun aevFixedPrefix(): Prefix? =
        when {
            entityConstant != null && valueVarCanonicalIndex >= 0 -> listOf(attribute, entityConstant)
            entityVarCanonicalIndex >= 0 && valueVarCanonicalIndex >= 0 -> listOf(attribute)
            else -> null
        }

    private fun aveFixedPrefix(): Prefix? =
        when {
            valueConstant != null && entityVarCanonicalIndex >= 0 -> listOf(attribute, valueConstant)
            entityVarCanonicalIndex >= 0 && valueVarCanonicalIndex >= 0 -> listOf(attribute)
            else -> null
        }

    private fun emptyArrangement(kind: ArrangementKind): IZSet<Any, IntegerWeight, *> =
        emptyIndexFor(variableCount(kind))

    private fun variableCount(kind: ArrangementKind): Int =
        when (kind) {
            ArrangementKind.AEV -> aevFixedPrefix()?.let { 3 - it.size } ?: 0
            ArrangementKind.AVE -> aveFixedPrefix()?.let { 3 - it.size } ?: 0
        }
}

private fun emptyIndexFor(levels: Int): IZSet<Any, IntegerWeight, *> =
    when (levels) {
        1 -> ZSet.empty<Any>()
        2 -> IndexedZSet.empty<Any, IntegerWeight>(IntegerWeight.ZERO, IntegerWeight.ONE)
        else -> throw IllegalArgumentException("Unsupported triple variable count $levels")
    }

private fun addIndexedZSets(
    left: IndexedZSet<Any, IntegerWeight>,
    right: IndexedZSet<Any, IntegerWeight>
): IndexedZSet<Any, IntegerWeight> =
    when {
        left.isEmpty() -> right
        right.isEmpty() -> left
        else -> left.add(right)
    }

@Suppress("UNCHECKED_CAST")
private fun addArrangements(
    left: IZSet<Any, IntegerWeight, *>?,
    right: IZSet<Any, IntegerWeight, *>?
): IZSet<Any, IntegerWeight, *> {
    if (left == null || left.isEmpty()) return right ?: ZSet.empty<Any>()
    if (right == null || right.isEmpty()) return left
    return when {
        left is ZSet<*, *> && right is ZSet<*, *> ->
            (left as ZSet<Any, IntegerWeight>).add(right as ZSet<Any, IntegerWeight>)
        left is IndexedZSet<*, *> && right is IndexedZSet<*, *> ->
            (left as IndexedZSet<Any, IntegerWeight>).add(right as IndexedZSet<Any, IntegerWeight>)
        else -> throw IllegalStateException("Cannot add arrangements ${left::class} and ${right::class}")
    }
}

fun ZSetIndices.add(other: ZSetIndices): ZSetIndices =
    ZSetIndices(
        addIndexedZSets(eav, other.eav),
        addIndexedZSets(aev, other.aev),
        addIndexedZSets(ave, other.ave),
        addIndexedZSets(vae, other.vae)
    )

internal class IncrementalWcojJoinEngine(
    private val patterns: List<CompiledTriplePattern>,
    private val levels: Int
) {
    private fun variableOrderForDeltaTerm(deltaPattern: CompiledTriplePattern): List<Int> {
        val deltaVariables = deltaPattern.variableIndexes()
        val remaining = (0 until levels).filterNot { deltaVariables.contains(it) }
        return deltaVariables + remaining
    }

    private fun runWeightedWcoj(extenders: List<ZSetPrefixExtender>): ResultZSet {
        var prefixes = ZSet.singleton<ResultTuple>(persistentListOf())

        for (level in 0 until levels) {
            val participating = extenders.filter { it.participatesInLevel(level) }
            if (participating.isEmpty()) continue

            var nextPrefixes = ZSet.empty<ResultTuple>()
            for ((prefix, prefixWeight) in prefixes.entries()) {
                val minIndex = participating.indices.minBy { participating[it].count(prefix) }
                var extensions = participating[minIndex].propose(prefix)

                for (i in participating.indices) {
                    if (i != minIndex) {
                        extensions = participating[i].intersect(prefix, extensions)
                    }
                }

                for ((extension, extensionWeight) in extensions.entries()) {
                    val nextPrefix = (prefix + extension) as ResultTuple
                    val weight = prefixWeight.multiply(extensionWeight)
                    nextPrefixes = nextPrefixes.add(ZSet.singleton(nextPrefix, weight))
                }
            }
            prefixes = nextPrefixes
            if (prefixes.isEmpty()) break
        }

        return prefixes
    }

    private fun permuteToCanonical(termResult: ResultZSet, variableOrder: List<Int>): ResultZSet {
        if (variableOrder == (0 until levels).toList()) return termResult

        val result = mutableMapOf<ResultTuple, IntegerWeight>()
        for ((tuple, weight) in termResult.entries()) {
            val canonical = MutableList<Any?>(levels) { null }
            for ((variableOrderIndex, canonicalIndex) in variableOrder.withIndex()) {
                canonical[canonicalIndex] = tuple[variableOrderIndex]
            }
            @Suppress("UNCHECKED_CAST")
            val resultTuple = canonical as ResultTuple
            result.merge(resultTuple, weight) { left, right ->
                val sum = left.add(right)
                if (sum.isZero()) null else sum
            }
        }
        return ZSet.fromMap(result)
    }

    fun eval(input: ZSetIndices): ResultZSet {
        patterns.forEach { it.receiveDelta(input) }
        var result = ZSet.empty<ResultTuple>()

        for (deltaIndex in patterns.indices.reversed()) {
            val variableOrder = variableOrderForDeltaTerm(patterns[deltaIndex])
            val extenders = patterns.mapIndexed { patternIndex, pattern ->
                val arrangementState = when {
                    patternIndex < deltaIndex -> ArrangementState.OLD
                    patternIndex == deltaIndex -> ArrangementState.DELTA
                    else -> ArrangementState.CURRENT
                }
                pattern.view(variableOrder, arrangementState).toExtender()
            }

            val term = permuteToCanonical(runWeightedWcoj(extenders), variableOrder)
            result = result.add(term)
        }

        return result
    }

    fun commit() {
        patterns.forEach { it.commit() }
    }
}

fun emptyZSetIndices(): ZSetIndices =
    ZSetIndices(
        IndexedZSet.empty(IntegerWeight.ZERO, IntegerWeight.ONE),
        IndexedZSet.empty(IntegerWeight.ZERO, IntegerWeight.ONE),
        IndexedZSet.empty(IntegerWeight.ZERO, IntegerWeight.ONE),
        IndexedZSet.empty(IntegerWeight.ZERO, IntegerWeight.ONE)
    )
