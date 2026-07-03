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

                override fun participatesInLevel(level: Long): Boolean = levelSet.contains(level.toInt())
            }
        }
    }
}

enum class ArrangementState {
    CURRENT,
    DELTA
}

private enum class ArrangementKind {
    AEV,
    AVE
}

sealed interface Arrangement {
    val index: IZSet<Any, IntegerWeight, *>

    fun add(other: Arrangement): Arrangement

    companion object {
        @Suppress("UNCHECKED_CAST")
        fun from(index: IZSet<out Any?, IntegerWeight, *>?): Arrangement? =
            when (index) {
                null -> null
                is ZSet<*, *> -> FlatArrangement(index as ZSet<Any, IntegerWeight>)
                is IndexedZSet<*, *> -> NestedArrangement(index as IndexedZSet<Any, IntegerWeight>)
                else -> throw IllegalArgumentException("Unsupported arrangement type ${index::class}")
            }

        fun empty(levels: Int): Arrangement =
            when (levels) {
                1 -> FlatArrangement(ZSet.empty())
                2 -> NestedArrangement(IndexedZSet.empty(IntegerWeight.ZERO, IntegerWeight.ONE))
                else -> throw IllegalArgumentException("Unsupported triple variable count $levels")
            }
    }
}

data class FlatArrangement(val zSet: ZSet<Any, IntegerWeight>) : Arrangement {
    override val index: IZSet<Any, IntegerWeight, *> = zSet

    override fun add(other: Arrangement): Arrangement {
        require(other is FlatArrangement) {
            "Cannot add flat arrangement to ${other::class.simpleName}"
        }
        return FlatArrangement(zSet.add(other.zSet))
    }
}

data class NestedArrangement(val zSet: IndexedZSet<Any, IntegerWeight>) : Arrangement {
    override val index: IZSet<Any, IntegerWeight, *> = zSet

    override fun add(other: Arrangement): Arrangement {
        require(other is NestedArrangement) {
            "Cannot add nested arrangement to ${other::class.simpleName}"
        }
        return NestedArrangement(zSet.add(other.zSet))
    }
}

data class ArrangedView(val arrangement: Arrangement, val participatingLevels: List<Int>) {
    fun toExtender(): ZSetPrefixExtender = ZSetPrefixExtender.fromIndexedZSet(arrangement.index, participatingLevels)
}

/**
 * An object handling the state of compiled triple pattern during the WCOJ evaluation.
 * current and delta arrangments are used for different variable ordering during the join process.
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
    private val arrangementKinds: List<ArrangementKind> =
        when {
            entityConstant != null && valueVarCanonicalIndex >= 0 -> listOf(ArrangementKind.AEV)
            valueConstant != null && entityVarCanonicalIndex >= 0 -> listOf(ArrangementKind.AVE)
            entityVarCanonicalIndex >= 0 && valueVarCanonicalIndex >= 0 ->
                listOf(ArrangementKind.AEV, ArrangementKind.AVE)
            else -> throw IllegalArgumentException("Unsupported compiled triple pattern $this")
        }

    private var currentArrangements: MutableMap<ArrangementKind, Arrangement> = emptyArrangements()
    private var deltaArrangements: MutableMap<ArrangementKind, Arrangement> = emptyArrangements()

    fun variableIndexes(): List<Int> =
        listOf(entityVarCanonicalIndex, valueVarCanonicalIndex).filter { it >= 0 }.distinct()

    private fun fixedPrefix(kind: ArrangementKind): Prefix {
        val prefix = mutableListOf(attribute)
        when (kind) {
            ArrangementKind.AEV -> {
                if (entityConstant != null) prefix.add(entityConstant)
            }
            ArrangementKind.AVE -> {
                if (valueConstant != null) prefix.add(valueConstant)
            }
        }
        return prefix
    }

    private fun extractArrangement(indices: ZSetIndices, kind: ArrangementKind): Arrangement {
        val indexedZSet = when (kind) {
            ArrangementKind.AEV -> indices.aev
            ArrangementKind.AVE -> indices.ave
        }
        return Arrangement.from(indexedZSet.getByPrefix(fixedPrefix(kind))) ?: emptyArrangement()
    }

    fun receiveDelta(indices: ZSetIndices) {
        deltaArrangements = arrangementKinds.associateWith { extractArrangement(indices, it) }.toMutableMap()
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
        for (kind in arrangementKinds) {
            currentArrangements[kind] = current(kind).add(delta(kind))
        }
        deltaArrangements = emptyArrangements()
    }

    private fun current(kind: ArrangementKind): Arrangement =
        currentArrangements[kind] ?: throw IllegalStateException("No current $kind arrangement for $this")

    private fun delta(kind: ArrangementKind): Arrangement =
        deltaArrangements[kind] ?: throw IllegalStateException("No delta $kind arrangement for $this")

    private fun emptyArrangements(): MutableMap<ArrangementKind, Arrangement> =
        arrangementKinds.associateWith { emptyArrangement() }.toMutableMap()

    private fun emptyArrangement(): Arrangement =
        Arrangement.empty(variableCount())

    private fun arrangement(kind: ArrangementKind, state: ArrangementState): Arrangement =
        when (state) {
            ArrangementState.DELTA -> delta(kind)
            ArrangementState.CURRENT -> current(kind)
        }

    private fun variableCount(): Int =
        variableIndexes().size
}

internal class IncrementalWcojJoinEngine(
    private val patterns: List<CompiledTriplePattern>,
    private val levels: Int
) {

    private fun variableOrderForDeltaTerm(deltaPattern: CompiledTriplePattern): List<Int> {
        val deltaVariables = deltaPattern.variableIndexes()
        val remaining = (0 until levels).filterNot { deltaVariables.contains(it) }
        return deltaVariables + remaining
    }

    private fun zSetGenericJoin(extenders: List<ZSetPrefixExtender>): ResultZSet {
        var prefixes = ZSet.singleton<ResultTuple>(persistentListOf())

        for (level in 0 until levels) {
            val participating = extenders.filter { it.participatesInLevel(level.toLong()) }
            require(participating.isNotEmpty(), { "No extenders participate in level $level, cannot perform join" })

            // TODO really extend the prefixes instead of createing new longer prefixes on every level
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
                    val nextPrefix = (prefix + extension)
                    val weight = prefixWeight.multiply(extensionWeight)
                    nextPrefixes = nextPrefixes.add(ZSet.singleton(nextPrefix, weight))
                }
            }
            prefixes = nextPrefixes
            if (prefixes.isEmpty()) break
        }

        return prefixes
    }

    private fun canonicalTuplePermutation(variableOrder: List<Int>): (ResultTuple) -> ResultTuple {
        if (variableOrder == (0 until levels).toList()) return { it }

        return { tuple ->
            val canonical = MutableList<Any?>(levels) { null }
            for ((variableOrderIndex, canonicalIndex) in variableOrder.withIndex()) {
                canonical[canonicalIndex] = tuple[variableOrderIndex]
            }
            @Suppress("UNCHECKED_CAST")
            canonical as ResultTuple
        }
    }

    private fun permuteToCanonical(termResult: ResultZSet, variableOrder: List<Int>): ResultZSet {
        if (variableOrder == (0 until levels).toList()) return termResult
        val permuteTuple = canonicalTuplePermutation(variableOrder)

        val result = mutableMapOf<ResultTuple, IntegerWeight>()
        for ((tuple, weight) in termResult.entries()) {
            result.merge(permuteTuple(tuple), weight) { left, right ->
                val sum = left.add(right)
                if (sum.isZero()) null else sum
            }
        }
        return ZSet.fromMap(result)
    }

    fun eval(input: ZSetIndices): ResultZSet {

        patterns.forEach { it.receiveDelta(input) }

        var result = ZSet.empty<ResultTuple>()

        for (deltaIndex in patterns.indices) {
            val variableOrder = variableOrderForDeltaTerm(patterns[deltaIndex])
            val extenders = patterns.mapIndexed { patternIndex, pattern ->
                val arrangementState = when {
                    patternIndex == deltaIndex -> ArrangementState.DELTA
                    else -> ArrangementState.CURRENT
                }
                pattern.view(variableOrder, arrangementState).toExtender()
            }

            val term = permuteToCanonical(zSetGenericJoin(extenders), variableOrder)
            patterns[deltaIndex].commit()
            result = result.add(term)
        }

        return result
    }

    fun commit() {
        // This is a no-op as we take advantage
        // patterns.forEach { it.commit() }
    }
}
