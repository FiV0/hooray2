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

                override fun intersect(
                    prefix: Prefix,
                    extensions: ZSet<Extension, IntegerWeight>
                ): ZSet<Extension, IntegerWeight> =
                    candidates(prefix).equiJoin(extensions)

                override fun participatesInLevel(level: Int): Boolean = levelSet.contains(level)
            }
        }
    }
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
    val entityVarIndex: Int,
    val valueVarIndex: Int
) {
    fun variableIndexes(): List<Int> =
        listOf(entityVarIndex, valueVarIndex).filter { it >= 0 }.distinct()

    fun participatesInCanonicalLevel(level: Int): Boolean =
        entityVarIndex == level || valueVarIndex == level

    fun extender(indices: ZSetIndices, termOrder: List<Int>): ZSetPrefixExtender {
        val entityLevel = if (entityVarIndex >= 0) termOrder.indexOf(entityVarIndex) else -1
        val valueLevel = if (valueVarIndex >= 0) termOrder.indexOf(valueVarIndex) else -1

        val (indexedZSet, fixedPrefix, participatingLevels) = when {
            entityConstant != null && valueVarIndex >= 0 -> {
                TripleIndex(indices.aev, listOf(attribute, entityConstant), listOf(valueLevel))
            }
            valueConstant != null && entityVarIndex >= 0 -> {
                TripleIndex(indices.ave, listOf(attribute, valueConstant), listOf(entityLevel))
            }
            entityVarIndex >= 0 && valueVarIndex >= 0 && entityLevel < valueLevel -> {
                TripleIndex(indices.aev, listOf(attribute), listOf(entityLevel, valueLevel))
            }
            entityVarIndex >= 0 && valueVarIndex >= 0 -> {
                TripleIndex(indices.ave, listOf(attribute), listOf(valueLevel, entityLevel))
            }
            else -> throw IllegalArgumentException("Unsupported compiled triple pattern $this")
        }

        val localIndex = indexedZSet.getByPrefix(fixedPrefix) ?: emptyIndexFor(participatingLevels.size)
        return ZSetPrefixExtender.fromIndexedZSet(localIndex, participatingLevels)
    }

    private data class TripleIndex(
        val indexedZSet: IndexedZSet<Any, IntegerWeight>,
        val fixedPrefix: Prefix,
        val participatingLevels: List<Int>
    )
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
    private var oldState: ZSetIndices = emptyZSetIndices()
    private var pendingDelta: ZSetIndices? = null

    private fun termOrder(deltaPattern: CompiledTriplePattern): List<Int> {
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

    private fun permuteToCanonical(termResult: ResultZSet, order: List<Int>): ResultZSet {
        if (order == (0 until levels).toList()) return termResult

        val result = mutableMapOf<ResultTuple, IntegerWeight>()
        for ((tuple, weight) in termResult.entries()) {
            val canonical = MutableList<Any?>(levels) { null }
            for ((termIndex, canonicalIndex) in order.withIndex()) {
                canonical[canonicalIndex] = tuple[termIndex]
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
        val newState = oldState.add(input)
        var result = ZSet.empty<ResultTuple>()

        for (deltaIndex in patterns.indices.reversed()) {
            val order = termOrder(patterns[deltaIndex])
            val extenders = patterns.mapIndexed { patternIndex, pattern ->
                val state = when {
                    patternIndex < deltaIndex -> oldState
                    patternIndex == deltaIndex -> input
                    else -> newState
                }
                pattern.extender(state, order)
            }

            val term = permuteToCanonical(runWeightedWcoj(extenders), order)
            result = result.add(term)
        }

        pendingDelta = input
        return result
    }

    fun commit() {
        pendingDelta?.let { oldState = oldState.add(it) }
        pendingDelta = null
    }
}

fun emptyZSetIndices(): ZSetIndices =
    ZSetIndices(
        IndexedZSet.empty(IntegerWeight.ZERO, IntegerWeight.ONE),
        IndexedZSet.empty(IntegerWeight.ZERO, IntegerWeight.ONE),
        IndexedZSet.empty(IntegerWeight.ZERO, IntegerWeight.ONE),
        IndexedZSet.empty(IntegerWeight.ZERO, IntegerWeight.ONE)
    )
