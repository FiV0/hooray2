package org.hooray.incremental.stream

import org.hooray.algo.Prefix
import org.hooray.incremental.ArrangedView
import org.hooray.incremental.Arrangement
import org.hooray.incremental.ArrangementState
import org.hooray.incremental.CompiledTriplePattern
import org.hooray.incremental.ZSetIndices

class BaseRelationStream(
    private val pattern: CompiledTriplePattern
) {
    private enum class ArrangementKind {
        AEV,
        AVE
    }

    private val arrangementKinds: List<ArrangementKind> =
        when {
            pattern.entityConstant != null && pattern.valueVarCanonicalIndex >= 0 -> listOf(ArrangementKind.AEV)
            pattern.valueConstant != null && pattern.entityVarCanonicalIndex >= 0 -> listOf(ArrangementKind.AVE)
            pattern.entityVarCanonicalIndex >= 0 && pattern.valueVarCanonicalIndex >= 0 ->
                listOf(ArrangementKind.AEV, ArrangementKind.AVE)
            else -> throw IllegalArgumentException("Unsupported compiled triple pattern $pattern")
        }

    private var activeInput: ZSetIndices? = null
    private var currentArrangements: MutableMap<ArrangementKind, Arrangement> = emptyArrangements()

    fun variableIndexes(): List<Int> =
        pattern.variableIndexes()

    fun receiveDelta(indices: ZSetIndices) {
        activeInput = indices
    }

    fun view(variableOrder: List<Int>, state: ArrangementState): ArrangedView {
        val entityLevel = if (pattern.entityVarCanonicalIndex >= 0) variableOrder.indexOf(pattern.entityVarCanonicalIndex) else -1
        val valueLevel = if (pattern.valueVarCanonicalIndex >= 0) variableOrder.indexOf(pattern.valueVarCanonicalIndex) else -1

        val (arrangementKind, participatingLevels) = when {
            pattern.entityConstant != null && pattern.valueVarCanonicalIndex >= 0 ->
                ArrangementKind.AEV to listOf(valueLevel)
            pattern.valueConstant != null && pattern.entityVarCanonicalIndex >= 0 ->
                ArrangementKind.AVE to listOf(entityLevel)
            pattern.entityVarCanonicalIndex >= 0 && pattern.valueVarCanonicalIndex >= 0 && entityLevel < valueLevel ->
                ArrangementKind.AEV to listOf(entityLevel, valueLevel)
            pattern.entityVarCanonicalIndex >= 0 && pattern.valueVarCanonicalIndex >= 0 ->
                ArrangementKind.AVE to listOf(valueLevel, entityLevel)
            else -> throw IllegalArgumentException("Unsupported compiled triple pattern $pattern")
        }

        return ArrangedView(arrangement(arrangementKind, state), participatingLevels)
    }

    fun commit() {
        val input = activeInput ?: return
        for (kind in arrangementKinds) {
            currentArrangements[kind] = current(kind).add(extractArrangement(input, kind))
        }
        activeInput = null
    }

    private fun arrangement(kind: ArrangementKind, state: ArrangementState): Arrangement =
        when (state) {
            ArrangementState.DELTA -> activeInput?.let { extractArrangement(it, kind) } ?: emptyArrangement()
            ArrangementState.CURRENT -> current(kind)
        }

    private fun current(kind: ArrangementKind): Arrangement =
        currentArrangements[kind] ?: throw IllegalStateException("No current $kind arrangement for $pattern")

    private fun extractArrangement(indices: ZSetIndices, kind: ArrangementKind): Arrangement {
        val indexedZSet = when (kind) {
            ArrangementKind.AEV -> indices.aev
            ArrangementKind.AVE -> indices.ave
        }
        return Arrangement.from(indexedZSet.getByPrefix(fixedPrefix(kind))) ?: emptyArrangement()
    }

    private fun fixedPrefix(kind: ArrangementKind): Prefix {
        val prefix = mutableListOf(pattern.attribute)
        when (kind) {
            ArrangementKind.AEV -> {
                if (pattern.entityConstant != null) prefix.add(pattern.entityConstant)
            }
            ArrangementKind.AVE -> {
                if (pattern.valueConstant != null) prefix.add(pattern.valueConstant)
            }
        }
        return prefix
    }

    private fun emptyArrangements(): MutableMap<ArrangementKind, Arrangement> =
        arrangementKinds.associateWith { emptyArrangement() }.toMutableMap()

    private fun emptyArrangement(): Arrangement =
        Arrangement.empty(pattern.variableIndexes().size)
}
