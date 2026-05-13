package org.hooray.incremental.stream

import org.hooray.incremental.ArrangedView
import org.hooray.incremental.ArrangementState
import org.hooray.incremental.CompiledTriplePattern
import org.hooray.incremental.ZSetIndices

class BaseRelationStream(
    private val pattern: CompiledTriplePattern
) {
    fun variableIndexes(): List<Int> =
        pattern.variableIndexes()

    fun receiveDelta(indices: ZSetIndices) {
        pattern.receiveDelta(indices)
    }

    fun view(variableOrder: List<Int>, state: ArrangementState): ArrangedView =
        pattern.view(variableOrder, state)

    fun commit() {
        pattern.commit()
    }
}
