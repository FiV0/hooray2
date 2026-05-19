package org.hooray.incremental.stream

import org.hooray.incremental.ArrangementState
import org.hooray.incremental.ResultZSet
import org.hooray.incremental.ZSet
import org.hooray.incremental.ZSetIndices
import org.hooray.incremental.stream.ops.permuteToCanonical
import org.hooray.incremental.stream.ops.zSetGenericJoin

class IncrementalWcojSource(spec: IncrementalWcojJoinSpec) {
    private val streams = spec.patterns.map(::BaseRelationStream)
    private val levels = spec.levels

    fun step(input: ZSetIndices): ResultZSet {
        streams.forEach { it.receiveDelta(input) }

        var result = ZSet.empty<org.hooray.algo.ResultTuple>()

        for (deltaIndex in streams.indices) {
            val variableOrder = variableOrderForDeltaTerm(streams[deltaIndex])
            val extenders = streams.mapIndexed { streamIndex, stream ->
                val arrangementState = when {
                    streamIndex == deltaIndex -> ArrangementState.DELTA
                    else -> ArrangementState.CURRENT
                }
                stream.view(variableOrder, arrangementState).toExtender()
            }

            val term = permuteToCanonical(
                zSetGenericJoin(extenders, levels),
                variableOrder,
                levels
            )
            streams[deltaIndex].commit()
            result = result.add(term)
        }

        return result
    }

    private fun variableOrderForDeltaTerm(deltaStream: BaseRelationStream): List<Int> {
        val deltaVariables = deltaStream.variableIndexes()
        val remaining = (0 until levels).filterNot { deltaVariables.contains(it) }
        return deltaVariables + remaining
    }
}
