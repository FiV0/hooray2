package org.hooray.incremental.stream

import org.hooray.incremental.ArrangementState
import org.hooray.incremental.CompiledTriplePattern
import org.hooray.incremental.IndexedZSet
import org.hooray.incremental.IntegerWeight
import org.hooray.incremental.ZSet
import org.hooray.incremental.ZSetIndices
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class BaseRelationStreamTest {
    private fun singletonPath(first: Any, second: Any, leaf: Any, weight: IntegerWeight = IntegerWeight.ONE): IndexedZSet<Any, IntegerWeight> =
        IndexedZSet.singleton(
            first,
            IndexedZSet.singleton(
                second,
                ZSet.singleton(leaf, weight),
                IntegerWeight.ZERO,
                IntegerWeight.ONE
            ),
            IntegerWeight.ZERO,
            IntegerWeight.ONE
        )

    private fun triple(e: Any, a: Any, v: Any, weight: IntegerWeight = IntegerWeight.ONE): ZSetIndices =
        ZSetIndices(
            aev = singletonPath(a, e, v, weight),
            ave = singletonPath(a, v, e, weight)
        )

    @Test
    fun `base relation stream exposes delta and delayed current views`() {
        val stream = BaseRelationStream(CompiledTriplePattern(null, "r", null, 0, 1))
        stream.receiveDelta(triple("old-e", "r", "old-v"))
        stream.commit()

        stream.receiveDelta(triple("new-e", "r", "new-v"))

        val delta = stream.view(listOf(0, 1), ArrangementState.DELTA).toExtender()
        val current = stream.view(listOf(0, 1), ArrangementState.CURRENT).toExtender()

        assertEquals(IntegerWeight.ONE, delta.propose(emptyList()).weight("new-e"))
        assertEquals(IntegerWeight.ZERO, delta.propose(emptyList()).weight("old-e"))
        assertEquals(IntegerWeight.ONE, current.propose(emptyList()).weight("old-e"))
        assertEquals(IntegerWeight.ZERO, current.propose(emptyList()).weight("new-e"))
    }

    @Test
    fun `base relation stream switches arrangement by variable order`() {
        val stream = BaseRelationStream(CompiledTriplePattern(null, "r", null, 0, 1))
        stream.receiveDelta(triple("e1", "r", "v1"))

        val entityFirst = stream.view(listOf(0, 1), ArrangementState.DELTA).toExtender()
        val valueFirst = stream.view(listOf(1, 0), ArrangementState.DELTA).toExtender()

        assertEquals(IntegerWeight.ONE, entityFirst.propose(emptyList()).weight("e1"))
        assertEquals(IntegerWeight.ONE, valueFirst.propose(emptyList()).weight("v1"))
    }
}
