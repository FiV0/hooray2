package org.hooray.incremental

import org.hooray.algo.ResultTuple
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class IncrementalWcojJoinTest {
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
            eav = singletonPath(e, a, v, weight),
            aev = singletonPath(a, e, v, weight),
            ave = singletonPath(a, v, e, weight),
            vae = singletonPath(v, a, e, weight)
        )

    private fun triples(vararg triples: ZSetIndices): ZSetIndices =
        triples.fold(emptyZSetIndices()) { acc, next -> acc.add(next) }

    @Test
    fun `compiled pattern with fixed entity reads AEV`() {
        val pattern = CompiledTriplePattern("e1", "attr", null, -1, 0)
        val indices = triple("e1", "attr", "v1", IntegerWeight(3))
        val extender = pattern.extender(indices, listOf(0))

        val proposed = extender.propose(emptyList())

        assertEquals(IntegerWeight(3), proposed.weight("v1"))
    }

    @Test
    fun `two variable compiled pattern switches between AEV and AVE by term order`() {
        val pattern = CompiledTriplePattern(null, "attr", null, 0, 1)
        val indices = triple("e1", "attr", "v1")

        val entityFirst = pattern.extender(indices, listOf(0, 1)).propose(emptyList())
        val valueFirst = pattern.extender(indices, listOf(1, 0)).propose(emptyList())

        assertEquals(IntegerWeight.ONE, entityFirst.weight("e1"))
        assertEquals(IntegerWeight.ONE, valueFirst.weight("v1"))
    }

    @Test
    fun `telescoping triangle delta is emitted in canonical variable order`() {
        val patterns = listOf(
            CompiledTriplePattern(null, "r", null, 0, 1),
            CompiledTriplePattern(null, "s", null, 0, 2),
            CompiledTriplePattern(null, "t", null, 1, 2)
        )
        val engine = IncrementalWcojJoinEngine(patterns, 3)

        engine.eval(
            triples(
                triple(1, "r", 2),
                triple(1, "s", 3),
                triple(2, "t", 3)
            )
        )
        engine.commit()

        val delta = engine.eval(triple(2, "t", 4))

        assertEquals(IntegerWeight.ZERO, delta.weight(listOf(1, 2, 4) as ResultTuple))

        engine.commit()
        val secondDelta = engine.eval(triple(1, "s", 4))

        assertEquals(IntegerWeight.ONE, secondDelta.weight(listOf(1, 2, 4) as ResultTuple))
    }
}
