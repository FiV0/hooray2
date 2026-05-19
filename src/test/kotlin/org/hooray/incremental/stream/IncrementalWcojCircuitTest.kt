package org.hooray.incremental.stream

import org.hooray.incremental.CompiledTriplePattern
import org.hooray.incremental.IncrementalJoinOperator
import org.hooray.incremental.IndexedZSet
import org.hooray.incremental.IntegerWeight
import org.hooray.incremental.ZSet
import org.hooray.incremental.ZSetIndices
import org.hooray.algo.ResultTuple
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class IncrementalWcojCircuitTest {
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

    private fun emptyIndices(): ZSetIndices =
        ZSetIndices(
            IndexedZSet.empty(IntegerWeight.ZERO, IntegerWeight.ONE),
            IndexedZSet.empty(IntegerWeight.ZERO, IntegerWeight.ONE)
        )

    private fun addIndexedZSets(
        left: IndexedZSet<Any, IntegerWeight>,
        right: IndexedZSet<Any, IntegerWeight>
    ): IndexedZSet<Any, IntegerWeight> =
        when {
            left.isEmpty() -> right
            right.isEmpty() -> left
            else -> left.add(right)
        }

    private fun addIndices(left: ZSetIndices, right: ZSetIndices): ZSetIndices =
        ZSetIndices(
            addIndexedZSets(left.aev, right.aev),
            addIndexedZSets(left.ave, right.ave)
        )

    private fun triples(vararg triples: ZSetIndices): ZSetIndices =
        triples.fold(emptyIndices()) { acc, next -> addIndices(acc, next) }

    @Test
    fun `circuit source matches existing incremental join operator`() {
        val oldOperator = IncrementalJoinOperator(
            listOf(CompiledTriplePattern(null, "r", null, 0, 1)),
            levels = 2
        )
        val circuit = Circuit(
            CircuitSpec(
                input = InputHandle(),
                source = IncrementalWcojJoinSpec(
                    patterns = listOf(CompiledTriplePattern(null, "r", null, 0, 1)),
                    levels = 2
                )
            )
        )
        val delta = triple(1, "r", 2)

        val expected = oldOperator.eval(delta)
        oldOperator.commit()

        assertEquals(expected, circuit.step(delta))
    }

    @Test
    fun `circuit consumes pending input once`() {
        val circuit = Circuit(
            CircuitSpec(
                input = InputHandle(),
                source = IncrementalWcojJoinSpec(
                    patterns = listOf(CompiledTriplePattern(null, "r", null, 0, 1)),
                    levels = 2
                )
            )
        )

        val first = circuit.step(triple(1, "r", 2))
        val second = circuit.step()

        assertEquals(IntegerWeight.ONE, first.weight(listOf(1, 2)))
        assertEquals(ZSet.empty<Any>(), second)
    }

    @Test
    fun `circuit handles mixed AEV and AVE branch order in triangle query`() {
        val circuit = Circuit(
            CircuitSpec(
                input = InputHandle(),
                source = IncrementalWcojJoinSpec(
                    patterns = listOf(
                        CompiledTriplePattern(null, "r", null, 0, 1),
                        CompiledTriplePattern(null, "s", null, 0, 2),
                        CompiledTriplePattern(null, "t", null, 1, 2)
                    ),
                    levels = 3
                )
            )
        )

        circuit.step(
            triples(
                triple(1, "r", 2),
                triple(1, "s", 3),
                triple(2, "t", 3)
            )
        )

        val deltaWithoutTriangle = circuit.step(triple(2, "t", 4))

        assertEquals(IntegerWeight.ZERO, deltaWithoutTriangle.weight(listOf(1, 2, 4) as ResultTuple))

        val triangleDelta = circuit.step(triple(1, "s", 4))

        assertEquals(IntegerWeight.ONE, triangleDelta.weight(listOf(1, 2, 4) as ResultTuple))
    }
}
