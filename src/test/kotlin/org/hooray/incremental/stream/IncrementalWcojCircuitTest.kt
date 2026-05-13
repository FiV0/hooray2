package org.hooray.incremental.stream

import org.hooray.incremental.CompiledTriplePattern
import org.hooray.incremental.IncrementalJoinOperator
import org.hooray.incremental.IndexedZSet
import org.hooray.incremental.IntegerWeight
import org.hooray.incremental.ZSet
import org.hooray.incremental.ZSetIndices
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
}
