package org.hooray.incremental.stream

import org.hooray.incremental.CompiledTriplePattern
import org.hooray.incremental.IndexedZSet
import org.hooray.incremental.IntegerWeight
import org.hooray.incremental.ZSet
import org.hooray.incremental.ZSetIndices
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class ProjectCircuitTest {
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
    fun `project transform reorders canonical WCOJ tuples`() {
        val circuit = Circuit(
            CircuitSpec(
                input = InputHandle(),
                source = IncrementalWcojJoinSpec(
                    patterns = listOf(CompiledTriplePattern(null, "r", null, 0, 1)),
                    levels = 2
                ),
                transforms = listOf(ProjectSpec(outputLevels = listOf(1, 0)))
            )
        )

        val result = circuit.step(triple(1, "r", 2))

        assertEquals(IntegerWeight.ONE, result.weight(listOf(2, 1)))
        assertEquals(IntegerWeight.ZERO, result.weight(listOf(1, 2)))
    }

    @Test
    fun `project transform supports one variable output`() {
        val circuit = Circuit(
            CircuitSpec(
                input = InputHandle(),
                source = IncrementalWcojJoinSpec(
                    patterns = listOf(CompiledTriplePattern(null, "r", null, 0, 1)),
                    levels = 2
                ),
                transforms = listOf(ProjectSpec(outputLevels = listOf(1)))
            )
        )

        val result = circuit.step(triple(1, "r", 2))

        assertEquals(IntegerWeight.ONE, result.weight(listOf(2)))
        assertEquals(IntegerWeight.ZERO, result.weight(listOf(1, 2)))
    }
}
