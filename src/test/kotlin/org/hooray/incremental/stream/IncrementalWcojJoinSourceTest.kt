package org.hooray.incremental.stream

import org.hooray.algo.ResultTuple
import org.hooray.incremental.CompiledTriplePattern
import org.hooray.incremental.IncrementalJoinOperator
import org.hooray.incremental.IndexedZSet
import org.hooray.incremental.IntegerWeight
import org.hooray.incremental.ZSet
import org.hooray.incremental.ZSetIndices
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class IncrementalWcojJoinSourceTest {

    // --- fixture helpers (mirrors IncrementalWcojJoinTest) ---

    private fun singletonPath(
        first: Any,
        second: Any,
        leaf: Any,
        weight: IntegerWeight = IntegerWeight.ONE
    ): IndexedZSet<Any, IntegerWeight> =
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

    private fun emptyIndices(): ZSetIndices = ZSetIndices(
        IndexedZSet.empty(IntegerWeight.ZERO, IntegerWeight.ONE),
        IndexedZSet.empty(IntegerWeight.ZERO, IntegerWeight.ONE)
    )

    private fun addIndexedZSets(
        left: IndexedZSet<Any, IntegerWeight>,
        right: IndexedZSet<Any, IntegerWeight>
    ): IndexedZSet<Any, IntegerWeight> = when {
        left.isEmpty() -> right
        right.isEmpty() -> left
        else -> left.add(right)
    }

    private fun addIndices(l: ZSetIndices, r: ZSetIndices): ZSetIndices =
        ZSetIndices(addIndexedZSets(l.aev, r.aev), addIndexedZSets(l.ave, r.ave))

    private fun triples(vararg ts: ZSetIndices): ZSetIndices =
        ts.fold(emptyIndices()) { acc, next -> addIndices(acc, next) }

    private fun trianglePatterns(): List<CompiledTriplePattern> = listOf(
        CompiledTriplePattern(null, "r", null, 0, 1),
        CompiledTriplePattern(null, "s", null, 0, 2),
        CompiledTriplePattern(null, "t", null, 1, 2)
    )

    // --- tests ---

    @Test
    fun `triangle delta emitted in canonical variable order — parity with engine`() {
        // Build two independent pattern lists (state is per-instance).
        val streamSource = IncrementalWcojJoinSource(trianglePatterns(), levels = 3)
        val legacy = IncrementalJoinOperator(trianglePatterns(), levels = 3)

        val tick1 = triples(
            triple(1, "r", 2),
            triple(1, "s", 3),
            triple(2, "t", 3)
        )
        val streamOut1 = streamSource.eval(tick1)
        val legacyOut1 = legacy.eval(tick1)
        streamSource.commit()
        legacy.commit()
        assertEquals(legacyOut1, streamOut1, "tick 1")

        val tick2 = triple(2, "t", 4)
        val streamOut2 = streamSource.eval(tick2)
        val legacyOut2 = legacy.eval(tick2)
        streamSource.commit()
        legacy.commit()
        assertEquals(legacyOut2, streamOut2, "tick 2")

        val tick3 = triple(1, "s", 4)
        val streamOut3 = streamSource.eval(tick3)
        val legacyOut3 = legacy.eval(tick3)
        streamSource.commit()
        legacy.commit()
        assertEquals(legacyOut3, streamOut3, "tick 3")
    }

    @Test
    fun `single-pattern fixed-entity query returns the value bindings`() {
        // Pattern: [?e :attr ?v] with fixed entity in the value position.
        // Concretely: CompiledTriplePattern("e1", "attr", null, -1, 0) — entity is constant.
        val pattern = CompiledTriplePattern("e1", "attr", null, -1, 0)
        val source = IncrementalWcojJoinSource(listOf(pattern), levels = 1)
        val legacy = IncrementalJoinOperator(listOf(CompiledTriplePattern("e1", "attr", null, -1, 0)), levels = 1)

        val tick = triple("e1", "attr", "v1", IntegerWeight(3))
        val streamOut = source.eval(tick)
        val legacyOut = legacy.eval(tick)
        source.commit()
        legacy.commit()
        assertEquals(legacyOut, streamOut)
        assertEquals(IntegerWeight(3), streamOut.weight(listOf<Any>("v1") as ResultTuple))
    }

    @Test
    fun `empty delta produces empty output`() {
        val source = IncrementalWcojJoinSource(trianglePatterns(), levels = 3)
        val out = source.eval(emptyIndices())
        source.commit()
        assert(out.isEmpty())
    }
}
