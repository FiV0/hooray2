package org.hooray.incremental.stream

import org.hooray.algo.ResultTuple
import org.hooray.incremental.CompiledTriplePattern
import org.hooray.incremental.IndexedZSet
import org.hooray.incremental.IntegerWeight
import org.hooray.incremental.ZSet
import org.hooray.incremental.ZSetIndices
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class IncrementalWcojJoinSpecTest {

    private fun trianglePatterns(): List<CompiledTriplePattern> = listOf(
        CompiledTriplePattern(null, "r", null, 0, 1),
        CompiledTriplePattern(null, "s", null, 0, 2),
        CompiledTriplePattern(null, "t", null, 1, 2)
    )

    private fun singletonPath(
        first: Any, second: Any, leaf: Any
    ): IndexedZSet<Any, IntegerWeight> = IndexedZSet.singleton(
        first,
        IndexedZSet.singleton(second, ZSet.singleton(leaf, IntegerWeight.ONE),
            IntegerWeight.ZERO, IntegerWeight.ONE),
        IntegerWeight.ZERO, IntegerWeight.ONE
    )

    private fun triple(e: Any, a: Any, v: Any): ZSetIndices =
        ZSetIndices(singletonPath(a, e, v), singletonPath(a, v, e))

    @Test
    fun `spec holds the inputs verbatim`() {
        val patterns = trianglePatterns()
        val spec = IncrementalWcojJoinSpec(patterns, levels = 3, canonicalOrder = listOf(0, 1, 2))
        assertEquals(patterns, spec.patterns)
        assertEquals(3, spec.levels)
        assertEquals(listOf(0, 1, 2), spec.canonicalOrder)
    }

    @Test
    fun `spec rejects empty patterns`() {
        assertThrows<IllegalArgumentException> {
            IncrementalWcojJoinSpec(emptyList(), levels = 1, canonicalOrder = listOf(0))
        }
    }

    @Test
    fun `spec rejects canonicalOrder whose size does not match levels`() {
        assertThrows<IllegalArgumentException> {
            IncrementalWcojJoinSpec(trianglePatterns(), levels = 3, canonicalOrder = listOf(0, 1))
        }
    }

    @Test
    fun `buildWcojSource returns a runnable source matching the spec`() {
        val spec = IncrementalWcojJoinSpec(trianglePatterns(), levels = 3, canonicalOrder = listOf(0, 1, 2))
        val source = buildWcojSource(spec)
        // smoke run: load a single triangle, expect the canonical tuple back
        val tick = ZSetIndices(
            aev = singletonPath("r", 1, 2).add(singletonPath("s", 1, 3)).add(singletonPath("t", 2, 3)),
            ave = singletonPath("r", 2, 1).add(singletonPath("s", 3, 1)).add(singletonPath("t", 3, 2))
        )
        val out = source.eval(tick)
        source.commit()
        // The triangle (1, 2, 3) was created entirely in this tick — it's emitted (with potential
        // multiplicities from telescoping, like the existing engine).
        assert(out.weight(listOf<Any>(1, 2, 3) as ResultTuple).value > 0) {
            "expected the triangle (1,2,3) in output, got $out"
        }
    }
}
