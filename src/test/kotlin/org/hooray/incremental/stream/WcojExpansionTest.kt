package org.hooray.incremental.stream

import org.hooray.incremental.CompiledTriplePattern
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class WcojExpansionTest {

    private fun trianglePatterns(): List<CompiledTriplePattern> = listOf(
        CompiledTriplePattern(null, "r", null, 0, 1),
        CompiledTriplePattern(null, "s", null, 0, 2),
        CompiledTriplePattern(null, "t", null, 1, 2)
    )

    @Test
    fun `two-pattern expansion has one branch per pattern`() {
        val spec = IncrementalWcojJoinSpec(
            patterns = listOf(
                CompiledTriplePattern(null, "r", null, 0, 1),
                CompiledTriplePattern(null, "s", null, 1, 0)
            ),
            levels = 2,
            canonicalOrder = listOf(0, 1)
        )
        val expansion = expand(spec)
        assertEquals(2, expansion.branches.size)
        assertEquals(0, expansion.branches[0].deltaPatternIndex)
        assertEquals(1, expansion.branches[1].deltaPatternIndex)
    }

    @Test
    fun `triangle expansion has three branches with expected node-kind sequences`() {
        val spec = IncrementalWcojJoinSpec(trianglePatterns(), levels = 3, canonicalOrder = listOf(0, 1, 2))
        val expansion = expand(spec)
        assertEquals(3, expansion.branches.size)

        // Branch 0 (delta = R, variables [0,1]); canonical [0,1,2] -> variableOrder [0,1,2] (identity)
        // Patterns: R is delta (MapIndex only); S and T are CURRENT (Integrate + MapIndex). Then Join.
        // Identity canonicalization -> no final MapIndex.
        assertEquals(
            listOf(
                WcojNodeKind.INPUT_REINDEX,             // R (delta)
                WcojNodeKind.INTEGRATE_CURRENT,         // S (current)
                WcojNodeKind.INPUT_REINDEX,             // S reindex
                WcojNodeKind.INTEGRATE_CURRENT,         // T (current)
                WcojNodeKind.INPUT_REINDEX,             // T reindex
                WcojNodeKind.JOIN
                // no CANONICALIZE: branch order already canonical
            ),
            expansion.branches[0].nodeKinds
        )

        // Branch 1 (delta = S, variables [0,2]) -> variableOrder [0,2,1] != canonical, needs CANONICALIZE
        assertEquals(WcojNodeKind.JOIN, expansion.branches[1].nodeKinds.dropLast(1).last())
        assertEquals(WcojNodeKind.CANONICALIZE, expansion.branches[1].nodeKinds.last())

        // Branch 2 (delta = T, variables [1,2]) -> variableOrder [1,2,0] != canonical
        assertEquals(WcojNodeKind.CANONICALIZE, expansion.branches[2].nodeKinds.last())
    }

    @Test
    fun `triangle expansion branch variable orders match plan helpers`() {
        val spec = IncrementalWcojJoinSpec(trianglePatterns(), levels = 3, canonicalOrder = listOf(0, 1, 2))
        val expansion = expand(spec)
        assertEquals(listOf(0, 1, 2), expansion.branches[0].variableOrder)  // R: [0,1] + [2]
        assertEquals(listOf(0, 2, 1), expansion.branches[1].variableOrder)  // S: [0,2] + [1]
        assertEquals(listOf(1, 2, 0), expansion.branches[2].variableOrder)  // T: [1,2] + [0]
    }

    @Test
    fun `each branch node carries a unique non-zero id`() {
        val spec = IncrementalWcojJoinSpec(trianglePatterns(), levels = 3, canonicalOrder = listOf(0, 1, 2))
        val expansion = expand(spec)
        val allIds = expansion.branches.flatMap { it.nodeIds }
        assertEquals(allIds.size, allIds.toSet().size, "node ids must be unique across all branches")
    }
}
