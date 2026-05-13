package org.hooray.incremental.stream

import org.hooray.incremental.CompiledTriplePattern
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows

class TypeCheckTest {

    private fun trianglePatterns(): List<CompiledTriplePattern> = listOf(
        CompiledTriplePattern(null, "r", null, 0, 1),
        CompiledTriplePattern(null, "s", null, 0, 2),
        CompiledTriplePattern(null, "t", null, 1, 2)
    )

    @Test
    fun `happy path triangle spec passes typeCheck`() {
        val spec = IncrementalWcojJoinSpec(trianglePatterns(), levels = 3, canonicalOrder = listOf(0, 1, 2))
        assertDoesNotThrow { typeCheck(spec) }
    }

    @Test
    fun `canonicalOrder that is not a permutation is rejected`() {
        val spec = IncrementalWcojJoinSpec(
            patterns = trianglePatterns(),
            levels = 3,
            canonicalOrder = listOf(0, 0, 2) // duplicate
        )
        val ex = assertThrows<WcojTypeError> { typeCheck(spec) }
        assert(ex.message!!.contains("permutation")) { "expected message about permutation, got: ${ex.message}" }
    }

    @Test
    fun `pattern variable index out of range is rejected`() {
        // levels=2 but a pattern references variable index 5
        val spec = IncrementalWcojJoinSpec(
            patterns = listOf(
                CompiledTriplePattern(null, "r", null, 0, 1),
                CompiledTriplePattern(null, "s", null, 0, 5)
            ),
            levels = 2,
            canonicalOrder = listOf(0, 1)
        )
        val ex = assertThrows<WcojTypeError> { typeCheck(spec) }
        // The message should mention the pattern index and the offending var.
        assert(ex.message!!.contains("pattern 1")) { ex.message ?: "" }
        assert(ex.message!!.contains("5")) { ex.message ?: "" }
    }

    @Test
    fun `unbound variable is rejected`() {
        // levels=3 but only variables 0, 1 appear in any pattern
        val spec = IncrementalWcojJoinSpec(
            patterns = listOf(CompiledTriplePattern(null, "r", null, 0, 1)),
            levels = 3,
            canonicalOrder = listOf(0, 1, 2)
        )
        val ex = assertThrows<WcojTypeError> { typeCheck(spec) }
        assert(ex.message!!.contains("not bound")) { ex.message ?: "" }
        assert(ex.message!!.contains("2")) { ex.message ?: "" }
    }

    @Test
    fun `buildWcojSource runs typeCheck`() {
        val badSpec = IncrementalWcojJoinSpec(
            patterns = listOf(CompiledTriplePattern(null, "r", null, 0, 1)),
            levels = 3,
            canonicalOrder = listOf(0, 1, 2)
        )
        assertThrows<WcojTypeError> { buildWcojSource(badSpec) }
    }
}
