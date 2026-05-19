package org.hooray.incremental.stream

import org.hooray.incremental.CompiledTriplePattern
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class PatternSpecTest {
    @Test
    fun `incremental WCOJ spec stores existing compiled triple patterns`() {
        val patterns = listOf(
            CompiledTriplePattern(null, "r", null, 0, 1),
            CompiledTriplePattern(null, "s", null, 1, 2)
        )

        val joinSpec = IncrementalWcojJoinSpec(
            patterns = patterns,
            levels = 3,
            canonicalOrder = listOf(0, 1, 2)
        )
        val circuitSpec = CircuitSpec(input = InputHandle(), source = joinSpec)

        assertEquals(patterns, joinSpec.patterns)
        assertEquals(3, joinSpec.levels)
        assertEquals(listOf(0, 1, 2), joinSpec.canonicalOrder)
        assertEquals(joinSpec, circuitSpec.source)
    }
}
