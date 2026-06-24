package org.hooray.engine

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class NotPatternTest {

    @Test
    fun `keeps seeded rows with no branch completion`() {
        val branch = listOf(
            Stage(
                introduces = emptyList(),
                participants = listOf(FilteringPattern(idx = 0) { row -> row[0] == "a" }),
                targetVariables = listOf("?e"),
            ),
        )
        val pattern = NotPattern(
            idx = 0,
            variables = setOf("?e"),
            branch = branch,
        )
        val input = BindingSet(
            variables = listOf("?e"),
            rows = listOf(
                listOf("a"),
                listOf("b"),
            ),
        )

        val result = pattern.validate(input)

        assertEquals(listOf(listOf("b")), result.rows)
    }

    private class FilteringPattern(
        override val idx: Int,
        private val keep: (BindingRow) -> Boolean,
    ) : ExecPattern {
        override val variables: Set<Any> = emptySet()

        override fun validate(input: BindingSet): BindingSet {
            return BindingSet(input.variables, input.rows.filter(keep))
        }
    }
}
