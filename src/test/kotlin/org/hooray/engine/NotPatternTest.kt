package org.hooray.engine

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class NotPatternTest {

    @Test
    fun `keeps seeded rows with no branch completion`() {
        val branch = listOf(
            Stage(
                introduces = emptyList(),
                participants = listOf(FilteringPattern { row -> row[0] == "a" }),
                targetVariables = listOf("?e"),
            ),
        )
        val pattern = NotPattern(
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

        val result = pattern.validate(input, listOf("?e"))

        assertEquals(listOf(listOf("b")), result.rows)
    }

    private class FilteringPattern(
        private val keep: (BindingRow) -> Boolean,
    ) : ExecPattern {
        override val variables: Set<Any> = emptySet()
        override val proposerEligible: Boolean = false

        override fun validate(input: BindingSet, targetVariables: List<Any>): BindingSet {
            return BindingSet(input.variables, input.rows.filter(keep))
        }
    }
}
