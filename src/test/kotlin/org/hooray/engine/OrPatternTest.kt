package org.hooray.engine

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class OrPatternTest {

    @Test
    fun `validator mode keeps rows completed by at least one branch`() {
        val branchA = listOf(
            Stage(
                introduces = emptyList(),
                participants = listOf(FilteringPattern { row ->
                    row[0] == "a" && (row[1] as Int) < 30
                }),
                targetVariables = listOf("?e", "?age"),
            ),
        )
        val branchB = listOf(
            Stage(
                introduces = emptyList(),
                participants = listOf(FilteringPattern { row ->
                    row[0] == "b" && (row[1] as Int) < 40
                }),
                targetVariables = listOf("?e", "?age"),
            ),
        )
        val pattern = OrPattern(
            variables = setOf("?e", "?age"),
            branches = listOf(branchA, branchB),
            proposerEligible = false,
        )
        val input = BindingSet(
            variables = listOf("?e", "?age"),
            rows = listOf(
                listOf("a", 35),
                listOf("b", 35),
            ),
        )

        val result = pattern.validate(input, listOf("?e", "?age"))

        assertFalse(pattern.proposerEligible)
        assertEquals(listOf(listOf("b", 35)), result.rows)
    }

    @Test
    fun `dedicated proposer mode unions complete branch rows`() {
        val branchA = listOf(
            Stage(
                introduces = listOf("?x", "?c"),
                participants = listOf(
                    InputPattern.relation(
                        variables = listOf("?x", "?c"),
                        rows = listOf(listOf("x1", "c1")),
                    ),
                ),
                targetVariables = listOf("?a", "?x", "?c"),
            ),
        )
        val branchB = listOf(
            Stage(
                introduces = listOf("?x", "?c"),
                participants = listOf(
                    InputPattern.relation(
                        variables = listOf("?x", "?c"),
                        rows = listOf(
                            listOf("x1", "c1"),
                            listOf("x2", "c2"),
                        ),
                    ),
                ),
                targetVariables = listOf("?a", "?x", "?c"),
            ),
        )
        val pattern = OrPattern(
            variables = setOf("?a", "?x", "?c"),
            branches = listOf(branchA, branchB),
            proposerEligible = true,
        )
        val input = BindingSet(
            variables = listOf("?a"),
            rows = listOf(listOf("seed")),
        )

        val result = pattern.propose(
            input = input,
            introduces = listOf("?x", "?c"),
            targetVariables = listOf("?a", "?x", "?c"),
        )

        assertTrue(pattern.proposerEligible)
        assertEquals(
            listOf(
                listOf("seed", "x1", "c1"),
                listOf("seed", "x2", "c2"),
            ),
            result.rows,
        )
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
