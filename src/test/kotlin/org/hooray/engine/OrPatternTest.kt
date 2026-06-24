package org.hooray.engine

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

class OrPatternTest {

    @Test
    fun `validator mode keeps rows completed by at least one branch`() {
        val branchA = listOf(
            Stage(
                introduces = emptyList(),
                participants = listOf(FilteringPattern(idx = 0) { row ->
                    row[0] == "a" && (row[1] as Int) < 30
                }),
                targetVariables = listOf("?e", "?age"),
            ),
        )
        val branchB = listOf(
            Stage(
                introduces = emptyList(),
                participants = listOf(FilteringPattern(idx = 0) { row ->
                    row[0] == "b" && (row[1] as Int) < 40
                }),
                targetVariables = listOf("?e", "?age"),
            ),
        )
        val pattern = OrPattern(
            idx = 0,
            variables = setOf("?e", "?age"),
            proposalBranches = emptyList(),
            validationBranches = listOf(branchA, branchB),
            canPropose = false,
        )
        val input = BindingSet(
            variables = listOf("?e", "?age"),
            rows = listOf(
                listOf("a", 35),
                listOf("b", 35),
            ),
        )

        val result = pattern.validate(input)

        assertEquals(listOf(listOf("b", 35)), result.rows)
    }

    @Test
    fun `count only claims rows when no ordinary proposal exists and all variables are covered`() {
        val pattern = OrPattern(
            idx = 2,
            variables = setOf("?e", "?age"),
            proposalBranches = listOf(emptyList()),
            validationBranches = listOf(emptyList()),
            canPropose = true,
        )
        val input = BindingSet(
            variables = listOf("?e"),
            rows = listOf(listOf("a"), listOf("b")),
        )
        val proposals = listOf(
            Proposal(NO_PROPOSAL, Int.MAX_VALUE),
            Proposal(1, 3),
        )

        assertEquals(
            listOf(
                Proposal(2, Int.MAX_VALUE),
                Proposal(1, 3),
            ),
            pattern.count(input, listOf("?age"), proposals),
        )
    }

    @Test
    fun `dedicated proposer mode unions complete branch rows`() {
        val branchA = listOf(
            Stage(
                introduces = listOf("?x", "?c"),
                participants = listOf(
                    InputPattern.relation(
                        idx = 0,
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
                        idx = 0,
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
            idx = 0,
            variables = setOf("?a", "?x", "?c"),
            proposalBranches = listOf(branchA, branchB),
            validationBranches = listOf(branchA, branchB),
            canPropose = true,
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

        assertEquals(
            listOf(
                listOf("seed", "x1", "c1"),
                listOf("seed", "x2", "c2"),
            ),
            result.rows,
        )
    }

    @Test
    fun `proposal requires all or variables to be covered`() {
        val pattern = OrPattern(
            idx = 0,
            variables = setOf("?a", "?x", "?c"),
            proposalBranches = listOf(emptyList()),
            validationBranches = listOf(emptyList()),
            canPropose = true,
        )
        val input = BindingSet(
            variables = listOf("?a"),
            rows = listOf(listOf("seed")),
        )

        val error = assertThrows(IllegalArgumentException::class.java) {
            pattern.propose(
                input = input,
                introduces = listOf("?x"),
                targetVariables = listOf("?a", "?x"),
            )
        }

        assertEquals(
            "OR pattern can only propose when input variables and introduced variables cover all OR variables",
            error.message,
        )
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
