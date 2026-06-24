package org.hooray.engine

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

class PredicateFunctionPatternTest {

    @Test
    fun `predicate validates bound argument rows`() {
        val pattern = PredicatePattern(
            idx = 0,
            arguments = listOf(PatternValue.Variable("?age"), PatternValue.Constant(40)),
            predicate = { args -> (args[0] as Int) < (args[1] as Int) },
        )
        val input = BindingSet(
            variables = listOf("?age"),
            rows = listOf(
                listOf(35),
                listOf(45),
            ),
        )

        val result = pattern.validate(input)

        assertEquals(listOf(listOf(35)), result.rows)
    }

    @Test
    fun `predicate validation requires variable arguments to be bound`() {
        val pattern = PredicatePattern(
            idx = 0,
            arguments = listOf(PatternValue.Variable("?age")),
            predicate = { true },
        )
        val input = BindingSet(
            variables = listOf("?e"),
            rows = listOf(listOf("a")),
        )

        val error = assertThrows(IllegalArgumentException::class.java) {
            pattern.validate(input)
        }

        assertEquals("Predicate variables must be bound before validation", error.message)
    }

    @Test
    fun `function proposes return variable from bound inputs`() {
        val pattern = FunctionPattern(
            idx = 0,
            arguments = listOf(PatternValue.Variable("?age")),
            returnVariable = "?next",
            function = { args -> (args[0] as Int) + 1 },
        )
        val input = BindingSet(
            variables = listOf("?age"),
            rows = listOf(listOf(34)),
        )

        assertEquals(
            listOf(Proposal(0, 1)),
            pattern.count(input, listOf("?next"), initialProposals(input)),
        )

        val result = pattern.propose(
            input = input,
            introduces = listOf("?next"),
            targetVariables = listOf("?age", "?next"),
        )

        assertEquals(listOf(listOf(34, 35)), result.rows)
    }

    @Test
    fun `function validates already bound return variable`() {
        val pattern = FunctionPattern(
            idx = 0,
            arguments = listOf(PatternValue.Variable("?age")),
            returnVariable = "?next",
            function = { args -> (args[0] as Int) + 1 },
        )
        val input = BindingSet(
            variables = listOf("?age", "?next"),
            rows = listOf(
                listOf(34, 35),
                listOf(34, 36),
            ),
        )

        val result = pattern.validate(input)

        assertEquals(listOf(listOf(34, 35)), result.rows)
    }

    private fun initialProposals(input: BindingSet): List<Proposal> =
        List(input.rowCount) { Proposal(NO_PROPOSAL, Int.MAX_VALUE) }
}
