package org.hooray.engine

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class InputPatternTest {

    @Test
    fun `scalar input proposes one variable`() {
        val pattern = InputPattern.scalar("?e", listOf("a", "b"))
        val input = BindingSet(variables = emptyList(), rows = listOf(emptyList()))

        assertEquals(listOf(2), pattern.count(input, listOf("?e")))

        val result = pattern.propose(
            input = input,
            introduces = listOf("?e"),
            targetVariables = listOf("?e"),
        )

        assertEquals(listOf(listOf("a"), listOf("b")), result.rows)
    }

    @Test
    fun `relation input proposes correlated tuples`() {
        val pattern = InputPattern.relation(
            variables = listOf("?e", "?age"),
            rows = listOf(
                listOf("a", 35),
                listOf("b", 40),
            ),
        )
        val input = BindingSet(variables = emptyList(), rows = listOf(emptyList()))

        val result = pattern.propose(
            input = input,
            introduces = listOf("?e", "?age"),
            targetVariables = listOf("?e", "?age"),
        )

        assertEquals(
            listOf(
                listOf("a", 35),
                listOf("b", 40),
            ),
            result.rows,
        )
    }

    @Test
    fun `input proposal respects already bound variables`() {
        val pattern = InputPattern.relation(
            variables = listOf("?e", "?age"),
            rows = listOf(
                listOf("a", 35),
                listOf("b", 40),
            ),
        )
        val input = BindingSet(
            variables = listOf("?e"),
            rows = listOf(
                listOf("a"),
                listOf("c"),
            ),
        )

        val result = pattern.propose(
            input = input,
            introduces = listOf("?age"),
            targetVariables = listOf("?e", "?age"),
        )

        assertEquals(listOf(listOf("a", 35)), result.rows)
    }

    @Test
    fun `input validation filters existing rows`() {
        val pattern = InputPattern.relation(
            variables = listOf("?e", "?age"),
            rows = listOf(
                listOf("a", 35),
                listOf("b", 40),
            ),
        )
        val input = BindingSet(
            variables = listOf("?e", "?age"),
            rows = listOf(
                listOf("a", 35),
                listOf("a", 40),
                listOf("b", 40),
            ),
        )

        val result = pattern.validate(input)

        assertEquals(
            listOf(
                listOf("a", 35),
                listOf("b", 40),
            ),
            result.rows,
        )
    }
}
