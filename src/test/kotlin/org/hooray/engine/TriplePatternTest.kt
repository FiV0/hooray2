package org.hooray.engine

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class TriplePatternTest {

    @Test
    fun `proposes one variable from constants`() {
        val pattern = TriplePattern(
            index = TripleIndex.of(
                Triple("a", "age", 35),
                Triple("a", "name", "A"),
            ),
            entity = PatternValue.Constant("a"),
            attribute = PatternValue.Constant("age"),
            value = PatternValue.Variable("?age"),
        )
        val input = BindingSet(variables = emptyList(), rows = listOf(emptyList()))

        assertEquals(listOf(1), pattern.count(input, listOf("?age")))

        val result = pattern.propose(
            input = input,
            introduces = listOf("?age"),
            targetVariables = listOf("?age"),
        )

        assertEquals(listOf("?age"), result.variables)
        assertEquals(listOf(listOf(35)), result.rows)
    }

    @Test
    fun `proposes multiple variables from one triple pattern`() {
        val pattern = TriplePattern(
            index = TripleIndex.of(
                Triple("a", "age", 35),
                Triple("b", "age", 40),
                Triple("a", "name", "A"),
            ),
            entity = PatternValue.Variable("?e"),
            attribute = PatternValue.Constant("age"),
            value = PatternValue.Variable("?age"),
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
    fun `fully validates bound rows`() {
        val pattern = TriplePattern(
            index = TripleIndex.of(
                Triple("a", "age", 35),
                Triple("b", "age", 40),
            ),
            entity = PatternValue.Variable("?e"),
            attribute = PatternValue.Constant("age"),
            value = PatternValue.Variable("?age"),
        )
        val input = BindingSet(
            variables = listOf("?e", "?age"),
            rows = listOf(
                listOf("a", 35),
                listOf("a", 40),
                listOf("b", 40),
            ),
        )

        val result = pattern.validate(input, listOf("?e", "?age"))

        assertEquals(
            listOf(
                listOf("a", 35),
                listOf("b", 40),
            ),
            result.rows,
        )
    }

    @Test
    fun `partially validates rows with an existential completion`() {
        val pattern = TriplePattern(
            index = TripleIndex.of(
                Triple("a", "age", 35),
                Triple("b", "name", "B"),
            ),
            entity = PatternValue.Variable("?e"),
            attribute = PatternValue.Constant("age"),
            value = PatternValue.Variable("?age"),
        )
        val input = BindingSet(
            variables = listOf("?e"),
            rows = listOf(
                listOf("a"),
                listOf("b"),
                listOf("c"),
            ),
        )

        val result = pattern.validate(input, listOf("?e"))

        assertEquals(listOf(listOf("a")), result.rows)
    }
}
