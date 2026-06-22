package org.hooray.engine

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class TriplePatternTest {

    @Test
    fun `proposes one variable from constants`() {
        val pattern = triplePattern(
            TestTriple("a", "age", 35),
            TestTriple("a", "name", "A"),
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
        val pattern = triplePattern(
            TestTriple("a", "age", 35),
            TestTriple("b", "age", 40),
            TestTriple("a", "name", "A"),
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
        val pattern = triplePattern(
            TestTriple("a", "age", 35),
            TestTriple("b", "age", 40),
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
        val pattern = triplePattern(
            TestTriple("a", "age", 35),
            TestTriple("b", "name", "B"),
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

    private fun triplePattern(
        vararg triples: TestTriple,
        entity: PatternValue,
        attribute: PatternValue,
        value: PatternValue,
    ): TriplePattern {
        val indexes = indexes(*triples)
        return TriplePattern(
            eav = indexes.eav,
            aev = indexes.aev,
            ave = indexes.ave,
            entity = entity,
            attribute = attribute,
            value = value,
        )
    }

    private fun indexes(vararg triples: TestTriple): TestIndexes {
        val eav = linkedMapOf<Any, MutableMap<Any, MutableSet<Any>>>()
        val aev = linkedMapOf<Any, MutableMap<Any, MutableSet<Any>>>()
        val ave = linkedMapOf<Any, MutableMap<Any, MutableSet<Any>>>()

        for ((entity, attribute, value) in triples) {
            eav.getOrPut(entity) { linkedMapOf() }
                .getOrPut(attribute) { linkedSetOf() }
                .add(value)
            aev.getOrPut(attribute) { linkedMapOf() }
                .getOrPut(entity) { linkedSetOf() }
                .add(value)
            ave.getOrPut(attribute) { linkedMapOf() }
                .getOrPut(value) { linkedSetOf() }
                .add(entity)
        }

        return TestIndexes(
            eav = eav.freeze(),
            aev = aev.freeze(),
            ave = ave.freeze(),
        )
    }

    private fun Map<Any, MutableMap<Any, MutableSet<Any>>>.freeze(): Map<Any, Map<Any, Set<Any>>> {
        return mapValues { (_, inner) ->
            inner.mapValues { (_, values) -> values.toSet() }
        }
    }

    private data class TestTriple(
        val entity: Any,
        val attribute: Any,
        val value: Any,
    )

    private data class TestIndexes(
        val eav: Map<Any, Map<Any, Set<Any>>>,
        val aev: Map<Any, Map<Any, Set<Any>>>,
        val ave: Map<Any, Map<Any, Set<Any>>>,
    )
}
