package org.hooray.engine

import clojure.lang.Symbol
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class TriplePatternTest {
    private val e = Symbol.intern("?e")
    private val age = Symbol.intern("?age")
    private val seed = Symbol.intern("?seed")

    @Test
    fun `grounds either triple variable and proposes both together`() {
        val pattern = triplePattern(
            TestTriple("a", "age", 35),
            TestTriple("b", "age", 40),
            entity = PatternValue.Variable(e),
            attribute = "age",
            value = PatternValue.Variable(age),
        )
        val input = BindingSet(listOf(seed), listOf(listOf(1), listOf(2)))

        assertEquals(
            listOf(e, age),
            pattern.groundable(emptySet()),
        )
        assertEquals(
            listOf(Proposal(0, 2), Proposal(0, 2)),
            pattern.count(
                input,
                listOf(e, age),
                listOf(Proposal(NO_PROPOSER, Int.MAX_VALUE), Proposal(NO_PROPOSER, Int.MAX_VALUE)),
            ),
        )
        assertEquals(
            BindingSet(
                listOf(seed, e, age),
                listOf(listOf(1, "a", 35), listOf(2, "a", 35), listOf(1, "b", 40), listOf(2, "b", 40)),
            ),
            pattern.propose(input, listOf(e, age), listOf(seed, e, age)),
        )
    }

    @Test
    fun `uses entity and value bound shapes for proposal`() {
        val byEntity = triplePattern(
            TestTriple("a", "age", 35),
            TestTriple("b", "age", 40),
            entity = PatternValue.Variable(e),
            attribute = "age",
            value = PatternValue.Variable(age),
        )

        assertEquals(
            listOf(listOf("b", 40), listOf("a", 35)),
            byEntity.propose(
                BindingSet(listOf(e), listOf(listOf("b"), listOf("a"))),
                listOf(age),
                listOf(e, age),
            ).rows,
        )
        assertEquals(
            listOf(listOf(40, "b"), listOf(35, "a")),
            byEntity.propose(
                BindingSet(listOf(age), listOf(listOf(40), listOf(35))),
                listOf(e),
                listOf(age, e),
            ).rows,
        )
    }

    @Test
    fun `proposes one variable when the other variable is unbound`() {
        val pattern = triplePattern(
            TestTriple("a", "age", 35),
            TestTriple("a", "age", 40),
            TestTriple("b", "age", 40),
            entity = PatternValue.Variable(e),
            attribute = "age",
            value = PatternValue.Variable(age),
        )

        assertEquals(
            BindingSet(
                listOf(seed, e),
                listOf(listOf(1, "a"), listOf(2, "a"), listOf(1, "b"), listOf(2, "b")),
            ),
            pattern.propose(
                BindingSet(listOf(seed), listOf(listOf(1), listOf(2))),
                listOf(e),
                listOf(seed, e),
            ),
        )
        assertEquals(
            BindingSet(
                listOf(seed, age),
                listOf(listOf(1, 35), listOf(2, 35), listOf(1, 40), listOf(2, 40)),
            ),
            pattern.propose(
                BindingSet(listOf(seed), listOf(listOf(1), listOf(2))),
                listOf(age),
                listOf(seed, age),
            ),
        )
    }

    @Test
    fun `performs full and partial existential validation`() {
        val pattern = triplePattern(
            TestTriple("a", "age", 35),
            TestTriple("b", "name", "B"),
            entity = PatternValue.Variable(e),
            attribute = "age",
            value = PatternValue.Variable(age),
        )

        assertEquals(
            listOf(listOf("a")),
            pattern.validate(
                BindingSet(listOf(e), listOf(listOf("a"), listOf("b"))),
                emptyList(),
                listOf(e),
            ).rows,
        )
        assertEquals(
            listOf(listOf("a", 35)),
            pattern.validate(
                BindingSet(listOf(e, age), listOf(listOf("a", 35), listOf("a", 40))),
                emptyList(),
                listOf(e, age),
            ).rows,
        )
    }

    private fun triplePattern(
        vararg triples: TestTriple,
        entity: PatternValue,
        attribute: Any,
        value: PatternValue,
    ): TriplePattern {
        val indexes = indexes(*triples)
        return TriplePattern(0, indexes.aev, indexes.ave, entity, attribute, value)
    }

    private fun indexes(vararg triples: TestTriple): TestIndexes {
        val aev = linkedMapOf<Any, MutableMap<Any, MutableSet<Any>>>()
        val ave = linkedMapOf<Any, MutableMap<Any, MutableSet<Any>>>()
        for ((entity, attribute, value) in triples) {
            aev.getOrPut(attribute, ::linkedMapOf).getOrPut(entity, ::linkedSetOf).add(value)
            ave.getOrPut(attribute, ::linkedMapOf).getOrPut(value, ::linkedSetOf).add(entity)
        }
        fun freeze(index: Map<Any, MutableMap<Any, MutableSet<Any>>>) =
            index.mapValues { (_, inner) -> inner.mapValues { (_, values) -> values.toSet() } }
        return TestIndexes(freeze(aev), freeze(ave))
    }

    private data class TestTriple(val entity: Any, val attribute: Any, val value: Any)
    private data class TestIndexes(
        val aev: Map<Any, Map<Any, Set<Any>>>,
        val ave: Map<Any, Map<Any, Set<Any>>>,
    )
}
