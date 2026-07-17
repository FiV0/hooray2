package org.hooray.engine

import clojure.lang.Symbol
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

class CompositePatternTest {
    private val x = Symbol.intern("?x")
    private val y = Symbol.intern("?y")
    private val testAttribute = Any()
    private val present = Any()

    @Test
    fun `or exposes missing variables from its branch stages`() {
        val firstTriple = binaryTriplePattern(0, x, y, 1 to 2)
        val secondTriple = binaryTriplePattern(1, x, y, 3 to 4)
        val first = listOf(
            Stage(listOf(x, y), listOf(firstTriple), listOf(x, y)),
        )
        val second = listOf(
            Stage(listOf(x, y), listOf(secondTriple), listOf(x, y)),
        )
        val pattern = OrPattern(2, listOf(first, second))

        assertEquals(listOf(x, y), pattern.groundable(emptySet()))
        assertEquals(listOf(y), pattern.groundable(setOf(x)))
        assertEquals(emptyList<Variable>(), pattern.groundable(setOf(x, y)))
    }

    @Test
    fun `or grounds only variables groundable by every branch`() {
        val firstTriple = unaryTriplePattern(0, x, 1)
        val function = FunctionPattern(
            idx = 1,
            arguments = listOf(PatternValue.Variable(x)),
            output = y,
            function = { value: Any -> value },
        )
        val secondTriple = unaryTriplePattern(2, x, 2)
        val predicate = PredicatePattern(
            idx = 3,
            arguments = listOf(PatternValue.Variable(y)),
            predicate = { _: Any -> true },
        )
        val first = listOf(
            Stage(listOf(x, y), listOf(function, firstTriple), listOf(x, y)),
        )
        val second = listOf(
            Stage(listOf(x, y), listOf(secondTriple, predicate), listOf(x, y)),
        )
        val pattern = OrPattern(4, listOf(first, second))

        assertEquals(listOf(x), pattern.groundable(emptySet()))
        assertEquals(emptyList<Variable>(), pattern.groundable(setOf(x)))
    }

    @Test
    fun `or rejects branches with different variable order`() {
        val firstTriple = binaryTriplePattern(0, x, y, 1 to "a")
        val secondTriple = binaryTriplePattern(1, y, x, "b" to 2)
        val first = listOf(
            Stage(listOf(x, y), listOf(firstTriple), listOf(x, y)),
        )
        val second = listOf(
            Stage(listOf(y, x), listOf(secondTriple), listOf(y, x)),
        )

        val error = assertThrows(IllegalArgumentException::class.java) {
            OrPattern(2, listOf(first, second))
        }
        assertEquals("OR branches must introduce variables in the same order", error.message)
    }

    @Test
    fun `or rejects branches with different variables`() {
        val binaryTriple = binaryTriplePattern(0, x, y)
        val unaryTriple = unaryTriplePattern(1, x)
        val first = listOf(
            Stage(listOf(x, y), listOf(binaryTriple), listOf(x, y)),
        )
        val second = listOf(
            Stage(listOf(x), listOf(unaryTriple), listOf(x)),
        )

        val error = assertThrows(IllegalArgumentException::class.java) {
            OrPattern(2, listOf(first, second))
        }
        assertEquals("OR branches must have the same variables", error.message)
    }

    @Test
    fun `or count is a no-op and single-participant stages propose`() {
        val firstTriple = unaryTriplePattern(0, x, 1, 2)
        val secondTriple = unaryTriplePattern(1, x, 2, 3)
        fun branch(pattern: TriplePattern) =
            listOf(Stage(listOf(x), listOf(pattern), listOf(x)))
        val pattern = OrPattern(
            idx = 5,
            branches = listOf(branch(firstTriple), branch(secondTriple)),
        )
        val countInput = BindingSet(emptyList(), listOf(emptyList(), emptyList()))
        val proposals = listOf(Proposal(NO_PROPOSER, Int.MAX_VALUE), Proposal(9, 4))

        assertEquals(
            proposals,
            pattern.count(
                countInput,
                listOf(x),
                proposals,
            ),
        )
        assertEquals(
            listOf(listOf(1), listOf(2), listOf(3)),
            GenericJoinEngine().execute(
                listOf(Stage(listOf(x), listOf(pattern), listOf(x))),
                BindingSet(emptyList(), listOf(emptyList())),
            ).rows,
        )
    }

    @Test
    fun `or injected relation does not collide with branch participants`() {
        val triple = binaryTriplePattern(Int.MAX_VALUE, x, y, 1 to "a")
        val branch = listOf(Stage(listOf(x, y), listOf(triple), listOf(x, y)))
        val pattern = OrPattern(1, listOf(branch))

        assertEquals(
            listOf(listOf(1, "a")),
            pattern.join(
                BindingSet(listOf(x), listOf(listOf(1))),
                listOf(y),
                listOf(x, y),
            ).rows,
        )
    }

    @Test
    fun `or joins wider input through its projected branch relation`() {
        val triple = binaryTriplePattern(0, x, y, 1 to "a", 2 to "b")
        val branch = listOf(Stage(listOf(x, y), listOf(triple), listOf(x, y)))
        val pattern = OrPattern(1, listOf(branch))
        val outer = Symbol.intern("?outer")
        val proposalInput = BindingSet(
            listOf(outer, x),
            listOf(
                listOf("first", 1),
                listOf("second", 3),
            ),
        )

        assertEquals(
            listOf(listOf("first", 1, "a")),
            pattern.join(
                proposalInput,
                listOf(y),
                listOf(outer, x, y),
            ).rows,
        )

        val validationInput = BindingSet(
            listOf(outer, x, y),
            listOf(
                listOf("first", 1, "a"),
                listOf("second", 1, "missing"),
                listOf("third", 3, "missing"),
            ),
        )

        assertEquals(
            listOf(listOf("first", 1, "a")),
            pattern.join(
                validationInput,
                emptyList(),
                validationInput.variables,
            ).rows,
        )
        assertEquals(
            BindingSet(
                listOf(y, outer, x),
                listOf(listOf("a", "first", 1)),
            ),
            GenericJoinEngine().execute(
                listOf(Stage(emptyList(), listOf(pattern), listOf(y, outer, x))),
                validationInput,
            ),
        )
    }

    @Test
    fun `not projects wider input to subplan variables before matching`() {
        val triple = binaryTriplePattern(0, x, y, 1 to "a", 2 to "b")
        val not = NotPattern(
            idx = 2,
            stages = listOf(Stage(listOf(x, y), listOf(triple), listOf(x, y))),
        )
        val outer = Symbol.intern("?outer")
        val input = BindingSet(
            listOf(outer, x, y),
            listOf(
                listOf("first", 1, "a"),
                listOf("second", 1, "missing"),
                listOf("third", 3, "missing"),
            ),
        )

        assertEquals(
            listOf(
                listOf("second", 1, "missing"),
                listOf("third", 3, "missing"),
            ),
            not.join(input, emptyList(), input.variables).rows,
        )
    }

    private fun unaryTriplePattern(
        idx: Int,
        variable: Variable,
        vararg values: Any,
    ): TriplePattern = triplePattern(
        idx = idx,
        entity = PatternValue.Variable(variable),
        value = PatternValue.Constant(present),
        pairs = values.map { it to present },
    )

    private fun binaryTriplePattern(
        idx: Int,
        entityVariable: Variable,
        valueVariable: Variable,
        vararg pairs: Pair<Any, Any>,
    ): TriplePattern = triplePattern(
        idx = idx,
        entity = PatternValue.Variable(entityVariable),
        value = PatternValue.Variable(valueVariable),
        pairs = pairs.toList(),
    )

    private fun triplePattern(
        idx: Int,
        entity: PatternValue,
        value: PatternValue,
        pairs: List<Pair<Any, Any>>,
    ): TriplePattern {
        val entities = linkedMapOf<Any, MutableSet<Any>>()
        val values = linkedMapOf<Any, MutableSet<Any>>()
        for ((entityValue, valueValue) in pairs) {
            entities.getOrPut(entityValue, ::linkedSetOf).add(valueValue)
            values.getOrPut(valueValue, ::linkedSetOf).add(entityValue)
        }
        return TriplePattern(
            idx = idx,
            aev = mapOf(testAttribute to entities),
            ave = mapOf(testAttribute to values),
            entity = entity,
            attribute = testAttribute,
            value = value,
        )
    }
}
