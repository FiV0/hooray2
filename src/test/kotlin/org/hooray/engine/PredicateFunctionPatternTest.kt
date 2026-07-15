package org.hooray.engine

import clojure.lang.Symbol
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class PredicateFunctionPatternTest {
    private val x = Symbol.intern("?x")
    private val y = Symbol.intern("?y")
    private val z = Symbol.intern("?z")

    @Test
    fun `predicate only validates fully bound arguments`() {
        val pattern = PredicatePattern(
            idx = 0,
            arguments = listOf(PatternValue.Variable(x), PatternValue.Constant(10)),
            predicate = { left: Any, right: Any -> (left as Int) < (right as Int) },
        )

        assertEquals(emptyList<Variable>(), pattern.groundable(emptySet()))
        assertEquals(
            listOf(listOf(5)),
            pattern.validate(
                BindingSet(listOf(x), listOf(listOf(5), listOf(15))),
                emptyList(),
                listOf(x),
            ).rows,
        )
    }

    @Test
    fun `function grounds computes and validates its output`() {
        val pattern = FunctionPattern(
            idx = 3,
            arguments = listOf(PatternValue.Variable(x), PatternValue.Constant(2)),
            output = y,
            function = { left: Any, right: Any -> (left as Int) + (right as Int) },
        )
        val input = BindingSet(listOf(x), listOf(listOf(5), listOf(8)))

        assertEquals(listOf(y), pattern.groundable(setOf(x)))
        assertEquals(
            listOf(Proposal(3, 1), Proposal(3, 1)),
            pattern.count(
                input,
                listOf(y),
                List(input.rowCount) { Proposal(NO_PROPOSER, Int.MAX_VALUE) },
            ),
        )
        assertEquals(
            BindingSet(listOf(x, y), listOf(listOf(5, 7), listOf(8, 10))),
            pattern.propose(input, listOf(y), listOf(x, y)),
        )
        assertEquals(
            listOf(listOf(5, 7)),
            pattern.validate(
                BindingSet(listOf(x, y), listOf(listOf(5, 7), listOf(5, 8))),
                emptyList(),
                listOf(x, y),
            ).rows,
        )
    }

    @Test
    fun `compiled evaluators use the binding set layout`() {
        val predicate = PredicatePattern(
            idx = 0,
            arguments = listOf(PatternValue.Variable(x), PatternValue.Variable(y)),
            predicate = { left: Any, right: Any -> (left as Int) < (right as Int) },
        )
        val function = FunctionPattern(
            idx = 1,
            arguments = listOf(PatternValue.Variable(x), PatternValue.Variable(y)),
            output = z,
            function = { left: Any, right: Any -> (left as Int) - (right as Int) },
        )

        assertEquals(
            listOf(listOf(10, 5)),
            predicate.validate(
                BindingSet(listOf(y, x), listOf(listOf(10, 5), listOf(10, 15))),
                emptyList(),
                listOf(y, x),
            ).rows,
        )
        assertEquals(
            BindingSet(listOf(y, x, z), listOf(listOf(2, 5, 3))),
            function.propose(
                BindingSet(listOf(y, x), listOf(listOf(2, 5))),
                listOf(z),
                listOf(y, x, z),
            ),
        )
        assertEquals(
            listOf(listOf(3, 2, 5)),
            function.validate(
                BindingSet(listOf(z, y, x), listOf(listOf(3, 2, 5), listOf(7, 2, 5))),
                emptyList(),
                listOf(z, y, x),
            ).rows,
        )
    }
}
