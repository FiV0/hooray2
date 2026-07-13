package org.hooray.engine

import clojure.lang.Symbol
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class PredicateFunctionPatternTest {
    private val x = Symbol.intern("?x")
    private val y = Symbol.intern("?y")

    @Test
    fun `predicate only validates fully bound arguments`() {
        val pattern = PredicatePattern(
            idx = 0,
            arguments = listOf(PatternValue.Variable(x), PatternValue.Constant(10)),
            predicate = { left: Any, right: Any -> (left as Int) < (right as Int) },
        )

        assertEquals(emptyList<GroundingGroup>(), pattern.groundingGroups(emptyList()))
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

        assertEquals(listOf(GroundingGroup(listOf(y))), pattern.groundingGroups(listOf(x)))
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
}
