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
    fun `and computes groundable variables to a fixed point`() {
        val triple = unaryTriplePattern(0, x, 1)
        val function = FunctionPattern(
            idx = 1,
            arguments = listOf(PatternValue.Variable(x)),
            output = y,
            function = { value: Any -> (value as Int) + 1 },
        )
        val pattern = AndPattern(
            idx = 2,
            branch = PatternBranch(listOf(function, triple), emptyList()),
        )

        assertEquals(
            listOf(x, y),
            pattern.groundable(emptySet()),
        )
    }

    @Test
    fun `or exposes missing variables only when every branch covers them`() {
        val first = PatternBranch(
            patterns = listOf(binaryTriplePattern(0, x, y, 1 to 2)),
            stages = emptyList(),
        )
        val second = PatternBranch(
            patterns = listOf(binaryTriplePattern(1, x, y, 3 to 4)),
            stages = emptyList(),
        )
        val pattern = OrPattern(2, listOf(first, second))

        assertEquals(listOf(x, y), pattern.groundable(emptySet()))
        assertEquals(listOf(y), pattern.groundable(setOf(x)))
        assertEquals(emptyList<Variable>(), pattern.groundable(setOf(x, y)))

        val incompleteBranch = PatternBranch(
            patterns = listOf(
                PredicatePattern(
                    3,
                    listOf(PatternValue.Variable(x), PatternValue.Variable(y)),
                    { _: Any, _: Any -> true },
                ),
            ),
            stages = emptyList(),
        )
        assertEquals(
            emptyList<Variable>(),
            OrPattern(4, listOf(first, incompleteBranch)).groundable(emptySet()),
        )
    }

    @Test
    fun `or rejects branches with different variable order`() {
        val first = PatternBranch(
            listOf(binaryTriplePattern(0, x, y)),
            emptyList(),
        )
        val second = PatternBranch(
            listOf(binaryTriplePattern(1, y, x)),
            emptyList(),
        )

        val error = assertThrows(IllegalArgumentException::class.java) {
            OrPattern(2, listOf(first, second))
        }
        assertEquals("OR branches must have the same ordered variables", error.message)
    }

    @Test
    fun `and executes construction-time subplan stages`() {
        val triple = unaryTriplePattern(0, x, 1, 2)
        val proposalBranch = PatternBranch(
            patterns = listOf(triple),
            stages = listOf(Stage(listOf(x), listOf(triple), listOf(x))),
        )
        val validationBranch = PatternBranch(
            patterns = listOf(triple),
            stages = listOf(Stage(emptyList(), listOf(triple), listOf(x))),
        )
        val proposalPattern = AndPattern(1, proposalBranch)
        val validationPattern = AndPattern(1, validationBranch)
        val unit = BindingSet(emptyList(), listOf(emptyList()))

        assertEquals(
            listOf(Proposal(1, 2)),
            proposalPattern.count(unit, listOf(x), listOf(Proposal(NO_PROPOSER, Int.MAX_VALUE))),
        )
        assertEquals(listOf(listOf(1), listOf(2)), proposalPattern.join(unit, listOf(x), listOf(x)).rows)
        assertEquals(listOf(listOf(1)), validationPattern.join(
            BindingSet(listOf(x), listOf(listOf(1), listOf(3))),
            emptyList(),
            listOf(x),
        ).rows)
    }

    @Test
    fun `or is a fallback proposer and unions branch results distinctly`() {
        val firstTriple = unaryTriplePattern(0, x, 1, 2)
        val secondTriple = unaryTriplePattern(1, x, 2, 3)
        fun branch(pattern: TriplePattern) = PatternBranch(
            patterns = listOf(pattern),
            stages = listOf(Stage(listOf(x), listOf(pattern), listOf(x))),
        )
        val pattern = OrPattern(
            idx = 5,
            branches = listOf(branch(firstTriple), branch(secondTriple)),
        )
        val unit = BindingSet(emptyList(), listOf(emptyList(), emptyList()))

        assertEquals(
            listOf(Proposal(5, Int.MAX_VALUE), Proposal(9, 4)),
            pattern.count(
                unit,
                listOf(x),
                listOf(Proposal(NO_PROPOSER, Int.MAX_VALUE), Proposal(9, 4)),
            ),
        )
        assertEquals(
            listOf(listOf(1), listOf(2), listOf(3)),
            pattern.join(unit, listOf(x), listOf(x)).rows,
        )
    }

    @Test
    fun `or validates existential branch completions and not antijoins them`() {
        val triple = binaryTriplePattern(0, x, y, 1 to "a", 2 to "b")
        val subStages = listOf(Stage(listOf(y), listOf(triple), listOf(x, y)))
        val branch = PatternBranch(listOf(triple), subStages)
        val or = OrPattern(1, listOf(branch))
        val not = NotPattern(2, branch)
        val input = BindingSet(listOf(x), listOf(listOf(1), listOf(3)))

        assertEquals(listOf(listOf(1)), or.join(input, emptyList(), listOf(x)).rows)
        assertEquals(listOf(listOf(3)), not.join(input, emptyList(), listOf(x)).rows)
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
