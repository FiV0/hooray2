package org.hooray.engine

import clojure.lang.Symbol
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

class CompositePatternTest {
    private val x = Symbol.intern("?x")
    private val y = Symbol.intern("?y")

    @Test
    fun `and computes groundable variables to a fixed point`() {
        val relation = RelationPattern(0, BindingSet(listOf(x), listOf(listOf(1))))
        val function = FunctionPattern(
            idx = 1,
            arguments = listOf(PatternValue.Variable(x)),
            output = y,
            function = { value: Any -> (value as Int) + 1 },
        )
        val pattern = AndPattern(
            idx = 2,
            branch = PatternBranch(listOf(function, relation), StageFactory { emptyList() }),
        )

        assertEquals(
            listOf(x, y),
            pattern.groundable(emptySet()),
        )
    }

    @Test
    fun `or exposes missing variables only when every branch covers them`() {
        val first = PatternBranch(
            patterns = listOf(RelationPattern(0, BindingSet(listOf(x, y), listOf(listOf(1, 2))))),
            stageFactory = StageFactory { emptyList() },
        )
        val second = PatternBranch(
            patterns = listOf(RelationPattern(1, BindingSet(listOf(x, y), listOf(listOf(3, 4))))),
            stageFactory = StageFactory { emptyList() },
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
            stageFactory = StageFactory { emptyList() },
        )
        assertEquals(
            emptyList<Variable>(),
            OrPattern(4, listOf(first, incompleteBranch)).groundable(emptySet()),
        )
    }

    @Test
    fun `or rejects branches with different variable order`() {
        val first = PatternBranch(
            listOf(RelationPattern(0, BindingSet(listOf(x, y), emptyList()))),
            StageFactory { emptyList() },
        )
        val second = PatternBranch(
            listOf(RelationPattern(1, BindingSet(listOf(y, x), emptyList()))),
            StageFactory { emptyList() },
        )

        val error = assertThrows(IllegalArgumentException::class.java) {
            OrPattern(2, listOf(first, second))
        }
        assertEquals("OR branches must have the same ordered variables", error.message)
    }

    @Test
    fun `and executes and caches proposal and validation approaches separately`() {
        val relation = RelationPattern(0, BindingSet(listOf(x), listOf(listOf(1), listOf(2))))
        val requests = mutableListOf<StageRequest>()
        val factory = StageFactory { request ->
            requests += request
            when (request.mode) {
                StageRequestMode.PROPOSE -> listOf(Stage(listOf(x), listOf(relation), listOf(x)))
                StageRequestMode.VALIDATE -> listOf(Stage(emptyList(), listOf(relation), request.seedVariables))
            }
        }
        val pattern = AndPattern(1, PatternBranch(listOf(relation), factory))
        val unit = BindingSet(emptyList(), listOf(emptyList()))

        assertEquals(
            listOf(Proposal(1, 2)),
            pattern.count(unit, listOf(x), listOf(Proposal(NO_PROPOSER, Int.MAX_VALUE))),
        )
        assertEquals(listOf(listOf(1), listOf(2)), pattern.propose(unit, listOf(x), listOf(x)).rows)
        assertEquals(listOf(listOf(1)), pattern.validate(
            BindingSet(listOf(x), listOf(listOf(1), listOf(3))),
            emptyList(),
            listOf(x),
        ).rows)

        assertEquals(2, requests.size)
        assertEquals(listOf(StageRequestMode.PROPOSE, StageRequestMode.VALIDATE), requests.map { it.mode })
    }

    @Test
    fun `or is a fallback proposer and unions branch results distinctly`() {
        val firstRelation = RelationPattern(0, BindingSet(listOf(x), listOf(listOf(1), listOf(2))))
        val secondRelation = RelationPattern(1, BindingSet(listOf(x), listOf(listOf(2), listOf(3))))
        val firstRequests = mutableListOf<StageRequest>()
        fun branch(pattern: RelationPattern, requests: MutableList<StageRequest>) = PatternBranch(
            listOf(pattern),
            StageFactory { request ->
                requests += request
                if (request.mode == StageRequestMode.PROPOSE) {
                    listOf(Stage(listOf(x), listOf(pattern), listOf(x)))
                } else {
                    listOf(Stage(emptyList(), listOf(pattern), request.seedVariables))
                }
            },
        )
        val pattern = OrPattern(
            idx = 5,
            branches = listOf(branch(firstRelation, firstRequests), branch(secondRelation, mutableListOf())),
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
            pattern.propose(unit, listOf(x), listOf(x)).rows,
        )
        pattern.propose(unit, listOf(x), listOf(x))
        assertEquals(1, firstRequests.size)
    }

    @Test
    fun `or validates existential branch completions and not antijoins them`() {
        val relation = RelationPattern(
            0,
            BindingSet(listOf(x, y), listOf(listOf(1, "a"), listOf(2, "b"))),
        )
        val factory = StageFactory { request ->
            listOf(Stage(listOf(y), listOf(relation), request.seedVariables + y))
        }
        val branch = PatternBranch(listOf(relation), factory)
        val or = OrPattern(1, listOf(branch))
        val not = NotPattern(2, branch)
        val input = BindingSet(listOf(x), listOf(listOf(1), listOf(3)))

        assertEquals(listOf(listOf(1)), or.validate(input, emptyList(), listOf(x)).rows)
        assertEquals(listOf(listOf(3)), not.validate(input, emptyList(), listOf(x)).rows)
    }
}
