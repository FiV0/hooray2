package org.hooray.engine

import clojure.lang.Symbol
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class RelationPatternTest {
    private val e = Symbol.intern("?e")
    private val age = Symbol.intern("?age")

    private val pattern = RelationPattern(
        idx = 4,
        relation = BindingSet(
            listOf(e, age),
            listOf(listOf("a", 35), listOf("a", 36), listOf("b", 40)),
        ),
    )

    @Test
    fun `exposes unbound relation variables as singleton grounding groups`() {
        assertEquals(
            listOf(GroundingGroup(listOf(e)), GroundingGroup(listOf(age))),
            pattern.groundingGroups(emptyList()),
        )
        assertEquals(listOf(GroundingGroup(listOf(age))), pattern.groundingGroups(listOf(e)))
    }

    @Test
    fun `counts and proposes distinct correlated introductions`() {
        val input = BindingSet(listOf(e), listOf(listOf("a"), listOf("b"), listOf("c")))
        val proposals = List(input.rowCount) { Proposal(NO_PROPOSER, Int.MAX_VALUE) }

        assertEquals(
            listOf(Proposal(4, 2), Proposal(4, 1), Proposal(NO_PROPOSER, Int.MAX_VALUE)),
            pattern.count(input, listOf(age), proposals),
        )
        assertEquals(
            BindingSet(
                listOf(age, e),
                listOf(listOf(35, "a"), listOf(36, "a"), listOf(40, "b")),
            ),
            pattern.propose(input, listOf(age), listOf(age, e)),
        )
    }

    @Test
    fun `validates with existential relation support`() {
        val input = BindingSet(listOf(e), listOf(listOf("a"), listOf("c")))

        assertEquals(
            BindingSet(listOf(e), listOf(listOf("a"))),
            pattern.validate(input, emptyList(), listOf(e)),
        )
    }
}
