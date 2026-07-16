package org.hooray.engine

import clojure.lang.Symbol
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class RelationPatternTest {
    private val e = Symbol.intern("?e")
    private val age = Symbol.intern("?age")
    private val name = Symbol.intern("?name")
    private val city = Symbol.intern("?city")
    private val source = Symbol.intern("?source")

    private val pattern = RelationPattern(
        idx = 4,
        relation = BindingSet(
            listOf(e, age),
            listOf(listOf("a", 35), listOf("a", 36), listOf("b", 40)),
        ),
    )

    @Test
    fun `exposes unbound relation variables as groundable`() {
        assertEquals(
            listOf(e, age),
            pattern.groundable(emptySet()),
        )
        assertEquals(listOf(age), pattern.groundable(setOf(e)))
    }

    @Test
    fun `counts and proposes distinct correlated introductions`() {
        val input = BindingSet(listOf(e), listOf(listOf("a"), listOf("b"), listOf("c")))
        val proposals = List(input.rowCount) { Proposal(NO_PROPOSER, Int.MAX_VALUE) }

        assertEquals(
            listOf(Proposal(4, 2), Proposal(4, 1), Proposal(NO_PROPOSER, Int.MAX_VALUE)),
            pattern.count(input, listOf(age), proposals),
        )
        val proposed = pattern.join(input, listOf(age), listOf(age, e))
        assertEquals(listOf(age, e), proposed.variables)
        assertEquals(3, proposed.rowCount)
        assertEquals(
            setOf(listOf(35, "a"), listOf(36, "a"), listOf(40, "b")),
            proposed.rows.toSet(),
        )
    }

    @Test
    fun `validates with existential relation support`() {
        val input = BindingSet(listOf(e), listOf(listOf("a"), listOf("c")))

        assertEquals(
            BindingSet(listOf(e), listOf(listOf("a"))),
            pattern.join(input, emptyList(), listOf(e)),
        )
    }

    @Test
    fun `counts and proposes a consecutive block with an existential suffix`() {
        val blockPattern = RelationPattern(
            idx = 7,
            relation = BindingSet(
                listOf(e, age, name, city),
                listOf(
                    listOf("a", 35, "Alice", "Berlin"),
                    listOf("a", 35, "Alice", "Paris"),
                    listOf("a", 36, "Ada", "London"),
                    listOf("a", 36, "Ada", "London"),
                    listOf("b", 40, "Bob", "Rome"),
                ),
            ),
        )
        val input = BindingSet(
            listOf(source, e),
            listOf(listOf("left", "a"), listOf("right", "b"), listOf("missing", "c")),
        )
        val proposals = List(input.rowCount) { Proposal(NO_PROPOSER, Int.MAX_VALUE) }

        assertEquals(
            listOf(Proposal(7, 2), Proposal(7, 1), Proposal(NO_PROPOSER, Int.MAX_VALUE)),
            blockPattern.count(input, listOf(age, name), proposals),
        )

        val proposed = blockPattern.join(
            input,
            added = listOf(age, name),
            targetVariables = listOf(name, source, e, age),
        )
        assertEquals(listOf(name, source, e, age), proposed.variables)
        assertEquals(3, proposed.rowCount)
        assertEquals(
            setOf(
                listOf("Alice", "left", "a", 35),
                listOf("Ada", "left", "a", 36),
                listOf("Bob", "right", "b", 40),
            ),
            proposed.rows.toSet(),
        )
    }

    @Test
    fun `counts and proposes from the trie root`() {
        val rootPattern = RelationPattern(
            idx = 8,
            relation = BindingSet(
                listOf(e, age, name),
                listOf(
                    listOf("a", 35, "Alice"),
                    listOf("a", 35, "Alicia"),
                    listOf("a", 36, "Ada"),
                    listOf("b", 40, "Bob"),
                ),
            ),
        )
        val input = BindingSet(listOf(source), listOf(listOf("seed")))

        assertEquals(
            listOf(Proposal(8, 3)),
            rootPattern.count(
                input,
                added = listOf(e, age),
                proposals = listOf(Proposal(NO_PROPOSER, Int.MAX_VALUE)),
            ),
        )

        val proposed = rootPattern.join(
            input,
            added = listOf(e, age),
            targetVariables = listOf(source, age, e),
        )
        assertEquals(listOf(source, age, e), proposed.variables)
        assertEquals(3, proposed.rowCount)
        assertEquals(
            setOf(
                listOf("seed", 35, "a"),
                listOf("seed", 36, "a"),
                listOf("seed", 40, "b"),
            ),
            proposed.rows.toSet(),
        )
    }

    @Test
    fun `counts and proposes with unrelated variables interleaved through the relation prefix`() {
        val interleavedPattern = RelationPattern(
            idx = 9,
            relation = BindingSet(
                listOf(e, age, name),
                listOf(listOf("a", 35, "Alice"), listOf("a", 36, "Ada"), listOf("b", 40, "Bob")),
            ),
        )
        val input = BindingSet(
            listOf(e, source, age),
            listOf(listOf("a", "first", 35), listOf("a", "second", 40)),
        )
        val proposals = List(input.rowCount) { Proposal(NO_PROPOSER, Int.MAX_VALUE) }

        assertEquals(
            listOf(Proposal(9, 1), Proposal(NO_PROPOSER, Int.MAX_VALUE)),
            interleavedPattern.count(input, listOf(name), proposals),
        )

        val proposed = interleavedPattern.join(
            input,
            added = listOf(name),
            targetVariables = listOf(source, name, e, age),
        )
        assertEquals(
            BindingSet(
                listOf(source, name, e, age),
                listOf(listOf("first", "Alice", "a", 35)),
            ),
            proposed,
        )
    }

    @Test
    fun `validates partial and complete relation prefixes`() {
        val prefixPattern = RelationPattern(
            idx = 10,
            relation = BindingSet(
                listOf(e, age, name),
                listOf(listOf("a", 35, "Alice"), listOf("a", 36, "Ada"), listOf("b", 40, "Bob")),
            ),
        )
        val partialInput = BindingSet(
            listOf(e, source, age),
            listOf(
                listOf("a", "first", 35),
                listOf("a", "second", 40),
                listOf("c", "third", 35),
            ),
        )
        val completeInput = BindingSet(
            listOf(e, age, name),
            listOf(listOf("a", 36, "Ada"), listOf("a", 36, "Alice")),
        )

        assertEquals(
            BindingSet(listOf(e, source, age), listOf(listOf("a", "first", 35))),
            prefixPattern.join(partialInput, emptyList(), partialInput.variables),
        )
        assertEquals(
            BindingSet(listOf(e, age, name), listOf(listOf("a", 36, "Ada"))),
            prefixPattern.join(completeInput, emptyList(), completeInput.variables),
        )
    }

    @Test
    fun `validates zero variable relations by whether they contain a row`() {
        val input = BindingSet(listOf(source), listOf(listOf("first"), listOf("second")))
        val nonEmptyPattern = RelationPattern(
            idx = 11,
            relation = BindingSet(emptyList(), listOf(emptyList())),
        )
        val emptyPattern = RelationPattern(
            idx = 12,
            relation = BindingSet(emptyList(), emptyList()),
        )

        assertEquals(input, nonEmptyPattern.join(input, emptyList(), input.variables))
        assertEquals(
            BindingSet(listOf(source), emptyList()),
            emptyPattern.join(input, emptyList(), input.variables),
        )
    }
}
