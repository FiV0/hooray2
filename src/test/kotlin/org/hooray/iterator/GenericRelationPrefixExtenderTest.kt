package org.hooray.iterator

import org.hooray.algo.GenericJoin
import org.hooray.algo.PrefixExtender
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class GenericRelationPrefixExtenderTest {

    @Test
    fun `participates only in configured levels`() {
        val extender = GenericRelationPrefixExtender(
            listOf(0, 2),
            listOf(listOf("a", "x")),
        )

        assertEquals(true, extender.participatesInLevel(0))
        assertEquals(false, extender.participatesInLevel(1))
        assertEquals(true, extender.participatesInLevel(2))
        assertEquals(false, extender.participatesInLevel(3))
    }

    @Test
    fun `proposes distinct values from relation trie`() {
        val extender = GenericRelationPrefixExtender(
            listOf(0, 1),
            listOf(
                listOf("a", "x"),
                listOf("a", "y"),
                listOf("a", "y"),
                listOf("b", "z")
            ),
        )

        assertEquals(2, extender.count(emptyList()))
        assertEquals(setOf("a", "b"), extender.propose(emptyList()).toSet())
        assertEquals(2, extender.count(listOf("a")))
        assertEquals(setOf("x", "y"), extender.propose(listOf("a")).toSet())
        assertEquals(1, extender.count(listOf("b")))
        assertEquals(listOf("z"), extender.propose(listOf("b")))
    }

    @Test
    fun `intersect filters candidates while preserving candidate order`() {
        val extender = GenericRelationPrefixExtender(
            listOf(0, 1),
            listOf(
                listOf("a", "x"),
                listOf("a", "y"),
                listOf("b", "z")
            ),
        )

        assertEquals(
            listOf("y", "x"),
            extender.intersect(listOf("a"), listOf("w", "y", "x", "z"))
        )
    }

    @Test
    fun `returns empty results for non matching prefixes`() {
        val extender = GenericRelationPrefixExtender(
            listOf(0, 1),
            listOf(
                listOf("a", "x"),
                listOf("b", "y")
            ),
        )

        assertEquals(0, extender.count(listOf("c")))
        assertEquals(emptyList<Any>(), extender.propose(listOf("c")))
        assertEquals(emptyList<Any>(), extender.intersect(listOf("c"), listOf("x", "y")))
    }

    @Test
    fun `supports non contiguous levels`() {
        val extender = GenericRelationPrefixExtender(
            listOf(0, 2),
            listOf(
                listOf("a", "x"),
                listOf("a", "y"),
                listOf("b", "z")
            ),
        )

        assertEquals(setOf("a", "b"), extender.propose(emptyList()).toSet())
        assertEquals(setOf("x", "y"), extender.propose(listOf("a")).toSet())
        assertEquals(listOf("y"), extender.intersect(listOf("a", "ignored"), listOf("y", "z")))
    }

    @Test
    fun `constrains GenericJoin to relation backed combinations`() {
        val level0Extender = PrefixExtender.createSingleLevel(listOf("Ivan", "Petr", "Bob"), 0)
        val level1Extender = PrefixExtender.createSingleLevel(listOf("Ivanov", "Petrov", "Smith"), 1)
        val relationExtender = GenericRelationPrefixExtender(
            listOf(0, 1),
            listOf(
                listOf("Ivan", "Ivanov"),
                listOf("Petr", "Petrov")
            ),
        )

        val result = GenericJoin(listOf(level0Extender, level1Extender, relationExtender), 2).join()

        assertEquals(
            setOf(
                listOf("Ivan", "Ivanov"),
                listOf("Petr", "Petrov")
            ),
            result.toSet()
        )
    }

    @Test
    fun `rejects invalid constructor arguments`() {
        assertThrows<IllegalArgumentException> {
            GenericRelationPrefixExtender(emptyList(), listOf(listOf("a")))
        }

        assertThrows<IllegalArgumentException> {
            GenericRelationPrefixExtender(listOf(0, 0), listOf(listOf("a", "b")))
        }

        assertThrows<IllegalArgumentException> {
            GenericRelationPrefixExtender(listOf(0, 1), listOf(listOf("a")))
        }
    }
}
