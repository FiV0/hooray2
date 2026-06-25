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
            listOf(listOf("a", "x")),
            listOf(0, 2)
        )

        assertEquals(true, extender.participatesInLevel(0))
        assertEquals(false, extender.participatesInLevel(1))
        assertEquals(true, extender.participatesInLevel(2))
        assertEquals(false, extender.participatesInLevel(3))
    }

    @Test
    fun `proposes distinct values from relation trie`() {
        val extender = GenericRelationPrefixExtender(
            listOf(
                listOf("a", "x"),
                listOf("a", "y"),
                listOf("a", "y"),
                listOf("b", "z")
            ),
            listOf(0, 1)
        )

        assertEquals(2, extender.count(emptyList()))
        assertEquals(listOf("a", "b"), extender.propose(emptyList()))
        assertEquals(2, extender.count(listOf("a")))
        assertEquals(listOf("x", "y"), extender.propose(listOf("a")))
        assertEquals(1, extender.count(listOf("b")))
        assertEquals(listOf("z"), extender.propose(listOf("b")))
    }

    @Test
    fun `intersect filters candidates while preserving candidate order`() {
        val extender = GenericRelationPrefixExtender(
            listOf(
                listOf("a", "x"),
                listOf("a", "y"),
                listOf("b", "z")
            ),
            listOf(0, 1)
        )

        assertEquals(
            listOf("y", "x"),
            extender.intersect(listOf("a"), listOf("w", "y", "x", "z"))
        )
    }

    @Test
    fun `returns empty results for non matching prefixes`() {
        val extender = GenericRelationPrefixExtender(
            listOf(
                listOf("a", "x"),
                listOf("b", "y")
            ),
            listOf(0, 1)
        )

        assertEquals(0, extender.count(listOf("c")))
        assertEquals(emptyList<Any>(), extender.propose(listOf("c")))
        assertEquals(emptyList<Any>(), extender.intersect(listOf("c"), listOf("x", "y")))
    }

    @Test
    fun `supports non contiguous levels`() {
        val extender = GenericRelationPrefixExtender(
            listOf(
                listOf("a", "x"),
                listOf("a", "y"),
                listOf("b", "z")
            ),
            listOf(0, 2)
        )

        assertEquals(listOf("a", "b"), extender.propose(emptyList()))
        assertEquals(listOf("x", "y"), extender.propose(listOf("a")))
        assertEquals(listOf("y"), extender.intersect(listOf("a", "ignored"), listOf("y", "z")))
    }

    @Test
    fun `constrains GenericJoin to relation backed combinations`() {
        val level0Extender = PrefixExtender.createSingleLevel(listOf("Ivan", "Petr", "Bob"), 0)
        val level1Extender = PrefixExtender.createSingleLevel(listOf("Ivanov", "Petrov", "Smith"), 1)
        val relationExtender = GenericRelationPrefixExtender(
            listOf(
                listOf("Ivan", "Ivanov"),
                listOf("Petr", "Petrov")
            ),
            listOf(0, 1)
        )

        val result = GenericJoin(listOf(level0Extender, level1Extender, relationExtender), 2).join()

        assertEquals(
            listOf(
                listOf("Ivan", "Ivanov"),
                listOf("Petr", "Petrov")
            ),
            result
        )
    }

    @Test
    fun `rejects invalid constructor arguments`() {
        assertThrows<IllegalArgumentException> {
            GenericRelationPrefixExtender(listOf(listOf("a")), emptyList())
        }

        assertThrows<IllegalArgumentException> {
            GenericRelationPrefixExtender(listOf(listOf("a", "b")), listOf(0, 0))
        }

        assertThrows<IllegalArgumentException> {
            GenericRelationPrefixExtender(listOf(listOf("a")), listOf(0, 1))
        }
    }
}
