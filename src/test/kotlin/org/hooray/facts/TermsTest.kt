package org.hooray.facts

import clojure.lang.Keyword
import clojure.lang.PersistentVector
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class TermsTest {

    private fun terms(vararg items: Any?): Terms {
        val terms = Terms()
        for (item in items) terms.pushItem(item)
        return terms
    }

    @Test
    fun `push and read items round trip`() {
        val vector = PersistentVector.create(1L, 2L)
        val terms = terms("a", null, vector)
        assertEquals(3, terms.size)
        assertEquals("a", terms[0])
        assertNull(terms[1])
        assertSame(vector, terms[2])
    }

    @Test
    fun `comparison follows the universal type discriminators`() {
        val terms = terms(
            1L,
            "s",
            Keyword.intern("k"),
            PersistentVector.create(0L),
            2L,
        )
        assertTrue(terms.compareItems(0, terms, 1) < 0) // Number < String
        assertTrue(terms.compareItems(1, terms, 2) < 0) // String < Keyword
        assertTrue(terms.compareItems(2, terms, 3) < 0) // Keyword < PersistentVector
        assertTrue(terms.compareItems(0, terms, 4) < 0) // 1 < 2
        assertTrue(terms.compareItems(4, terms, 0) > 0)
        assertFalse(terms.itemsEqual(0, terms, 4))
    }

    @Test
    fun `items are equal when they compare equal`() {
        // Util.compare(1L, 1.0) == 0 even though equals differs: dedup follows the order.
        val terms = terms(1L, 1.0, 2L)
        assertEquals(0, terms.compareItems(0, terms, 1))
        assertTrue(terms.itemsEqual(0, terms, 1))
        assertFalse(terms.itemsEqual(0, terms, 2))
    }

    @Test
    fun `null sorts before everything`() {
        val terms = terms(null, 0L)
        assertTrue(terms.compareItems(0, terms, 1) < 0)
        assertTrue(terms.itemsEqual(0, terms, 0))
    }

    @Test
    fun `extendFromRange appends to a non-empty destination`() {
        val src = terms(9L, 7L, 6L)
        val dst = terms(1L)
        dst.extendFromRange(src, 1, 3)
        assertEquals(3, dst.size)
        assertEquals(1L, dst[0])
        assertEquals(7L, dst[1])
        assertEquals(6L, dst[2])
    }

    @Test
    fun `layer extendFromSelf copies lists and shifts the list bounds`() {
        // Two lists: [a], [b, c].
        val src = Layer()
        src.values.pushItem(10L)
        src.bounds.push(1)
        src.values.pushItem(20L)
        src.values.pushItem(30L)
        src.bounds.push(3)

        val dst = Layer()
        dst.values.pushItem(1L)
        dst.bounds.push(1)

        dst.extendFromSelf(src, 1, 2) // just the list [b, c]
        assertEquals(2, dst.size)
        assertEquals(3, dst.itemCount)
        assertEquals(1, dst.listLower(1))
        assertEquals(3, dst.listUpper(1))
        assertEquals(20L, dst.values[1])
        assertEquals(30L, dst.values[2])
    }
}
