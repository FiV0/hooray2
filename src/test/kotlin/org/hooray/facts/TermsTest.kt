package org.hooray.facts

import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class TermsTest {

    private fun terms(vararg items: ByteArray): Terms {
        val terms = Terms()
        for (item in items) terms.pushItem(item)
        return terms
    }

    @Test
    fun `push and read items round trip`() {
        val terms = terms(byteArrayOf(1, 2), byteArrayOf(), byteArrayOf(3))
        assertEquals(3, terms.size)
        assertEquals(3, terms.byteSize())
        assertArrayEquals(byteArrayOf(1, 2), terms.itemToByteArray(0))
        assertArrayEquals(byteArrayOf(), terms.itemToByteArray(1))
        assertArrayEquals(byteArrayOf(3), terms.itemToByteArray(2))
    }

    @Test
    fun `comparison is unsigned and a shared prefix orders by length`() {
        val terms = terms(
            byteArrayOf(0x7F),               // 127
            byteArrayOf(0x80.toByte()),      // 128 unsigned, -128 signed
            byteArrayOf(1),
            byteArrayOf(1, 0),
            byteArrayOf(1),
        )
        assertTrue(terms.compareItems(0, terms, 1) < 0)
        assertTrue(terms.compareItems(1, terms, 0) > 0)
        assertTrue(terms.compareItems(2, terms, 3) < 0)
        assertTrue(terms.compareItems(3, terms, 2) > 0)
        assertEquals(0, terms.compareItems(2, terms, 4))
        assertTrue(terms.itemsEqual(2, terms, 4))
        assertFalse(terms.itemsEqual(2, terms, 3))
    }

    @Test
    fun `empty item sorts before everything`() {
        val terms = terms(byteArrayOf(), byteArrayOf(0))
        assertTrue(terms.compareItems(0, terms, 1) < 0)
    }

    @Test
    fun `extendFromRange shifts bounds into a non-empty destination`() {
        val src = terms(byteArrayOf(9), byteArrayOf(7, 8), byteArrayOf(6))
        val dst = terms(byteArrayOf(1, 2))
        dst.extendFromRange(src, 1, 3)
        assertEquals(3, dst.size)
        assertArrayEquals(byteArrayOf(1, 2), dst.itemToByteArray(0))
        assertArrayEquals(byteArrayOf(7, 8), dst.itemToByteArray(1))
        assertArrayEquals(byteArrayOf(6), dst.itemToByteArray(2))
    }

    @Test
    fun `fixed width copies keep the destination strided`() {
        val src = terms(byteArrayOf(1, 2), byteArrayOf(3, 4), byteArrayOf(5, 6))
        assertEquals(2, src.bounds.strided())
        val dst = terms(byteArrayOf(0, 0))
        dst.extendFromRange(src, 1, 3)
        assertEquals(2, dst.bounds.strided())
        assertEquals(3, dst.size)
    }

    @Test
    fun `layer extendFromSelf copies lists and shifts both levels of bounds`() {
        // Two lists: [a], [b, c] over single-byte items.
        val src = Layer()
        src.values.pushItem(byteArrayOf(10))
        src.bounds.push(1)
        src.values.pushItem(byteArrayOf(20))
        src.values.pushItem(byteArrayOf(30))
        src.bounds.push(3)

        val dst = Layer()
        dst.values.pushItem(byteArrayOf(1))
        dst.bounds.push(1)

        dst.extendFromSelf(src, 1, 2) // just the list [b, c]
        assertEquals(2, dst.size)
        assertEquals(3, dst.itemCount)
        assertEquals(1, dst.listLower(1))
        assertEquals(3, dst.listUpper(1))
        assertArrayEquals(byteArrayOf(20), dst.values.itemToByteArray(1))
        assertArrayEquals(byteArrayOf(30), dst.values.itemToByteArray(2))
    }
}
