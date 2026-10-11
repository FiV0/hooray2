package org.hooray.facts

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class StridesTest {

    @Test
    fun `push maintains the stride while offsets are evenly spaced`() {
        val strides = Strides()
        strides.push(4)
        strides.push(8)
        strides.push(12)
        assertEquals(3, strides.size)
        assertEquals(4, strides.strided())
        assertEquals(12, strides.last())
        assertEquals(0, strides.lower(0))
        assertEquals(4, strides.upper(0))
        assertEquals(4, strides.lower(1))
        assertEquals(8, strides.upper(1))
        assertEquals(8, strides.lower(2))
        assertEquals(12, strides.upper(2))
    }

    @Test
    fun `first push defines the stride`() {
        val strides = Strides()
        strides.push(7)
        assertEquals(7, strides.strided())
        strides.push(14)
        assertEquals(7, strides.strided())
        assertEquals(2, strides.size)
    }

    @Test
    fun `spilling starts when the pattern breaks and is sticky`() {
        val strides = Strides()
        strides.push(4)
        strides.push(8)
        strides.push(9)
        assertEquals(-1, strides.strided())
        // 12 would fit stride 4 again, but once spilled always spilled.
        strides.push(12)
        assertEquals(-1, strides.strided())
        assertEquals(4, strides.size)
        assertEquals(9, strides.offset(2))
        assertEquals(12, strides.offset(3))
        // Bounds across the implicit/spill boundary.
        assertEquals(8, strides.lower(2))
        assertEquals(9, strides.upper(2))
        assertEquals(9, strides.lower(3))
        assertEquals(12, strides.upper(3))
        assertEquals(12, strides.last())
    }

    @Test
    fun `pop removes from the spill and then the strided region`() {
        val strides = Strides()
        strides.push(4)
        strides.push(8)
        strides.push(9)
        strides.pop()
        assertEquals(2, strides.size)
        assertEquals(4, strides.strided())
        strides.pop()
        assertEquals(1, strides.size)
        assertEquals(4, strides.last())
    }

    @Test
    fun `constructor mirrors Strides new`() {
        val strides = Strides(1, 5)
        assertEquals(5, strides.size)
        assertEquals(1, strides.strided())
        assertEquals(3, strides.offset(2))
        assertEquals(2, strides.lower(2))
        assertEquals(3, strides.upper(2))
    }

    @Test
    fun `empty strides has no offsets and stride zero`() {
        val strides = Strides()
        assertTrue(strides.isEmpty())
        assertEquals(0, strides.size)
        assertEquals(0, strides.last())
        assertEquals(0, strides.strided())
    }

    @Test
    fun `bounds packs lower and upper`() {
        val strides = Strides()
        strides.push(3)
        strides.push(6)
        val bounds = strides.bounds(1)
        assertEquals(3, Strides.boundsLower(bounds))
        assertEquals(6, Strides.boundsUpper(bounds))
    }
}
