package org.hooray.facts

import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class RadixSortTest {

    private fun lcg(seed: Long): () -> Long {
        var state = seed * 2 + 1
        return {
            state = state * 6364136223846793005L + 1442695040888963407L
            state ushr 33
        }
    }

    @Test
    fun `sorts random keys and permutes the payload consistently`() {
        val next = lcg(42)
        // High dword is a small group (non-negative), low dword arbitrary including
        // values with the high bit set; signed Long order then equals unsigned order.
        val keys = LongArray(1000) {
            ((next() % 16) shl 32) or ((next() shl 20) and 0xFFFF_FFFFL)
        }
        val original = keys.copyOf()
        val payload = IntArray(keys.size) { it }

        RadixSort.lsbRange(keys, payload, 0, 8)

        assertArrayEquals(original.sortedArray(), keys)
        for (p in keys.indices) {
            assertEquals(original[payload[p]], keys[p], "payload must track its key")
        }
    }

    @Test
    fun `sorts without payload`() {
        val next = lcg(7)
        val keys = LongArray(257) { (next() % 512) }
        val expected = keys.sortedArray()
        RadixSort.lsbRange(keys, null, 0, 8)
        assertArrayEquals(expected, keys)
    }

    @Test
    fun `one active byte exercises the odd pass copy back`() {
        // All bytes agree except the least significant: exactly one scatter pass.
        val keys = longArrayOf(3, 1, 2, 0)
        val payload = intArrayOf(0, 1, 2, 3)
        RadixSort.lsbRange(keys, payload, 0, 8)
        assertArrayEquals(longArrayOf(0, 1, 2, 3), keys)
        assertArrayEquals(intArrayOf(3, 1, 2, 0), payload)
    }

    @Test
    fun `byte sub range ignores bytes outside it`() {
        // Keys differ only in the low dword; sorting bytes 0..4 must not move anything.
        val keys = longArrayOf(0xFFL, 0x01L, 0x7FL)
        val expected = keys.copyOf()
        RadixSort.lsbRange(keys, null, 0, 4)
        assertArrayEquals(expected, keys)
    }

    @Test
    fun `empty and single element inputs`() {
        val empty = LongArray(0)
        RadixSort.lsbRange(empty, null, 0, 8)
        val single = longArrayOf(5)
        RadixSort.lsbRange(single, null, 0, 8)
        assertArrayEquals(longArrayOf(5), single)
    }
}
