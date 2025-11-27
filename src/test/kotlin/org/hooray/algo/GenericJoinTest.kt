package org.hooray.algo

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class GenericJoinTest {

    @Test
    fun `test empty extenders fails`() {
        val emptyExtenders = emptyList<PrefixExtender>()
        val prefixes = listOf(emptyList<Any>())

        assertThrows<IllegalArgumentException> {
            GenericSingleJoin(emptyExtenders, prefixes)
        }
    }

    @Test
    fun `test two extenders with even and divisible by three`() {
        val evenExtender = PrefixExtender.createSingleLevel(listOf(2, 4, 6, 8, 10, 12), 0)
        val divisibleByThreeExtender = PrefixExtender.createSingleLevel(listOf(3, 6, 9, 12), 0)

        val extenders = listOf(evenExtender, divisibleByThreeExtender)
        val prefixes = listOf(emptyList<Any>())

        val join = GenericSingleJoin(extenders, prefixes)
        val result = join.join()

        assertEquals(2, result.size)

        val expected = listOf(
            listOf(6),
            listOf(12)
        )
        assertEquals(expected, result)
    }

    @Test
    fun `test GenericJoin with two levels`() {
        // First level: even numbers and numbers divisible by 3 (same as previous test)
        val evenExtender = PrefixExtender.createSingleLevel(listOf(2, 4, 6, 8, 10, 12), 0)
        val divisibleByThreeExtender = PrefixExtender.createSingleLevel(listOf(3, 6, 9, 12), 0)

        // Second level: single extender that takes the prefix value and returns all values divisible by it up to 12
        val divisibleByPrefixExtender = object : PrefixExtender {
            override fun count(prefix: Prefix): Int {
                val divisor = prefix[0] as Int
                return (1..12).count { it % divisor == 0 }
            }

            override fun propose(prefix: Prefix): List<Extension> {
                val divisor = prefix[0] as Int
                return (1..12).filter { it % divisor == 0 }
            }

            override fun intersect(prefix: Prefix, extensions: List<Extension>): List<Extension> {
                val divisor = prefix[0] as Int
                return extensions.filter { ext ->
                    ext is Int && ext % divisor == 0
                }
            }

            override fun participatesInLevel(level: Int) = level == 1
        }

        val extenders = listOf(
            evenExtender, divisibleByThreeExtender,  // First level
            divisibleByPrefixExtender                // Second level
        )

        val join = GenericJoin(extenders, 2)
        val result = join.join()

        // First level produces: [6] and [12]
        // Second level for prefix [6]: numbers divisible by 6 up to 12 -> 6, 12 -> results: [6, 6], [6, 12]
        // Second level for prefix [12]: numbers divisible by 12 up to 12 -> 12 -> result: [12, 12]
        assertEquals(3, result.size)

        val expected = listOf(
            listOf(6, 6),
            listOf(6, 12),
            listOf(12, 12)
        )
        assertEquals(expected, result)
    }
}
