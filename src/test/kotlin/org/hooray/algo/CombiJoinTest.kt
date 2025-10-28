package org.hooray.algo

import org.hooray.UniversalComparator
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class CombiJoinTest {

    private class SimpleLeapfrogIterator(private val values: List<Int>) : LeapfrogIterator {
        private var currentIndex = 0

        override fun seek(key: Any) {
            while (currentIndex < values.size && UniversalComparator.compare(values[currentIndex], key) < 0) {
                currentIndex++
            }
        }

        override fun next(): Any {
            if (currentIndex < values.size) {
                currentIndex++
            }
            return if (atEnd()) Unit else values[currentIndex]
        }

        override fun key(): Any {
            return if (atEnd()) throw IllegalStateException("At end") else values[currentIndex]
        }

        override fun atEnd(): Boolean {
            return currentIndex >= values.size
        }
    }

    private fun createSimpleExtender(values: List<Int>): CombiJoinExtender {
        return object : CombiJoinExtender {
            override fun getIterators(prefix: Prefix): LeapfrogIterator {
                return SimpleLeapfrogIterator(values)
            }
        }
    }

    @Test
    fun `test empty iterators fails`() {
        val emptyIterators = emptyList<LeapfrogIterator>()

        assertThrows<IllegalArgumentException> {
            CombiSingleJoin(emptyIterators)
        }
    }

    @Test
    fun `test two extenders with even and divisible by three`() {
        val evenExtender = createSimpleExtender(listOf(2, 4, 6, 8, 10, 12))
        val divisibleByThreeExtender = createSimpleExtender(listOf(3, 6, 9, 12))

        val extenders = listOf(evenExtender, divisibleByThreeExtender)

        val join = CombiJoin(listOf(extenders))
        val result = join.join()

        assertEquals(2, result.size)

        val expected = listOf(
            listOf(6),
            listOf(12)
        )
        assertEquals(expected, result)
    }

    @Test
    fun `test CombiJoin with two levels`() {
        // First level: even numbers and numbers divisible by 3 (same as previous test)
        val evenExtender = createSimpleExtender(listOf(2, 4, 6, 8, 10, 12))
        val divisibleByThreeExtender = createSimpleExtender(listOf(3, 6, 9, 12))

        // Second level: extender that takes the prefix value and returns all values divisible by it up to 12
        val divisibleByPrefixExtender = object : CombiJoinExtender {
            override fun getIterators(prefix: Prefix): LeapfrogIterator {
                val divisor = prefix[0] as Int
                val values = (1..12).filter { it % divisor == 0 }
                return SimpleLeapfrogIterator(values)
            }
        }

        val extenders = listOf(
            listOf(evenExtender, divisibleByThreeExtender),  // First level
            listOf(divisibleByPrefixExtender)                 // Second level
        )

        val join = CombiJoin(extenders)
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
