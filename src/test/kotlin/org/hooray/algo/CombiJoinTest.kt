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
}
