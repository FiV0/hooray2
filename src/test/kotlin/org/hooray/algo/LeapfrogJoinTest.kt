package org.hooray.algo

import org.hooray.UniversalComparator
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class LeapfrogJoinTest {

    // Helper class to create a simple LeapfrogIndex with a fixed sorted set of values
    private class SimpleLeapfrogIndex(
        private val values: List<Int>,
        private val maxLevels: Int = 1
    ) : LeapfrogIndex {
        private var currentIndex = 0
        private var currentLevel = 0

        override fun seek(key: Any) {
            // Find the first value >= key
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

        override fun openLevel() {
            currentLevel++
        }

        override fun closeLevel() {
            currentLevel--
            currentIndex = 0
        }

        override fun level(): Int {
            return currentLevel
        }

        override fun maxLevel(): Int {
            return maxLevels
        }

        override fun participatesInLevel(level: Int): Boolean {
            return level < maxLevels
        }
    }

    @Test
    fun `test empty indexes fails`() {
        val emptyIndexes = emptyList<LeapfrogIndex>()

        assertThrows<IllegalArgumentException> {
            LeapfrogJoin(emptyIndexes, 1)
        }
    }

    @Test
    fun `test two indexes with even and divisible by three`() {
        val evenIndex = SimpleLeapfrogIndex(listOf(2, 4, 6, 8, 10, 12))
        val divisibleByThreeIndex = SimpleLeapfrogIndex(listOf(3, 6, 9, 12))

        val indexes = listOf(evenIndex, divisibleByThreeIndex)

        val join = LeapfrogJoin(indexes, 1)
        val result = join.join()

        assertEquals(2, result.size)

        val expected = listOf(
            listOf(6),
            listOf(12)
        )
        assertEquals(expected, result)
    }
}