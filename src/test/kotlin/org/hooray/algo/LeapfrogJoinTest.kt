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

    @Test
    fun `test LeapfrogJoin with two levels`() {
        class MultiLevelIndex(
            private val levelData: List<List<Int>>  // levelData[i] contains values for level i
        ) : LeapfrogIndex {
            private var currentLevel = 0
            private val indexPerLevel = MutableList(levelData.size) { 0 }

            override fun seek(key: Any) {
                val values = levelData[currentLevel]
                var currentIndex = indexPerLevel[currentLevel]
                while (currentIndex < values.size && UniversalComparator.compare(values[currentIndex], key) < 0) {
                    currentIndex++
                }
                indexPerLevel[currentLevel] = currentIndex
            }

            override fun next(): Any {
                val values = levelData[currentLevel]
                val currentIndex = indexPerLevel[currentLevel]
                if (currentIndex < values.size) {
                    indexPerLevel[currentLevel] = currentIndex + 1
                }
                return if (atEnd()) Unit else values[indexPerLevel[currentLevel]]
            }

            override fun key(): Any {
                val values = levelData[currentLevel]
                val currentIndex = indexPerLevel[currentLevel]
                return if (atEnd()) throw IllegalStateException("At end") else values[currentIndex]
            }

            override fun atEnd(): Boolean {
                val values = levelData[currentLevel]
                val currentIndex = indexPerLevel[currentLevel]
                return currentIndex >= values.size
            }

            override fun openLevel() {
                currentLevel++
                indexPerLevel[currentLevel] = 0
            }

            override fun closeLevel() {
                currentLevel--
            }

            override fun level(): Int {
                return currentLevel
            }

            override fun maxLevel(): Int {
                return levelData.size
            }

            override fun participatesInLevel(level: Int): Boolean {
                return level < levelData.size
            }
        }

        val index1 = MultiLevelIndex(listOf(
            (2 .. 12 step 2).toList(),
            (4 .. 40 step 4).toList()
        ))

        val index2 = MultiLevelIndex(listOf(
            (3 .. 12 step 3).toList(),
            (5 .. 40 step 5).toList()
        ))

        val indexes = listOf(index1, index2)

        val join = LeapfrogJoin(indexes, 2)
        val result = join.join()

        // Level 0 intersection: (2,4,6,8,10,12) ∩ (3,6,9,12) = (6, 12)
        // Level 1 intersection: (4,8,12,16,20,24,28,32,36,40) ∩ (5,10,15,20,25,30,35,40) = (20, 40)
        // Results: (6, 20), (6, 40), (12, 20), (12, 40)
        assertEquals(4, result.size)

        val expected = listOf(
            listOf(6, 20),
            listOf(6, 40),
            listOf(12, 20),
            listOf(12, 40)
        )
        assertEquals(expected, result)
    }
}