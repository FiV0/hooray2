package org.hooray.iterator

import org.hooray.UniversalComparator
import org.hooray.algo.LeapfrogIndex
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class AVLNotLeapfrogIndexTest {

    @Test
    fun `test negative with different maxLevels fails`() {
        val positive = LeapfrogIndex.createSingleLevel(listOf(1, 2, 3), maxLevels = 1)
        val negative = LeapfrogIndex.createSingleLevel(listOf(2, 3), maxLevels = 2)

        assertThrows<IllegalArgumentException> {
            AVLNotLeapfrogIndex(positive, listOf(negative), participationLevel = 0)
        }
    }

    @Test
    fun `test empty negative list behaves like positive index`() {
        val positive = LeapfrogIndex.createSingleLevel(listOf(1, 2, 3, 4, 5))
        val notIndex = AVLNotLeapfrogIndex(positive, emptyList(), participationLevel = 0)

        val results = mutableListOf<Any>()
        while (!notIndex.atEnd()) {
            results.add(notIndex.key())
            notIndex.next()
        }

        assertEquals(listOf(1, 2, 3, 4, 5), results)
    }

    @Test
    fun `test single negative child excludes matching elements`() {
        val positive = LeapfrogIndex.createSingleLevel(listOf(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
        val negative = LeapfrogIndex.createSingleLevel(listOf(2, 4, 6, 8, 10)) // evens
        val notIndex = AVLNotLeapfrogIndex(positive, listOf(negative), participationLevel = 0)

        val results = mutableListOf<Any>()
        while (!notIndex.atEnd()) {
            results.add(notIndex.key())
            notIndex.next()
        }

        // Should return odds only (positive minus negative)
        assertEquals(listOf(1, 3, 5, 7, 9), results)
    }

    @Test
    fun `test disjoint sets returns all positive elements`() {
        val positive = LeapfrogIndex.createSingleLevel(listOf(1, 3, 5, 7, 9)) // odds
        val negative = LeapfrogIndex.createSingleLevel(listOf(2, 4, 6, 8, 10)) // evens
        val notIndex = AVLNotLeapfrogIndex(positive, listOf(negative), participationLevel = 0)

        val results = mutableListOf<Any>()
        while (!notIndex.atEnd()) {
            results.add(notIndex.key())
            notIndex.next()
        }

        // No overlap, so all positive elements returned
        assertEquals(listOf(1, 3, 5, 7, 9), results)
    }

    @Test
    fun `test complete overlap returns empty`() {
        val positive = LeapfrogIndex.createSingleLevel(listOf(2, 4, 6))
        val negative = LeapfrogIndex.createSingleLevel(listOf(2, 4, 6, 8, 10))
        val notIndex = AVLNotLeapfrogIndex(positive, listOf(negative), participationLevel = 0)

        assertEquals(true, notIndex.atEnd())
    }

    @Test
    fun `test AND semantics with multiple negative children`() {
        // With AND semantics: exclude only if key exists in ALL negative children
        val positive = LeapfrogIndex.createSingleLevel(listOf(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12))
        val multiplesOfTwo = LeapfrogIndex.createSingleLevel(listOf(2, 4, 6, 8, 10, 12))
        val multiplesOfThree = LeapfrogIndex.createSingleLevel(listOf(3, 6, 9, 12))
        val notIndex = AVLNotLeapfrogIndex(positive, listOf(multiplesOfTwo, multiplesOfThree), participationLevel = 0)

        val results = mutableListOf<Any>()
        while (!notIndex.atEnd()) {
            results.add(notIndex.key())
            notIndex.next()
        }

        // Exclude only multiples of 6 (both 2 AND 3): 6, 12
        // Keep: 1, 2, 3, 4, 5, 7, 8, 9, 10, 11
        assertEquals(listOf(1, 2, 3, 4, 5, 7, 8, 9, 10, 11), results)
    }

    @Test
    fun `test AND semantics excludes nothing if any negative is disjoint`() {
        val positive = LeapfrogIndex.createSingleLevel(listOf(1, 2, 3, 4, 5))
        val negativeOne = LeapfrogIndex.createSingleLevel(listOf(2, 4)) // overlaps
        val negativeTwo = LeapfrogIndex.createSingleLevel(listOf(100, 200)) // disjoint
        val notIndex = AVLNotLeapfrogIndex(positive, listOf(negativeOne, negativeTwo), participationLevel = 0)

        val results = mutableListOf<Any>()
        while (!notIndex.atEnd()) {
            results.add(notIndex.key())
            notIndex.next()
        }

        // Nothing is excluded because no key exists in ALL negative children
        assertEquals(listOf(1, 2, 3, 4, 5), results)
    }

    @Test
    fun `test seek on NOT index`() {
        val positive = LeapfrogIndex.createSingleLevel(listOf(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
        val negative = LeapfrogIndex.createSingleLevel(listOf(2, 4, 6, 8, 10))
        val notIndex = AVLNotLeapfrogIndex(positive, listOf(negative), participationLevel = 0)

        notIndex.seek(4)

        val results = mutableListOf<Any>()
        while (!notIndex.atEnd()) {
            results.add(notIndex.key())
            notIndex.next()
        }

        // After seek(4), should get odd numbers >= 4: 5, 7, 9
        assertEquals(listOf(5, 7, 9), results)
    }

    @Test
    fun `test seek to excluded value advances to next valid`() {
        val positive = LeapfrogIndex.createSingleLevel(listOf(1, 2, 3, 4, 5))
        val negative = LeapfrogIndex.createSingleLevel(listOf(2, 3, 4))
        val notIndex = AVLNotLeapfrogIndex(positive, listOf(negative), participationLevel = 0)

        notIndex.seek(2) // 2 is excluded, should advance to 5

        assertEquals(5, notIndex.key())
    }

    @Test
    fun `test seek past all elements results in atEnd`() {
        val positive = LeapfrogIndex.createSingleLevel(listOf(1, 2, 3))
        val negative = LeapfrogIndex.createSingleLevel(listOf(2))
        val notIndex = AVLNotLeapfrogIndex(positive, listOf(negative), participationLevel = 0)

        notIndex.seek(100)

        assertEquals(true, notIndex.atEnd())
    }

    @Test
    fun `test multi-level with openLevel and closeLevel`() {
        class MultiLevelIndex(
            private val levelData: List<List<Int>>
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

        val positive = MultiLevelIndex(listOf(
            listOf(1, 2, 3, 4, 5),
            listOf(10, 20, 30, 40, 50)
        ))

        val negative = MultiLevelIndex(listOf(
            listOf(2, 4),
            listOf(20, 40)
        ))

        val notIndex = AVLNotLeapfrogIndex(positive, listOf(negative), participationLevel = 0)

        assertEquals(0, notIndex.level())
        assertEquals(2, notIndex.maxLevel())

        // At level 0, should exclude 2 and 4
        val level0Results = mutableListOf<Any>()
        while (!notIndex.atEnd()) {
            level0Results.add(notIndex.key())
            notIndex.next()
        }

        assertEquals(listOf(1, 3, 5), level0Results)
    }

    @Test
    fun `test participatesInLevel returns true at participation level`() {
        val positive = LeapfrogIndex.createSingleLevel(listOf(1, 2, 3), maxLevels = 3)
        val negative = LeapfrogIndex.createSingleLevel(listOf(2), maxLevels = 3)
        val notIndex = AVLNotLeapfrogIndex(positive, listOf(negative), participationLevel = 1)

        // At participation level 1, should return true
        assertEquals(true, notIndex.participatesInLevel(1))

        // At other levels, delegates to positive
        assertEquals(true, notIndex.participatesInLevel(0))
        assertEquals(true, notIndex.participatesInLevel(2))
    }

    @Test
    fun `test NOT with positive being empty`() {
        val positive = LeapfrogIndex.createSingleLevel(emptyList())
        val negative = LeapfrogIndex.createSingleLevel(listOf(1, 2, 3))
        val notIndex = AVLNotLeapfrogIndex(positive, listOf(negative), participationLevel = 0)

        assertEquals(true, notIndex.atEnd())
    }
}
