package org.hooray.iterator

import org.hooray.UniversalComparator
import org.hooray.algo.LeapfrogIndex
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class AVLAndLeapfrogIndexTest {

    @Test
    fun `test empty children list fails`() {
        assertThrows<IllegalArgumentException> {
            AVLAndLeapfrogIndex(emptyList())
        }
    }

    @Test
    fun `test children with different maxLevels fails`() {
        val index1 = LeapfrogIndex.createSingleLevel(listOf(1, 2, 3), maxLevels = 1)
        val index2 = LeapfrogIndex.createSingleLevel(listOf(4, 5, 6), maxLevels = 2)

        assertThrows<IllegalArgumentException> {
            AVLAndLeapfrogIndex(listOf(index1, index2))
        }
    }

    @Test
    fun `test single child behaves like the child itself`() {
        val child = LeapfrogIndex.createSingleLevel(listOf(2, 4, 6, 8))
        val andIndex = AVLAndLeapfrogIndex(listOf(child))

        val results = mutableListOf<Any>()
        while (!andIndex.atEnd()) {
            results.add(andIndex.key())
            andIndex.next()
        }

        assertEquals(listOf(2, 4, 6, 8), results)
    }

    @Test
    fun `test AND with two disjoint sets returns empty`() {
        val evens = LeapfrogIndex.createSingleLevel(listOf(2, 4, 6, 8))
        val odds = LeapfrogIndex.createSingleLevel(listOf(1, 3, 5, 7, 9))
        val andIndex = AVLAndLeapfrogIndex(listOf(evens, odds))

        assertEquals(true, andIndex.atEnd())
    }

    @Test
    fun `test AND with overlapping sets returns intersection`() {
        val multiplesOfTwo = LeapfrogIndex.createSingleLevel(listOf(2, 4, 6, 8, 10, 12))
        val multiplesOfThree = LeapfrogIndex.createSingleLevel(listOf(3, 6, 9, 12))
        val andIndex = AVLAndLeapfrogIndex(listOf(multiplesOfTwo, multiplesOfThree))

        val results = mutableListOf<Any>()
        while (!andIndex.atEnd()) {
            results.add(andIndex.key())
            andIndex.next()
        }

        assertEquals(listOf(6, 12), results)
    }

    @Test
    fun `test AND with three children`() {
        val multiplesOfTwo = LeapfrogIndex.createSingleLevel(listOf(2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30))
        val multiplesOfThree = LeapfrogIndex.createSingleLevel(listOf(3, 6, 9, 12, 15, 18, 21, 24, 27, 30))
        val multiplesOfFive = LeapfrogIndex.createSingleLevel(listOf(5, 10, 15, 20, 25, 30))
        val andIndex = AVLAndLeapfrogIndex(listOf(multiplesOfTwo, multiplesOfThree, multiplesOfFive))

        val results = mutableListOf<Any>()
        while (!andIndex.atEnd()) {
            results.add(andIndex.key())
            andIndex.next()
        }

        // Only 30 is divisible by 2, 3, and 5
        assertEquals(listOf(30), results)
    }

    @Test
    fun `test seek on AND index`() {
        val set1 = LeapfrogIndex.createSingleLevel(listOf(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
        val set2 = LeapfrogIndex.createSingleLevel(listOf(2, 4, 6, 8, 10))
        val andIndex = AVLAndLeapfrogIndex(listOf(set1, set2))

        andIndex.seek(5)

        val results = mutableListOf<Any>()
        while (!andIndex.atEnd()) {
            results.add(andIndex.key())
            andIndex.next()
        }

        assertEquals(listOf(6, 8, 10), results)
    }

    @Test
    fun `test AND with one child having subset of other`() {
        val allNumbers = LeapfrogIndex.createSingleLevel(listOf(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
        val evens = LeapfrogIndex.createSingleLevel(listOf(2, 4, 6, 8, 10))
        val andIndex = AVLAndLeapfrogIndex(listOf(allNumbers, evens))

        val results = mutableListOf<Any>()
        while (!andIndex.atEnd()) {
            results.add(andIndex.key())
            andIndex.next()
        }

        assertEquals(listOf(2, 4, 6, 8, 10), results)
    }

    @Test
    fun `test AND with identical sets`() {
        val set1 = LeapfrogIndex.createSingleLevel(listOf(1, 2, 3, 4, 5))
        val set2 = LeapfrogIndex.createSingleLevel(listOf(1, 2, 3, 4, 5))
        val andIndex = AVLAndLeapfrogIndex(listOf(set1, set2))

        val results = mutableListOf<Any>()
        while (!andIndex.atEnd()) {
            results.add(andIndex.key())
            andIndex.next()
        }

        assertEquals(listOf(1, 2, 3, 4, 5), results)
    }

    @Test
    fun `test AND with multi-level indexes`() {
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

        val index1 = MultiLevelIndex(listOf(
            listOf(1, 2, 3, 4, 5),
            listOf(10, 20, 30)
        ))

        val index2 = MultiLevelIndex(listOf(
            listOf(2, 3, 4, 6),
            listOf(15, 20, 25, 30)
        ))

        val andIndex = AVLAndLeapfrogIndex(listOf(index1, index2))

        assertEquals(0, andIndex.level())
        assertEquals(2, andIndex.maxLevel())
        assertEquals(true, andIndex.participatesInLevel(0))
        assertEquals(true, andIndex.participatesInLevel(1))

        val level0Results = mutableListOf<Any>()
        while (!andIndex.atEnd()) {
            level0Results.add(andIndex.key())
            andIndex.next()
        }

        assertEquals(listOf(2, 3, 4), level0Results)
    }

    @Test
    fun `test openLevel and closeLevel on AND index`() {
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

        val index1 = MultiLevelIndex(listOf(
            listOf(1, 2, 3),
            listOf(10, 20, 30)
        ))

        val index2 = MultiLevelIndex(listOf(
            listOf(2, 3, 4),
            listOf(20, 30, 40)
        ))

        val andIndex = AVLAndLeapfrogIndex(listOf(index1, index2))

        assertEquals(0, andIndex.level())

        andIndex.openLevel()
        assertEquals(1, andIndex.level())

        val level1Results = mutableListOf<Any>()
        while (!andIndex.atEnd()) {
            level1Results.add(andIndex.key())
            andIndex.next()
        }

        assertEquals(listOf(20, 30), level1Results)

        andIndex.closeLevel()
        assertEquals(0, andIndex.level())
    }

    @Test
    fun `test seek past all elements results in atEnd`() {
        val set1 = LeapfrogIndex.createSingleLevel(listOf(1, 2, 3))
        val set2 = LeapfrogIndex.createSingleLevel(listOf(2, 3, 4))
        val andIndex = AVLAndLeapfrogIndex(listOf(set1, set2))

        andIndex.seek(100)

        assertEquals(true, andIndex.atEnd())
    }
}
