package org.hooray.iterator

import org.hooray.UniversalComparator
import org.hooray.algo.LeapfrogIndex
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class AVLOrLeapfrogIndexTest {

    @Test
    fun `test empty children list fails`() {
        assertThrows<IllegalArgumentException> {
            AVLOrLeapfrogIndex(emptyList())
        }
    }

    @Test
    fun `test children with different maxLevels fails`() {
        val index1 = LeapfrogIndex.createSingleLevel(listOf(1, 2, 3), maxLevels = 1)
        val index2 = LeapfrogIndex.createSingleLevel(listOf(4, 5, 6), maxLevels = 2)

        assertThrows<IllegalArgumentException> {
            AVLOrLeapfrogIndex(listOf(index1, index2))
        }
    }

    @Test
    fun `test single child behaves like the child itself`() {
        val child = LeapfrogIndex.createSingleLevel(listOf(2, 4, 6, 8))
        val orIndex = AVLOrLeapfrogIndex(listOf(child))

        val results = mutableListOf<Any>()
        while (!orIndex.atEnd()) {
            results.add(orIndex.key())
            orIndex.next()
        }

        assertEquals(listOf(2, 4, 6, 8), results)
    }

    @Test
    fun `test OR with two disjoint sets`() {
        val evens = LeapfrogIndex.createSingleLevel(listOf(2, 4, 6, 8))
        val odds = LeapfrogIndex.createSingleLevel(listOf(1, 3, 5, 7, 9))
        val orIndex = AVLOrLeapfrogIndex(listOf(evens, odds))

        val results = mutableListOf<Any>()
        while (!orIndex.atEnd()) {
            results.add(orIndex.key())
            orIndex.next()
        }

        assertEquals(listOf(1, 2, 3, 4, 5, 6, 7, 8, 9), results)
    }

    @Test
    fun `test OR with overlapping sets removes duplicates`() {
        val multiplesOfTwo = LeapfrogIndex.createSingleLevel(listOf(2, 4, 6, 8, 10, 12))
        val multiplesOfThree = LeapfrogIndex.createSingleLevel(listOf(3, 6, 9, 12))
        val orIndex = AVLOrLeapfrogIndex(listOf(multiplesOfTwo, multiplesOfThree))

        val results = mutableListOf<Any>()
        while (!orIndex.atEnd()) {
            results.add(orIndex.key())
            orIndex.next()
        }

        assertEquals(listOf(2, 3, 4, 6, 8, 9, 10, 12), results)
    }

    @Test
    fun `test OR with three children`() {
        val multiplesOfTwo = LeapfrogIndex.createSingleLevel(listOf(2, 4, 6, 8, 10))
        val multiplesOfThree = LeapfrogIndex.createSingleLevel(listOf(3, 6, 9))
        val multiplesOfFive = LeapfrogIndex.createSingleLevel(listOf(5, 10))
        val orIndex = AVLOrLeapfrogIndex(listOf(multiplesOfTwo, multiplesOfThree, multiplesOfFive))

        val results = mutableListOf<Any>()
        while (!orIndex.atEnd()) {
            results.add(orIndex.key())
            orIndex.next()
        }

        assertEquals(listOf(2, 3, 4, 5, 6, 8, 9, 10), results)
    }

    @Test
    fun `test seek on OR index`() {
        val multiplesOfTwo = LeapfrogIndex.createSingleLevel(listOf(2, 4, 6, 8, 10, 12))
        val multiplesOfThree = LeapfrogIndex.createSingleLevel(listOf(3, 6, 9, 12))
        val orIndex = AVLOrLeapfrogIndex(listOf(multiplesOfTwo, multiplesOfThree))

        orIndex.seek(7)

        val results = mutableListOf<Any>()
        while (!orIndex.atEnd()) {
            results.add(orIndex.key())
            orIndex.next()
        }

        assertEquals(listOf(8, 9, 10, 12), results)
    }

    @Test
    fun `test OR with one exhausted child continues with others`() {
        val shortList = LeapfrogIndex.createSingleLevel(listOf(1, 2))
        val longList = LeapfrogIndex.createSingleLevel(listOf(3, 4, 5, 6, 7, 8))
        val orIndex = AVLOrLeapfrogIndex(listOf(shortList, longList))

        val results = mutableListOf<Any>()
        while (!orIndex.atEnd()) {
            results.add(orIndex.key())
            orIndex.next()
        }

        assertEquals(listOf(1, 2, 3, 4, 5, 6, 7, 8), results)
    }

    @Test
    fun `test OR with multi-level indexes`() {
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

            override fun reinit() {
                currentLevel = 0
                for (i in indexPerLevel.indices) {
                    indexPerLevel[i] = 0
                }
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
            listOf(1, 3, 5),
            listOf(10, 20, 30)
        ))

        val index2 = MultiLevelIndex(listOf(
            listOf(2, 4, 6),
            listOf(15, 25, 35)
        ))

        val orIndex = AVLOrLeapfrogIndex(listOf(index1, index2))

        assertEquals(0, orIndex.level())
        assertEquals(2, orIndex.maxLevel())
        assertEquals(true, orIndex.participatesInLevel(0))
        assertEquals(true, orIndex.participatesInLevel(1))

        val level0Results = mutableListOf<Any>()
        while (!orIndex.atEnd()) {
            level0Results.add(orIndex.key())
            orIndex.next()
        }

        assertEquals(listOf(1, 2, 3, 4, 5, 6), level0Results)
    }

    @Test
    fun `test openLevel and closeLevel on OR index`() {
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

            override fun reinit() {
                currentLevel = 0
                for (i in indexPerLevel.indices) {
                    indexPerLevel[i] = 0
                }
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
            listOf(1, 2),
            listOf(10, 20)
        ))

        val index2 = MultiLevelIndex(listOf(
            listOf(3, 4),
            listOf(30, 40)
        ))

        val orIndex = AVLOrLeapfrogIndex(listOf(index1, index2))

        assertEquals(0, orIndex.level())

        orIndex.openLevel()
        assertEquals(1, orIndex.level())

        val level1Results = mutableListOf<Any>()
        while (!orIndex.atEnd()) {
            level1Results.add(orIndex.key())
            orIndex.next()
        }

        assertEquals(listOf(10, 20, 30, 40), level1Results)

        orIndex.closeLevel()
        assertEquals(0, orIndex.level())
    }
}
