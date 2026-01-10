package org.hooray.algo

import kotlinx.collections.immutable.persistentListOf
import org.hooray.UniversalComparator
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class LeapfrogJoinTest {

    @Test
    fun `test empty indexes fails`() {
        val emptyIndexes = emptyList<LeapfrogIndex>()

        assertThrows<IllegalArgumentException> {
            LeapfrogJoin(emptyIndexes, 1)
        }
    }

    @Test
    fun `test two indexes with even and divisible by three`() {
        val evenIndex = LeapfrogIndex.createSingleLevel(listOf(2, 4, 6, 8, 10, 12))
        val divisibleByThreeIndex = LeapfrogIndex.createSingleLevel(listOf(3, 6, 9, 12))

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

    @Test
    fun `test createFromTuple basic iteration`() {
        val tuple = persistentListOf<Any>(1, 2, 3)
        val index = LeapfrogIndex.createFromTuple(tuple)

        assertEquals(0, index.level())
        assertEquals(3, index.maxLevel())
        assertEquals(false, index.atEnd())
        assertEquals(1, index.key())

        index.next()
        assertEquals(true, index.atEnd())
    }

    @Test
    fun `test createFromTuple seek behavior`() {
        val tuple = persistentListOf<Any>(5, 10, 15)
        val index = LeapfrogIndex.createFromTuple(tuple)

        // Seek to value less than tuple value - should still be at the value
        index.seek(3)
        assertEquals(false, index.atEnd())
        assertEquals(5, index.key())

        // Seek to exact value - should still be at the value
        index.seek(5)
        assertEquals(false, index.atEnd())
        assertEquals(5, index.key())

        // Seek past the value - should be at end
        index.seek(6)
        assertEquals(true, index.atEnd())
    }

    @Test
    fun `test createFromTuple multi-level navigation`() {
        val tuple = persistentListOf<Any>(1, 2, 3)
        val index = LeapfrogIndex.createFromTuple(tuple)

        // Level 0
        assertEquals(0, index.level())
        assertEquals(1, index.key())

        // Open to level 1
        index.openLevel()
        assertEquals(1, index.level())
        assertEquals(false, index.atEnd())
        assertEquals(2, index.key())

        // Open to level 2
        index.openLevel()
        assertEquals(2, index.level())
        assertEquals(false, index.atEnd())
        assertEquals(3, index.key())

        // Close back to level 1
        index.closeLevel()
        assertEquals(1, index.level())
        assertEquals(false, index.atEnd())
        assertEquals(2, index.key())

        // Close back to level 0
        index.closeLevel()
        assertEquals(0, index.level())
        assertEquals(false, index.atEnd())
        assertEquals(1, index.key())
    }

    @Test
    fun `test createFromTuple participatesInLevel`() {
        val tuple = persistentListOf<Any>(1, 2, 3)
        val index = LeapfrogIndex.createFromTuple(tuple)

        assertEquals(true, index.participatesInLevel(0))
        assertEquals(true, index.participatesInLevel(1))
        assertEquals(true, index.participatesInLevel(2))
        assertEquals(false, index.participatesInLevel(3))
    }

    @Test
    fun `test createFromTuple in join with matching tuple`() {
        val tuple = persistentListOf<Any>(6)
        val tupleIndex = LeapfrogIndex.createFromTuple(tuple)
        val rangeIndex = LeapfrogIndex.createSingleLevel(listOf(2, 4, 6, 8, 10))

        val join = LeapfrogJoin(listOf(tupleIndex, rangeIndex), 1)
        val result = join.join()

        assertEquals(1, result.size)
        assertEquals(listOf(listOf(6)), result)
    }

    @Test
    fun `test createFromTuple in join with non-matching tuple`() {
        val tuple = persistentListOf<Any>(5)
        val tupleIndex = LeapfrogIndex.createFromTuple(tuple)
        val rangeIndex = LeapfrogIndex.createSingleLevel(listOf(2, 4, 6, 8, 10))

        val join = LeapfrogJoin(listOf(tupleIndex, rangeIndex), 1)
        val result = join.join()

        assertEquals(0, result.size)
    }

    @Test
    fun `test createFromTuple resets position on openLevel`() {
        val tuple = persistentListOf<Any>(1, 2)
        val index = LeapfrogIndex.createFromTuple(tuple)

        // Advance past value at level 0
        index.next()
        assertEquals(true, index.atEnd())

        // Open level - should reset
        index.openLevel()
        assertEquals(false, index.atEnd())
        assertEquals(2, index.key())
    }

    @Test
    fun `test createFromTuple resets position on closeLevel`() {
        val tuple = persistentListOf<Any>(1, 2)
        val index = LeapfrogIndex.createFromTuple(tuple)

        index.openLevel()
        // Advance past value at level 1
        index.next()
        assertEquals(true, index.atEnd())

        // Close level - should reset level 0
        index.closeLevel()
        assertEquals(false, index.atEnd())
        assertEquals(1, index.key())
    }
}