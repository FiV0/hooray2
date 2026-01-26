package org.hooray.algo

import kotlinx.collections.immutable.persistentListOf
import org.hooray.UniversalComparator
import org.hooray.iterator.AVLNotLeapfrogIndex
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

            override fun openLevel(prefix: List<Any>) {
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
        index.openLevel(emptyList())
        assertEquals(1, index.level())
        assertEquals(false, index.atEnd())
        assertEquals(2, index.key())

        // Open to level 2
        index.openLevel(emptyList())
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
        index.openLevel(emptyList())
        assertEquals(false, index.atEnd())
        assertEquals(2, index.key())
    }

    @Test
    fun `test createFromTuple resets position on closeLevel`() {
        val tuple = persistentListOf<Any>(1, 2)
        val index = LeapfrogIndex.createFromTuple(tuple)

        index.openLevel(emptyList())
        // Advance past value at level 1
        index.next()
        assertEquals(true, index.atEnd())

        // Close level - should reset level 0
        index.closeLevel()
        assertEquals(false, index.atEnd())
        assertEquals(1, index.key())
    }

    @Test
    fun `test LeapfrogJoin with FilterLeapfrogIndex excludes matching tuples`() {
        val evenIndex = LeapfrogIndex.createSingleLevel(listOf(2, 4, 6, 8, 10, 12))
        val divisibleByThreeIndex = LeapfrogIndex.createSingleLevel(listOf(3, 6, 9, 12))

        // Negative: exclude multiples of 4
        val multiplesOfFour = LeapfrogIndex.createSingleLevel(listOf(4, 8, 12))
        val notIndex = AVLNotLeapfrogIndex(listOf(multiplesOfFour), participationLevel = 0)

        val indexes = listOf(evenIndex, divisibleByThreeIndex)
        val join = LeapfrogJoin(indexes, 1, listOf(notIndex))
        val result = join.join()

        // Intersection of evens and divisible by 3: 6, 12
        // After excluding multiples of 4: only 6 remains (12 is excluded)
        assertEquals(1, result.size)
        assertEquals(listOf(listOf(6)), result)
    }

    @Test
    fun `test LeapfrogJoin with FilterLeapfrogIndex that excludes nothing`() {
        val evenIndex = LeapfrogIndex.createSingleLevel(listOf(2, 4, 6, 8, 10, 12))
        val divisibleByThreeIndex = LeapfrogIndex.createSingleLevel(listOf(3, 6, 9, 12))

        // Negative: disjoint set, excludes nothing
        val disjointNegative = LeapfrogIndex.createSingleLevel(listOf(100, 200, 300))
        val notIndex = AVLNotLeapfrogIndex(listOf(disjointNegative), participationLevel = 0)

        val indexes = listOf(evenIndex, divisibleByThreeIndex)
        val join = LeapfrogJoin(indexes, 1, listOf(notIndex))
        val result = join.join()

        // Intersection of evens and divisible by 3: 6, 12 - nothing excluded
        assertEquals(2, result.size)
        val expected = listOf(listOf(6), listOf(12))
        assertEquals(expected, result)
    }

    @Test
    fun `test LeapfrogJoin with FilterLeapfrogIndex that excludes all`() {
        val evenIndex = LeapfrogIndex.createSingleLevel(listOf(2, 4, 6, 8, 10, 12))
        val divisibleByThreeIndex = LeapfrogIndex.createSingleLevel(listOf(3, 6, 9, 12))

        // Negative: includes both 6 and 12
        val allMatching = LeapfrogIndex.createSingleLevel(listOf(6, 12))
        val notIndex = AVLNotLeapfrogIndex(listOf(allMatching), participationLevel = 0)

        val indexes = listOf(evenIndex, divisibleByThreeIndex)
        val join = LeapfrogJoin(indexes, 1, listOf(notIndex))
        val result = join.join()

        // All results are excluded
        assertEquals(0, result.size)
    }

    @Test
    fun `test createFromPrefixLeapfrogIndex participates in correct levels`() {
        val index = LeapfrogIndex.createFromPrefixLeapfrogIndex(
            participatingLevels = listOf(0, 2, 4),
            partialPrefix = persistentListOf("a", "b", "c")
        )

        assertEquals(true, index.participatesInLevel(0))
        assertEquals(false, index.participatesInLevel(1))
        assertEquals(true, index.participatesInLevel(2))
        assertEquals(false, index.participatesInLevel(3))
        assertEquals(true, index.participatesInLevel(4))
        assertEquals(false, index.participatesInLevel(5))
    }

    @Test
    fun `test createFromPrefixLeapfrogIndex basic iteration`() {
        val index = LeapfrogIndex.createFromPrefixLeapfrogIndex(
            participatingLevels = listOf(0, 1, 2),
            partialPrefix = persistentListOf(1, 2, 3)
        )

        assertEquals(0, index.level())
        assertEquals(3, index.maxLevel())
        assertEquals(false, index.atEnd())
        assertEquals(1, index.key())

        index.next()
        assertEquals(true, index.atEnd())
    }

    @Test
    fun `test createFromPrefixLeapfrogIndex seek behavior`() {
        val index = LeapfrogIndex.createFromPrefixLeapfrogIndex(
            participatingLevels = listOf(0),
            partialPrefix = persistentListOf(5)
        )

        // Seek to value less than prefix value - still at the value
        index.seek(3)
        assertEquals(false, index.atEnd())
        assertEquals(5, index.key())

        // Seek to exact value
        index.seek(5)
        assertEquals(false, index.atEnd())

        // Seek past the value - at end
        index.seek(6)
        assertEquals(true, index.atEnd())
    }

    @Test
    fun `test createFromPrefixLeapfrogIndex multi-level navigation with matching prefix`() {
        val index = LeapfrogIndex.createFromPrefixLeapfrogIndex(
            participatingLevels = listOf(0, 1, 2),
            partialPrefix = persistentListOf("a", "b", "c")
        )

        assertEquals(0, index.level())
        assertEquals("a", index.key())

        // Open level 1 with matching prefix
        index.openLevel(listOf("a"))
        assertEquals(1, index.level())
        assertEquals("b", index.key())

        // Open level 2 with matching prefix
        index.openLevel(listOf("a", "b"))
        assertEquals(2, index.level())
        assertEquals("c", index.key())

        // Close back down
        index.closeLevel()
        assertEquals(1, index.level())
        assertEquals("b", index.key())

        index.closeLevel()
        assertEquals(0, index.level())
        assertEquals("a", index.key())
    }

    @Test
    fun `test createFromPrefixLeapfrogIndex with non-matching prefix`() {
        val index = LeapfrogIndex.createFromPrefixLeapfrogIndex(
            participatingLevels = listOf(0, 1),
            partialPrefix = persistentListOf("x", "y")
        )

        assertEquals("x", index.key())

        // Open with non-matching prefix
        index.openLevel(listOf("z"))  // Doesn't match "x"
        assertEquals(true, index.atEnd())
    }

    @Test
    fun `test createFromPrefixLeapfrogIndex with non-consecutive participating levels`() {
        // Create indexes for a 3-level join
        val level0Index = LeapfrogIndex.createSingleLevel(listOf(1, 2), participatingLevel = 0)
        val level1Index = LeapfrogIndex.createSingleLevel(listOf(10, 20), participatingLevel = 1)
        val level2Index = LeapfrogIndex.createSingleLevel(listOf(100, 200), participatingLevel = 2)

        // Constraint that participates only at levels 0 and 2 (skips level 1)
        val constraintIndex = LeapfrogIndex.createFromPrefixLeapfrogIndex(
            participatingLevels = listOf(0, 2),
            partialPrefix = persistentListOf<Any>(1, 200)
        )

        // Verify participation
        assertEquals(true, constraintIndex.participatesInLevel(0))
        assertEquals(false, constraintIndex.participatesInLevel(1))
        assertEquals(true, constraintIndex.participatesInLevel(2))

        val join = LeapfrogJoin(listOf(level0Index, level1Index, level2Index, constraintIndex), 3)
        val result = join.join()

        // Only (1, *, 200) tuples
        val expected = listOf(
            listOf(1, 10, 200),
            listOf(1, 20, 200)
        )
        assertEquals(expected, result)
    }

    @Test
    fun `test createFromPrefixLeapfrogIndex in LeapfrogJoin`() {
        // Create a regular index for level 0
        val level0Index = LeapfrogIndex.createSingleLevel(listOf(1, 2, 3), participatingLevel = 0)

        // Create a regular index for level 1
        val level1Index = LeapfrogIndex.createSingleLevel(listOf(10, 20), participatingLevel = 1)

        // Create a regular index for level 2
        val level2Index = LeapfrogIndex.createSingleLevel(listOf(100, 200), participatingLevel = 2)

        // Create a constraint: level 0 must be 2, level 2 must be 100
        val constraintIndex = LeapfrogIndex.createFromPrefixLeapfrogIndex(
            participatingLevels = listOf(0, 2),
            partialPrefix = persistentListOf<Any>(2, 100)
        )

        val join = LeapfrogJoin(listOf(level0Index, level1Index, level2Index, constraintIndex), 3)
        val result = join.join()

        // Only tuples where level 0 = 2 and level 2 = 100
        val expected = listOf(
            listOf(2, 10, 100),
            listOf(2, 20, 100)
        )
        assertEquals(expected, result)
    }

    @Test
    fun `test createFromPrefixLeapfrogIndex reinit`() {
        val index = LeapfrogIndex.createFromPrefixLeapfrogIndex(
            participatingLevels = listOf(0, 1),
            partialPrefix = persistentListOf("a", "b")
        )

        index.openLevel(listOf("a"))
        assertEquals(1, index.level())

        index.reinit()
        assertEquals(0, index.level())
        assertEquals("a", index.key())
        assertEquals(false, index.atEnd())
    }
}