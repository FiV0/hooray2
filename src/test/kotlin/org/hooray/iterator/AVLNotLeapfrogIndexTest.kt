package org.hooray.iterator

import kotlinx.collections.immutable.persistentListOf
import org.hooray.algo.LeapfrogIndex
import org.hooray.algo.ResultTuple
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class AVLNotLeapfrogIndexTest {

    @Test
    fun `test empty negatives list returns all tuples`() {
        val notIndex = AVLNotLeapfrogIndex(emptyList(), participationLevel = 0)

        val tuples: List<ResultTuple> = listOf(
            persistentListOf(1),
            persistentListOf(2),
            persistentListOf(3)
        )

        val result = notIndex.checkNegation(tuples)

        assertEquals(tuples, result)
    }

    @Test
    fun `test single negative excludes matching tuples`() {
        val negative = LeapfrogIndex.createSingleLevel(listOf(2, 4, 6, 8, 10))
        val notIndex = AVLNotLeapfrogIndex(listOf(negative), participationLevel = 0)

        val tuples: List<ResultTuple> = (1..10).map { persistentListOf<Any>(it) }

        val result = notIndex.checkNegation(tuples)

        // Should keep odds only (tuples NOT in negative)
        val expected: List<ResultTuple> = listOf(1, 3, 5, 7, 9).map { persistentListOf<Any>(it) }
        assertEquals(expected, result)
    }

    @Test
    fun `test disjoint sets returns all tuples`() {
        val negative = LeapfrogIndex.createSingleLevel(listOf(100, 200, 300))
        val notIndex = AVLNotLeapfrogIndex(listOf(negative), participationLevel = 0)

        val tuples: List<ResultTuple> = listOf(
            persistentListOf(1),
            persistentListOf(2),
            persistentListOf(3)
        )

        val result = notIndex.checkNegation(tuples)

        assertEquals(tuples, result)
    }

    @Test
    fun `test complete overlap returns empty`() {
        val negative = LeapfrogIndex.createSingleLevel(listOf(1, 2, 3, 4, 5))
        val notIndex = AVLNotLeapfrogIndex(listOf(negative), participationLevel = 0)

        val tuples: List<ResultTuple> = listOf(
            persistentListOf(2),
            persistentListOf(4)
        )

        val result = notIndex.checkNegation(tuples)

        assertEquals(emptyList<ResultTuple>(), result)
    }

    @Test
    fun `test AND semantics with multiple negatives`() {
        // With multiple negatives: exclude only if tuple matches ALL negatives
        val multiplesOfTwo = LeapfrogIndex.createSingleLevel(listOf(2, 4, 6, 8, 10, 12))
        val multiplesOfThree = LeapfrogIndex.createSingleLevel(listOf(3, 6, 9, 12))
        val notIndex = AVLNotLeapfrogIndex(listOf(multiplesOfTwo, multiplesOfThree), participationLevel = 0)

        val tuples: List<ResultTuple> = (1..12).map { persistentListOf<Any>(it) }

        val result = notIndex.checkNegation(tuples)

        // Exclude only multiples of 6 (both 2 AND 3): 6, 12
        // Keep: 1, 2, 3, 4, 5, 7, 8, 9, 10, 11
        val expected: List<ResultTuple> = listOf(1, 2, 3, 4, 5, 7, 8, 9, 10, 11).map { persistentListOf<Any>(it) }
        assertEquals(expected, result)
    }

    @Test
    fun `test AND semantics excludes nothing if any negative is disjoint`() {
        val negativeOne = LeapfrogIndex.createSingleLevel(listOf(2, 4))
        val negativeTwo = LeapfrogIndex.createSingleLevel(listOf(100, 200))
        val notIndex = AVLNotLeapfrogIndex(listOf(negativeOne, negativeTwo), participationLevel = 0)

        val tuples: List<ResultTuple> = (1..5).map { persistentListOf<Any>(it) }

        val result = notIndex.checkNegation(tuples)

        // Nothing excluded because no tuple exists in ALL negatives
        assertEquals(tuples, result)
    }

    @Test
    fun `test participatesInLevel`() {
        val notIndex = AVLNotLeapfrogIndex(emptyList(), participationLevel = 1)

        assertEquals(false, notIndex.participatesInLevel(0))
        assertEquals(true, notIndex.participatesInLevel(1))
        assertEquals(false, notIndex.participatesInLevel(2))
    }

    @Test
    fun `test empty tuples list returns empty`() {
        val negative = LeapfrogIndex.createSingleLevel(listOf(1, 2, 3))
        val notIndex = AVLNotLeapfrogIndex(listOf(negative), participationLevel = 0)

        val result = notIndex.checkNegation(emptyList())

        assertEquals(emptyList<ResultTuple>(), result)
    }
}
