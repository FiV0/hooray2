package org.hooray.iterator

import kotlinx.collections.immutable.persistentListOf
import org.hooray.algo.LeapfrogIndex
import org.hooray.algo.ResultTuple
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class AVLNotLeapfrogIndexTest {

    @Test
    fun `test empty negatives list returns true for any tuple`() {
        val notIndex = AVLNotLeapfrogIndex(emptyList(), participationLevel = 0)

        assertEquals(true, notIndex.checkNegation(persistentListOf(1)))
        assertEquals(true, notIndex.checkNegation(persistentListOf(2)))
        assertEquals(true, notIndex.checkNegation(persistentListOf(3)))
    }

    @Test
    fun `test single negative excludes matching tuples`() {
        val negative = LeapfrogIndex.createSingleLevel(listOf(2, 4, 6, 8, 10))
        val notIndex = AVLNotLeapfrogIndex(listOf(negative), participationLevel = 0)

        // Odds should return true (not excluded)
        assertEquals(true, notIndex.checkNegation(persistentListOf(1)))
        assertEquals(true, notIndex.checkNegation(persistentListOf(3)))
        assertEquals(true, notIndex.checkNegation(persistentListOf(5)))

        // Evens should return false (excluded)
        assertEquals(false, notIndex.checkNegation(persistentListOf(2)))
        assertEquals(false, notIndex.checkNegation(persistentListOf(4)))
        assertEquals(false, notIndex.checkNegation(persistentListOf(6)))
    }

    @Test
    fun `test disjoint sets returns true`() {
        val negative = LeapfrogIndex.createSingleLevel(listOf(100, 200, 300))
        val notIndex = AVLNotLeapfrogIndex(listOf(negative), participationLevel = 0)

        assertEquals(true, notIndex.checkNegation(persistentListOf(1)))
        assertEquals(true, notIndex.checkNegation(persistentListOf(2)))
        assertEquals(true, notIndex.checkNegation(persistentListOf(3)))
    }

    @Test
    fun `test AND semantics with multiple negatives`() {
        // With multiple negatives: exclude only if tuple matches ALL negatives
        val multiplesOfTwo = LeapfrogIndex.createSingleLevel(listOf(2, 4, 6, 8, 10, 12))
        val multiplesOfThree = LeapfrogIndex.createSingleLevel(listOf(3, 6, 9, 12))
        val notIndex = AVLNotLeapfrogIndex(listOf(multiplesOfTwo, multiplesOfThree), participationLevel = 0)

        // Multiples of 6 (both 2 AND 3) should be excluded
        assertEquals(false, notIndex.checkNegation(persistentListOf(6)))
        assertEquals(false, notIndex.checkNegation(persistentListOf(12)))

        // Numbers that are only multiples of 2 should be kept
        assertEquals(true, notIndex.checkNegation(persistentListOf(2)))
        assertEquals(true, notIndex.checkNegation(persistentListOf(4)))
        assertEquals(true, notIndex.checkNegation(persistentListOf(8)))

        // Numbers that are only multiples of 3 should be kept
        assertEquals(true, notIndex.checkNegation(persistentListOf(3)))
        assertEquals(true, notIndex.checkNegation(persistentListOf(9)))

        // Numbers that are neither should be kept
        assertEquals(true, notIndex.checkNegation(persistentListOf(1)))
        assertEquals(true, notIndex.checkNegation(persistentListOf(5)))
    }

    @Test
    fun `test AND semantics excludes nothing if any negative is disjoint`() {
        val negativeOne = LeapfrogIndex.createSingleLevel(listOf(2, 4))
        val negativeTwo = LeapfrogIndex.createSingleLevel(listOf(100, 200))
        val notIndex = AVLNotLeapfrogIndex(listOf(negativeOne, negativeTwo), participationLevel = 0)

        // Nothing excluded because no tuple exists in ALL negatives
        assertEquals(true, notIndex.checkNegation(persistentListOf(1)))
        assertEquals(true, notIndex.checkNegation(persistentListOf(2)))
        assertEquals(true, notIndex.checkNegation(persistentListOf(3)))
        assertEquals(true, notIndex.checkNegation(persistentListOf(4)))
        assertEquals(true, notIndex.checkNegation(persistentListOf(5)))
    }

    @Test
    fun `test participatesInLevel`() {
        val notIndex = AVLNotLeapfrogIndex(emptyList(), participationLevel = 1)

        assertEquals(false, notIndex.participatesInLevel(0))
        assertEquals(true, notIndex.participatesInLevel(1))
        assertEquals(false, notIndex.participatesInLevel(2))
    }

    @Test
    fun `test multiple calls work correctly with reinit`() {
        val negative = LeapfrogIndex.createSingleLevel(listOf(2, 4, 6))
        val notIndex = AVLNotLeapfrogIndex(listOf(negative), participationLevel = 0)

        // First call
        assertEquals(false, notIndex.checkNegation(persistentListOf(2)))
        // Second call should still work correctly
        assertEquals(true, notIndex.checkNegation(persistentListOf(3)))
        // Third call
        assertEquals(false, notIndex.checkNegation(persistentListOf(4)))
    }
}
