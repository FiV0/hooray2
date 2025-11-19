package org.hooray.iterator

import org.hooray.algo.ResultTuple
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class GenericResultTupleRemoverTest {

    @Test
    fun `test filter with even numbers on level 0 and divisible by 3 on level 1`() {
        // Create input tuples: cross product of first 10 numbers (1..10) on two levels
        val inputTuples = mutableListOf<ResultTuple>()
        for (i in 1..10) {
            for (j in 1..10) {
                inputTuples.add(listOf(i, j))
            }
        }

        // First child: filter even numbers on level 0
        val evenIndex = SealedIndex.SetIndex((1..10).filter { it % 2 == 0 }.toSet())
        val evenExtender = GenericPrefixExtender(evenIndex, listOf(0))

        // Second child: filter numbers divisible by 3 on level 1
        val divisibleByThreeIndex = SealedIndex.SetIndex((1..10).filter { it % 3 == 0 }.toSet())
        val divisibleByThreeExtender = GenericPrefixExtender(divisibleByThreeIndex, listOf(1))

        val remover = GenericResultTupleRemover(listOf(evenExtender, divisibleByThreeExtender))
        val result = remover.filter(inputTuples)

        // Expected: tuples where first element is NOT even OR second element is NOT divisible by 3
        // (inverted logic - keeps tuples where join is empty)
        // Total input: 100 tuples (10x10)
        // Tuples that satisfy BOTH constraints: 5 even * 3 divisible by 3 = 15
        // Tuples that DON'T satisfy both constraints: 100 - 15 = 85
        assertEquals(85, result.size)

        // Build expected result: all tuples except those with even first AND divisible-by-3 second
        val expected = mutableListOf<ResultTuple>()
        for (i in 1..10) {
            for (j in 1..10) {
                val isEven = i % 2 == 0
                val isDivisibleByThree = j % 3 == 0
                // Keep if NOT (both even AND divisible by 3)
                if (!(isEven && isDivisibleByThree)) {
                    expected.add(listOf(i, j))
                }
            }
        }
        assertEquals(expected, result)
    }

    @Test
    fun `test filter with single child containing both levels`() {
        // Create input tuples: cross product of first 10 numbers (1..10) on two levels
        val inputTuples = mutableListOf<ResultTuple>()
        for (i in 1..10) {
            for (j in 1..10) {
                inputTuples.add(listOf(i, j))
            }
        }

        // Single child: nested map structure
        // Level 0 (outer): even numbers -> Level 1 (inner): numbers divisible by 3
        val evenNumbers = (1..10).filter { it % 2 == 0 }
        val divisibleByThree = (1..10).filter { it % 3 == 0 }.toSet()

        val nestedMap = mutableMapOf<Any, Any>()
        for (evenNum in evenNumbers) {
            nestedMap[evenNum] = divisibleByThree
        }

        val combinedIndex = SealedIndex.MapIndex(nestedMap)
        val combinedExtender = GenericPrefixExtender(combinedIndex, listOf(0, 1))

        val remover = GenericResultTupleRemover(listOf(combinedExtender))
        val result = remover.filter(inputTuples)

        // Expected: tuples where first element is NOT even OR second element is NOT divisible by 3
        // (inverted logic - keeps tuples where join is empty)
        // Total input: 100 tuples (10x10)
        // Tuples that satisfy BOTH constraints: 5 even * 3 divisible by 3 = 15
        // Tuples that DON'T satisfy both constraints: 100 - 15 = 85
        assertEquals(85, result.size)

        // Build expected result: all tuples except those with even first AND divisible-by-3 second
        val expected = mutableListOf<ResultTuple>()
        for (i in 1..10) {
            for (j in 1..10) {
                val isEven = i % 2 == 0
                val isDivisibleByThree = j % 3 == 0
                // Keep if NOT (both even AND divisible by 3)
                if (!(isEven && isDivisibleByThree)) {
                    expected.add(listOf(i, j))
                }
            }
        }
        assertEquals(expected, result)
    }
}
