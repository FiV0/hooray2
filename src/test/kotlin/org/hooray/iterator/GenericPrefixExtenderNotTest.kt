package org.hooray.iterator

import org.hooray.algo.ResultTuple
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class GenericPrefixExtenderNotTest {

    @Test
    fun `test NOT with even numbers on level 0 and divisible by 3 on level 1`() {
        // First child: filter even numbers on level 0
        val evenIndex = SealedIndex.SetIndex((1..10).filter { it % 2 == 0 }.toSet())
        val evenExtender = GenericPrefixExtender(evenIndex, listOf(0))

        // Second child: filter numbers divisible by 3 on level 1
        val divisibleByThreeIndex = SealedIndex.SetIndex((1..10).filter { it % 3 == 0 }.toSet())
        val divisibleByThreeExtender = GenericPrefixExtender(divisibleByThreeIndex, listOf(1))

        // NOT extender participates at level 1 (after level 0 is bound)
        val notExtender = GenericPrefixExtenderNot(listOf(evenExtender, divisibleByThreeExtender), 1)

        // Simulate the join process
        val resultTuples = mutableListOf<ResultTuple>()
        val proposedExtensions = (1..10).toList()
        for (i in 1..10) {
            val prefix = listOf(i)
            val filteredExtensions = notExtender.intersect(prefix, proposedExtensions)
            for (extension in filteredExtensions) {
                resultTuples.add(listOf(i, extension))
            }
        }

        // Expected: tuples where first element is NOT even OR second element is NOT divisible by 3
        // When level 0 is even (2,4,6,8,10): exclude divisible by 3 (3,6,9) -> keep 7 values each = 5 * 7 = 35
        // When level 0 is odd (1,3,5,7,9): keep all 10 values = 5 * 10 = 50
        // Total: 35 + 50 = 85
        assertEquals(85, resultTuples.size)

        // Build expected result
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
        assertEquals(expected.toSet(), resultTuples.toSet())
    }

    @Test
    fun `test NOT with single child containing both levels`() {
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

        // NOT extender participates at level 1 (after level 0 is bound)
        val notExtender = GenericPrefixExtenderNot(listOf(combinedExtender), 1)

        // Simulate the join process
        val resultTuples = mutableListOf<ResultTuple>()
        val proposedExtensions = (1..10).toList()
        for (i in 1..10) {
            val prefix = listOf(i)
            val filteredExtensions = notExtender.intersect(prefix, proposedExtensions)
            for (extension in filteredExtensions) {
                resultTuples.add(listOf(i, extension))
            }
        }

        // Expected: tuples where first element is NOT even OR second element is NOT divisible by 3
        // When level 0 is even (2,4,6,8,10): exclude divisible by 3 (3,6,9) -> keep 7 values each = 5 * 7 = 35
        // When level 0 is odd (1,3,5,7,9): keep all 10 values = 5 * 10 = 50
        // Total: 35 + 50 = 85
        assertEquals(85, resultTuples.size)

        // Build expected result
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
        assertEquals(expected.toSet(), resultTuples.toSet())
    }
}