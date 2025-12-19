package org.hooray.iterator

import org.hooray.algo.ResultTuple
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class GenericPredicatePrefixExtenderTest {

    @Test
    fun `test unary predicate - filter even numbers`() {
        // Predicate1: keep only even numbers
        val isEven: Predicate1<Any> = { x -> (x as Int) % 2 == 0 }
        val predicateExtender = GenericPredicatePrefixExtender(listOf(0), isEven)

        // Test participatesInLevel
        assertEquals(true, predicateExtender.participatesInLevel(0))
        assertEquals(false, predicateExtender.participatesInLevel(1))

        // Test count returns MAX_VALUE
        assertEquals(Int.MAX_VALUE, predicateExtender.count(emptyList()))

        // Test intersect filters extensions
        val extensions = (1..10).toList()
        val filtered = predicateExtender.intersect(emptyList(), extensions)

        val expected = listOf(2, 4, 6, 8, 10)
        assertEquals(expected, filtered)
    }

    @Test
    fun `test unary predicate - filter positive numbers`() {
        val isPositive: Predicate1<Any> = { x -> (x as Int) > 0 }
        val predicateExtender = GenericPredicatePrefixExtender(listOf(0), isPositive)

        val extensions = listOf(-3, -2, -1, 0, 1, 2, 3)
        val filtered = predicateExtender.intersect(emptyList(), extensions)

        val expected = listOf(1, 2, 3)
        assertEquals(expected, filtered)
    }

    @Test
    fun `test binary predicate - filter where second is greater than first`() {
        // Predicate2: keep where extension > prefix value
        val greaterThan: Predicate2<Any, Any> = { a, b -> (b as Int) > (a as Int) }
        val predicateExtender = GenericPredicatePrefixExtender(listOf(0, 1), greaterThan)

        // Test participatesInLevel - should participate in last level (1)
        assertEquals(false, predicateExtender.participatesInLevel(0))
        assertEquals(true, predicateExtender.participatesInLevel(1))

        // Test with prefix [5], should keep only extensions > 5
        val extensions = (1..10).toList()
        val filtered = predicateExtender.intersect(listOf(5), extensions)

        val expected = listOf(6, 7, 8, 9, 10)
        assertEquals(expected, filtered)
    }

    @Test
    fun `test binary predicate - filter where sum is even`() {
        // Predicate2: keep where a + b is even
        val sumIsEven: Predicate2<Any, Any> = { a, b -> ((a as Int) + (b as Int)) % 2 == 0 }
        val predicateExtender = GenericPredicatePrefixExtender(listOf(0, 1), sumIsEven)

        // Prefix [3] (odd), so keep only odd extensions (odd + odd = even)
        val extensions = (1..6).toList()
        val filtered = predicateExtender.intersect(listOf(3), extensions)

        val expected = listOf(1, 3, 5)
        assertEquals(expected, filtered)

        // Prefix [4] (even), so keep only even extensions (even + even = even)
        val filtered2 = predicateExtender.intersect(listOf(4), extensions)

        val expected2 = listOf(2, 4, 6)
        assertEquals(expected2, filtered2)
    }

    @Test
    fun `test propose throws IllegalStateException`() {
        val isEven: Predicate1<Any> = { x -> (x as Int) % 2 == 0 }
        val predicateExtender = GenericPredicatePrefixExtender(listOf(0), isEven)

        assertThrows<IllegalStateException> {
            predicateExtender.propose(emptyList())
        }
    }

    @Test
    fun `test invalid levels size throws exception`() {
        val pred: Predicate1<Any> = { true }

        assertThrows<IllegalArgumentException> {
            GenericPredicatePrefixExtender(emptyList(), pred)
        }

        assertThrows<IllegalArgumentException> {
            GenericPredicatePrefixExtender(listOf(0, 1, 2), pred)
        }
    }

    @Test
    fun `test binary predicate with join-like scenario`() {
        // Simulate a scenario where we have tuples and want to filter based on a predicate
        val lessThan: Predicate2<Any, Any> = { a, b -> (a as Int) < (b as Int) }
        val predicateExtender = GenericPredicatePrefixExtender(listOf(0, 1), lessThan)

        val resultTuples = mutableListOf<ResultTuple>()
        val proposedExtensions = (1..5).toList()

        for (i in 1..5) {
            val prefix = listOf(i)
            val filteredExtensions = predicateExtender.intersect(prefix, proposedExtensions)
            for (extension in filteredExtensions) {
                resultTuples.add(listOf(i, extension))
            }
        }

        // Expected: all pairs (i, j) where i < j
        val expected = mutableListOf<ResultTuple>()
        for (i in 1..5) {
            for (j in 1..5) {
                if (i < j) {
                    expected.add(listOf(i, j))
                }
            }
        }

        assertEquals(expected.size, resultTuples.size)
        assertEquals(expected.toSet(), resultTuples.toSet())
    }
}
