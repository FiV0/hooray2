package org.hooray.iterator

import org.hooray.algo.ResultTuple
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class GenericFnPrefixExtenderTest {

    @Test
    fun `test unary function - double the value`() {
        val double: Fn1<Any, Any> = { x -> (x as Int) * 2 }
        val fnExtender = GenericFnPrefixExtender(listOf(0), 1, double)

        // Test participatesInLevel
        assertEquals(false, fnExtender.participatesInLevel(0))
        assertEquals(true, fnExtender.participatesInLevel(1))

        // Test count returns 1
        assertEquals(1, fnExtender.count(listOf(5)))

        // Test propose returns function result
        val proposed = fnExtender.propose(listOf(5))
        assertEquals(listOf(10), proposed)

        // Test propose with different prefix
        assertEquals(listOf(14), fnExtender.propose(listOf(7)))
    }

    @Test
    fun `test unary function - intersect with matching extension`() {
        val double: Fn1<Any, Any> = { x -> (x as Int) * 2 }
        val fnExtender = GenericFnPrefixExtender(listOf(0), 1, double)

        // Extensions contain the result (10)
        val extensions = listOf(8, 9, 10, 11, 12)
        val result = fnExtender.intersect(listOf(5), extensions)
        assertEquals(listOf(10), result)
    }

    @Test
    fun `test unary function - intersect with no matching extension`() {
        val double: Fn1<Any, Any> = { x -> (x as Int) * 2 }
        val fnExtender = GenericFnPrefixExtender(listOf(0), 1, double)

        // Extensions do not contain the result (10)
        val extensions = listOf(1, 2, 3, 4, 5)
        val result = fnExtender.intersect(listOf(5), extensions)
        assertEquals(emptyList<Any>(), result)
    }

    @Test
    fun `test binary function - sum of two values`() {
        val sum: Fn2<Any, Any, Any> = { a, b -> (a as Int) + (b as Int) }
        val fnExtender = GenericFnPrefixExtender(listOf(0, 1), 2, sum)

        // Test participatesInLevel
        assertEquals(false, fnExtender.participatesInLevel(0))
        assertEquals(false, fnExtender.participatesInLevel(1))
        assertEquals(true, fnExtender.participatesInLevel(2))

        // Test propose with prefix [3, 7] -> 10
        val proposed = fnExtender.propose(listOf(3, 7))
        assertEquals(listOf(10), proposed)
    }

    @Test
    fun `test binary function - intersect with matching extension`() {
        val sum: Fn2<Any, Any, Any> = { a, b -> (a as Int) + (b as Int) }
        val fnExtender = GenericFnPrefixExtender(listOf(0, 1), 2, sum)

        // prefix [3, 7] -> result is 10
        val extensions = listOf(8, 9, 10, 11, 12)
        val result = fnExtender.intersect(listOf(3, 7), extensions)
        assertEquals(listOf(10), result)
    }

    @Test
    fun `test binary function - intersect with no matching extension`() {
        val sum: Fn2<Any, Any, Any> = { a, b -> (a as Int) + (b as Int) }
        val fnExtender = GenericFnPrefixExtender(listOf(0, 1), 2, sum)

        // prefix [3, 7] -> result is 10, but extensions don't contain 10
        val extensions = listOf(1, 2, 3, 4, 5)
        val result = fnExtender.intersect(listOf(3, 7), extensions)
        assertEquals(emptyList<Any>(), result)
    }

    @Test
    fun `test binary function - multiply`() {
        val multiply: Fn2<Any, Any, Any> = { a, b -> (a as Int) * (b as Int) }
        val fnExtender = GenericFnPrefixExtender(listOf(0, 1), 2, multiply)

        // prefix [4, 5] -> 20
        assertEquals(listOf(20), fnExtender.propose(listOf(4, 5)))

        // intersect with extensions containing 20
        val extensions = listOf(15, 20, 25)
        assertEquals(listOf(20), fnExtender.intersect(listOf(4, 5), extensions))
    }

    @Test
    fun `test invalid levels size throws exception`() {
        val fn: Fn1<Any, Any> = { it }

        assertThrows<IllegalArgumentException> {
            GenericFnPrefixExtender(emptyList(), 0, fn)
        }

        assertThrows<IllegalArgumentException> {
            GenericFnPrefixExtender(listOf(0, 1, 2), 3, fn)
        }
    }

    @Test
    fun `test unary function with join-like scenario`() {
        // Simulate computing squares: for each x, propose x^2
        val square: Fn1<Any, Any> = { x -> (x as Int) * (x as Int) }
        val fnExtender = GenericFnPrefixExtender(listOf(0), 1, square)

        val resultTuples = mutableListOf<ResultTuple>()
        val possibleExtensions = (1..25).toList() // includes all squares up to 5^2

        for (i in 1..5) {
            val prefix = listOf(i)
            val extensions = fnExtender.intersect(prefix, possibleExtensions)
            for (extension in extensions) {
                resultTuples.add(listOf(i, extension))
            }
        }

        // Expected: [(1, 1), (2, 4), (3, 9), (4, 16), (5, 25)]
        val expected = listOf(
            listOf(1, 1),
            listOf(2, 4),
            listOf(3, 9),
            listOf(4, 16),
            listOf(5, 25)
        )

        assertEquals(expected.size, resultTuples.size)
        assertEquals(expected.toSet(), resultTuples.toSet())
    }

    @Test
    fun `test binary function with non-contiguous levels`() {
        // Test that levels don't have to be contiguous
        // levels [0, 2] means we read from prefix[0] and prefix[2]
        val sum: Fn2<Any, Any, Any> = { a, b -> (a as Int) + (b as Int) }
        val fnExtender = GenericFnPrefixExtender(listOf(0, 2), 3, sum)

        // prefix [10, "ignored", 5] -> 10 + 5 = 15
        val prefix = listOf(10, "ignored", 5)
        assertEquals(listOf(15), fnExtender.propose(prefix))
    }
}
