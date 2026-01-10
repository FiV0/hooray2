package org.hooray.iterator

import kotlinx.collections.immutable.persistentListOf
import org.hooray.algo.LeapfrogIndex
import org.hooray.algo.LeapfrogJoin
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class AVLPredicateLeapfrogIndexTest {

    @Test
    fun `test unary predicate filters single value`() {
        val isEven: Predicate1<Any> = { x -> (x as Int) % 2 == 0 }
        val predicateIndex = AVLPredicateLeapfrogIndex(listOf(0), isEven)

        assertEquals(true, predicateIndex.accept(persistentListOf(2)))
        assertEquals(true, predicateIndex.accept(persistentListOf(4)))
        assertEquals(false, predicateIndex.accept(persistentListOf(1)))
        assertEquals(false, predicateIndex.accept(persistentListOf(3)))
    }

    @Test
    fun `test binary predicate filters two values`() {
        val lessThan: Predicate2<Any, Any> = { x, y -> (x as Int) < (y as Int) }
        val predicateIndex = AVLPredicateLeapfrogIndex(listOf(0, 1), lessThan)

        assertEquals(true, predicateIndex.accept(persistentListOf(1, 2)))
        assertEquals(true, predicateIndex.accept(persistentListOf(5, 10)))
        assertEquals(false, predicateIndex.accept(persistentListOf(2, 1)))
        assertEquals(false, predicateIndex.accept(persistentListOf(5, 5)))
    }

    @Test
    fun `test participatesInLevel returns last level`() {
        val unaryPredicate = AVLPredicateLeapfrogIndex(listOf(0), { _: Any -> true })
        assertEquals(true, unaryPredicate.participatesInLevel(0))
        assertEquals(false, unaryPredicate.participatesInLevel(1))

        val binaryPredicate = AVLPredicateLeapfrogIndex(listOf(0, 1), { _: Any, _: Any -> true })
        assertEquals(false, binaryPredicate.participatesInLevel(0))
        assertEquals(true, binaryPredicate.participatesInLevel(1))
    }

    @Test
    fun `test LeapfrogJoin with unary predicate filter`() {
        val numbers = LeapfrogIndex.createSingleLevel(listOf(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
        val isEven: Predicate1<Any> = { x -> (x as Int) % 2 == 0 }
        val predicateIndex = AVLPredicateLeapfrogIndex(listOf(0), isEven)

        val join = LeapfrogJoin(listOf(numbers), 1, listOf(predicateIndex))
        val result = join.join()

        val expected = listOf(listOf(2), listOf(4), listOf(6), listOf(8), listOf(10))
        assertEquals(expected, result)
    }

    @Test
    fun `test combined negation and predicate filters`() {
        val numbers = LeapfrogIndex.createSingleLevel(listOf(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))

        // Filter: keep only evens
        val isEven: Predicate1<Any> = { x -> (x as Int) % 2 == 0 }
        val predicateIndex = AVLPredicateLeapfrogIndex(listOf(0), isEven)

        // Negation: exclude multiples of 4
        val multiplesOfFour = LeapfrogIndex.createSingleLevel(listOf(4, 8))
        val notIndex = AVLNotLeapfrogIndex(listOf(multiplesOfFour), participationLevel = 0)

        val join = LeapfrogJoin(listOf(numbers), 1, listOf(predicateIndex, notIndex))
        val result = join.join()

        // Evens: 2, 4, 6, 8, 10
        // After excluding 4, 8: 2, 6, 10
        val expected = listOf(listOf(2), listOf(6), listOf(10))
        assertEquals(expected, result)
    }
}
