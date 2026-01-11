package org.hooray.iterator

import kotlinx.collections.immutable.persistentListOf
import org.hooray.algo.LeapfrogIndex
import org.hooray.algo.LeapfrogJoin
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class AVLFnLeapfrogIndexTest {

    @Test
    fun `test unary function computes value from prefix`() {
        val double: Fn1<Any, Any> = { x -> (x as Int) * 2 }
        val fnIndex = AVLFnLeapfrogIndex(listOf(0), outputLevel = 1, double)

        assertEquals(false, fnIndex.participatesInLevel(0))
        assertEquals(true, fnIndex.participatesInLevel(1))

        fnIndex.openLevel(listOf(5))
        assertEquals(10, fnIndex.key())
        assertEquals(false, fnIndex.atEnd())

        fnIndex.next()
        assertEquals(true, fnIndex.atEnd())
    }

    @Test
    fun `test binary function computes value from prefix`() {
        val add: Fn2<Any, Any, Any> = { x, y -> (x as Int) + (y as Int) }
        val fnIndex = AVLFnLeapfrogIndex(listOf(0, 1), outputLevel = 2, add)

        assertEquals(false, fnIndex.participatesInLevel(0))
        assertEquals(false, fnIndex.participatesInLevel(1))
        assertEquals(true, fnIndex.participatesInLevel(2))

        fnIndex.openLevel(listOf(3, 7))
        assertEquals(10, fnIndex.key())
        assertEquals(false, fnIndex.atEnd())

        fnIndex.next()
        assertEquals(true, fnIndex.atEnd())
    }

    @Test
    fun `test seek behavior`() {
        val double: Fn1<Any, Any> = { x -> (x as Int) * 2 }
        val fnIndex = AVLFnLeapfrogIndex(listOf(0), outputLevel = 1, double)

        fnIndex.openLevel(listOf(5))
        assertEquals(10, fnIndex.key())

        // Seeking to a value <= computed value should not change state
        fnIndex.seek(5)
        assertEquals(false, fnIndex.atEnd())
        assertEquals(10, fnIndex.key())

        fnIndex.seek(10)
        assertEquals(false, fnIndex.atEnd())
        assertEquals(10, fnIndex.key())

        // Seeking past computed value should set atEnd
        fnIndex.seek(11)
        assertEquals(true, fnIndex.atEnd())
    }

    @Test
    fun `test closeLevel resets state`() {
        val double: Fn1<Any, Any> = { x -> (x as Int) * 2 }
        val fnIndex = AVLFnLeapfrogIndex(listOf(0), outputLevel = 1, double)

        fnIndex.openLevel(listOf(5))
        assertEquals(10, fnIndex.key())

        fnIndex.closeLevel()
        assertEquals(0, fnIndex.level())

        // Can open again with different value
        fnIndex.openLevel(listOf(7))
        assertEquals(14, fnIndex.key())
    }

    @Test
    fun `test reinit resets state`() {
        val double: Fn1<Any, Any> = { x -> (x as Int) * 2 }
        val fnIndex = AVLFnLeapfrogIndex(listOf(0), outputLevel = 1, double)

        fnIndex.openLevel(listOf(5))
        fnIndex.next()
        assertEquals(true, fnIndex.atEnd())

        fnIndex.reinit()
        assertEquals(0, fnIndex.level())
        assertEquals(false, fnIndex.atEnd())
    }

    @Test
    fun `test LeapfrogJoin with unary function`() {
        // Numbers at level 0 only
        val numbers = LeapfrogIndex.createSingleLevel(listOf(1, 2, 3, 4, 5), 0)
        val double: Fn1<Any, Any> = { x -> (x as Int) * 2 }
        val fnIndex = AVLFnLeapfrogIndex(listOf(0), outputLevel = 1, double)

        // Another index at level 1 only with some doubled values
        val doubledValues = LeapfrogIndex.createSingleLevel(listOf(2, 4, 6, 8, 10), 1)

        val join = LeapfrogJoin(listOf(numbers, fnIndex, doubledValues), 2)
        val result = join.join()

        // Should find matches where numbers doubled exist in doubledValues
        // 1*2=2 (in doubledValues), 2*2=4, 3*2=6, 4*2=8, 5*2=10
        val expected = listOf(
            listOf(1, 2),
            listOf(2, 4),
            listOf(3, 6),
            listOf(4, 8),
            listOf(5, 10)
        )
        assertEquals(expected, result)
    }

    @Test
    fun `test LeapfrogJoin with binary function`() {
        // First index at level 0, second at level 1
        val first = LeapfrogIndex.createSingleLevel(listOf(1, 2, 3), 0)
        val second = LeapfrogIndex.createSingleLevel(listOf(10, 20), 1)
        val add: Fn2<Any, Any, Any> = { x, y -> (x as Int) + (y as Int) }
        val fnIndex = AVLFnLeapfrogIndex(listOf(0, 1), outputLevel = 2, add)

        // Sums at level 2
        val sums = LeapfrogIndex.createSingleLevel(listOf(11, 12, 13, 21, 22, 23), 2)

        val join = LeapfrogJoin(listOf(first, second, fnIndex, sums), 3)
        val result = join.join()

        val expected = listOf(
            listOf(1, 10, 11),
            listOf(1, 20, 21),
            listOf(2, 10, 12),
            listOf(2, 20, 22),
            listOf(3, 10, 13),
            listOf(3, 20, 23)
        )
        assertEquals(expected, result)
    }

    @Test
    fun `test function with no matching values`() {
        val numbers = LeapfrogIndex.createSingleLevel(listOf(1, 2, 3), 0)
        val double: Fn1<Any, Any> = { x -> (x as Int) * 2 }
        val fnIndex = AVLFnLeapfrogIndex(listOf(0), outputLevel = 1, double)

        // No overlap with doubled values at level 1
        val otherValues = LeapfrogIndex.createSingleLevel(listOf(100, 200), 1)

        val join = LeapfrogJoin(listOf(numbers, fnIndex, otherValues), 2)
        val result = join.join()

        assertEquals(emptyList<Any>(), result)
    }
}
