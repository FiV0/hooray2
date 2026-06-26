package org.hooray.iterator

import org.hooray.algo.PrefixExtender
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class GenericOrPrefixExtenderTest {

    @Test
    fun `count saturates when predicate-only branches cannot propose`() {
        val lessThanThirty: Predicate1<Any> = { age -> (age as Int) < 30 }
        val lessThanForty: Predicate1<Any> = { age -> (age as Int) < 40 }
        val branchA = GenericAndPrefixExtender(
            listOf(GenericPredicatePrefixExtender(listOf(1), lessThanThirty))
        )
        val branchB = GenericAndPrefixExtender(
            listOf(GenericPredicatePrefixExtender(listOf(1), lessThanForty))
        )
        val orExtender = GenericOrPrefixExtender(listOf(branchA, branchB))

        assertEquals(Int.MAX_VALUE, saturatingSum(listOf(Int.MAX_VALUE, Int.MAX_VALUE)))
        assertEquals(Int.MAX_VALUE, orExtender.count(listOf("entity")))
    }

    @Test
    fun `intersect preserves branch-local prefix constraints`() {
        val lessThanThirty: Predicate1<Any> = { age -> (age as Int) < 30 }
        val lessThanForty: Predicate1<Any> = { age -> (age as Int) < 40 }
        val namedA = PrefixExtender.createFromPrefixExtender(listOf(0), listOf("a"))
        val namedB = PrefixExtender.createFromPrefixExtender(listOf(0), listOf("b"))
        val branchA = GenericAndPrefixExtender(
            listOf(namedA, GenericPredicatePrefixExtender(listOf(1), lessThanThirty))
        )
        val branchB = GenericAndPrefixExtender(
            listOf(namedB, GenericPredicatePrefixExtender(listOf(1), lessThanForty))
        )
        val orExtender = GenericOrPrefixExtender(listOf(branchA, branchB))

        assertEquals(emptyList<Any>(), orExtender.intersect(listOf("a"), listOf(35)))
        assertEquals(listOf(35), orExtender.intersect(listOf("b"), listOf(35)))
    }
}
