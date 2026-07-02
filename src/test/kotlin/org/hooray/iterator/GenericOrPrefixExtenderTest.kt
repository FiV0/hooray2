package org.hooray.iterator

import org.hooray.algo.PrefixExtender
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class GenericOrPrefixExtenderTest {

    @Test
    fun `intersect does not leak candidates across branch prefixes`() {
        val lessThanThirty: Predicate1<Any> = { age -> (age as Int) < 30 }
        val lessThanForty: Predicate1<Any> = { age -> (age as Int) < 40 }
        val branchA = GenericAndPrefixExtender(
            listOf(
                PrefixExtender.createFromPrefixExtender(listOf(0), listOf("a")),
                GenericPredicatePrefixExtender(listOf(1L), lessThanThirty),
            )
        )
        val branchB = GenericAndPrefixExtender(
            listOf(
                PrefixExtender.createFromPrefixExtender(listOf(0), listOf("b")),
                GenericPredicatePrefixExtender(listOf(1L), lessThanForty),
            )
        )
        val orExtender = GenericOrPrefixExtender(listOf(branchA, branchB))

        assertEquals(listOf("a", "b"), orExtender.intersect(emptyList(), listOf("a", "b")))
        assertEquals(emptyList<Any>(), orExtender.intersect(listOf("a"), listOf(35)))
        assertEquals(listOf(35), orExtender.intersect(listOf("b"), listOf(35)))
    }

    @Test
    fun `count saturates when predicate-only branches cannot propose`() {
        val lessThanThirty: Predicate1<Any> = { age -> (age as Int) < 30 }
        val lessThanForty: Predicate1<Any> = { age -> (age as Int) < 40 }
        val branchA = GenericAndPrefixExtender(
            listOf(GenericPredicatePrefixExtender(listOf(1L), lessThanThirty))
        )
        val branchB = GenericAndPrefixExtender(
            listOf(GenericPredicatePrefixExtender(listOf(1L), lessThanForty))
        )
        val orExtender = GenericOrPrefixExtender(listOf(branchA, branchB))

        assertEquals(listOf("entity"), orExtender.intersect(emptyList(), listOf("entity")))
        assertEquals(Int.MAX_VALUE, saturatingSum(listOf(Int.MAX_VALUE, Int.MAX_VALUE)))
        assertEquals(Int.MAX_VALUE, orExtender.count(listOf("entity")))
    }
}
