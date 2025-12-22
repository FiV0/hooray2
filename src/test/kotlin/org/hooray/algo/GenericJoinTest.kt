package org.hooray.algo

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class GenericJoinTest {

    @Test
    fun `test empty extenders fails`() {
        val emptyExtenders = emptyList<PrefixExtender>()
        val prefixes = listOf(emptyList<Any>())

        assertThrows<IllegalArgumentException> {
            GenericSingleJoin(emptyExtenders, prefixes)
        }
    }

    @Test
    fun `test two extenders with even and divisible by three`() {
        val evenExtender = PrefixExtender.createSingleLevel(listOf(2, 4, 6, 8, 10, 12), 0)
        val divisibleByThreeExtender = PrefixExtender.createSingleLevel(listOf(3, 6, 9, 12), 0)

        val extenders = listOf(evenExtender, divisibleByThreeExtender)
        val prefixes = listOf(emptyList<Any>())

        val join = GenericSingleJoin(extenders, prefixes)
        val result = join.join()

        assertEquals(2, result.size)

        val expected = listOf(
            listOf(6),
            listOf(12)
        )
        assertEquals(expected, result)
    }

    @Test
    fun `test GenericJoin with two levels`() {
        // First level: even numbers and numbers divisible by 3 (same as previous test)
        val evenExtender = PrefixExtender.createSingleLevel(listOf(2, 4, 6, 8, 10, 12), 0)
        val divisibleByThreeExtender = PrefixExtender.createSingleLevel(listOf(3, 6, 9, 12), 0)

        // Second level: single extender that takes the prefix value and returns all values divisible by it up to 12
        val divisibleByPrefixExtender = object : PrefixExtender {
            override fun count(prefix: Prefix): Int {
                val divisor = prefix[0] as Int
                return (1..12).count { it % divisor == 0 }
            }

            override fun propose(prefix: Prefix): List<Extension> {
                val divisor = prefix[0] as Int
                return (1..12).filter { it % divisor == 0 }
            }

            override fun intersect(prefix: Prefix, extensions: List<Extension>): List<Extension> {
                val divisor = prefix[0] as Int
                return extensions.filter { ext ->
                    ext is Int && ext % divisor == 0
                }
            }

            override fun participatesInLevel(level: Int) = level == 1
        }

        val extenders = listOf(
            evenExtender, divisibleByThreeExtender,  // First level
            divisibleByPrefixExtender                // Second level
        )

        val join = GenericJoin(extenders, 2)
        val result = join.join()

        // First level produces: [6] and [12]
        // Second level for prefix [6]: numbers divisible by 6 up to 12 -> 6, 12 -> results: [6, 6], [6, 12]
        // Second level for prefix [12]: numbers divisible by 12 up to 12 -> 12 -> result: [12, 12]
        assertEquals(3, result.size)

        val expected = listOf(
            listOf(6, 6),
            listOf(6, 12),
            listOf(12, 12)
        )
        assertEquals(expected, result)
    }

    @Test
    fun `test createFromPrefixExtender participates in correct levels`() {
        val extender = PrefixExtender.createFromPrefixExtender(listOf(0, 2), listOf("a", "b"))

        assertEquals(true, extender.participatesInLevel(0))
        assertEquals(false, extender.participatesInLevel(1))
        assertEquals(true, extender.participatesInLevel(2))
        assertEquals(false, extender.participatesInLevel(3))
    }

    @Test
    fun `test createFromPrefixExtender count with matching prefix`() {
        // Extender expects value "x" at level 0 and "y" at level 2
        val extender = PrefixExtender.createFromPrefixExtender(listOf(0, 2), listOf("x", "y"))

        // Empty prefix - no levels to check yet, so it matches
        assertEquals(1, extender.count(emptyList()))

        // Prefix with matching value at level 0
        assertEquals(1, extender.count(listOf("x")))

        // Prefix with non-matching value at level 0
        assertEquals(0, extender.count(listOf("z")))

        // Prefix with matching values at levels 0 and 2 (level 1 can be anything)
        assertEquals(1, extender.count(listOf("x", "anything", "y")))

        // Prefix with matching value at level 0 but non-matching at level 2
        assertEquals(0, extender.count(listOf("x", "anything", "z")))
    }

    @Test
    fun `test createFromPrefixExtender propose with matching prefix`() {
        // Extender expects value "x" at level 0 and "y" at level 2
        val extender = PrefixExtender.createFromPrefixExtender(listOf(0, 2), listOf("x", "y"))

        // Empty prefix - propose first value "x"
        assertEquals(listOf("x"), extender.propose(emptyList()))

        // Prefix with matching value at level 0 - propose "y" (next value for level 2)
        assertEquals(listOf("y"), extender.propose(listOf("x")))

        // Prefix with non-matching value at level 0
        assertEquals(emptyList<Any>(), extender.propose(listOf("z")))

        // Prefix with matching value at level 0, arbitrary value at level 1
        assertEquals(listOf("y"), extender.propose(listOf("x", "anything")))
    }

    @Test
    fun `test createFromPrefixExtender intersect with matching prefix`() {
        // Extender expects value "x" at level 0 and "y" at level 2
        val extender = PrefixExtender.createFromPrefixExtender(listOf(0, 2), listOf("x", "y"))

        // Empty prefix, extensions contain the expected value
        assertEquals(listOf("x"), extender.intersect(emptyList(), listOf("x", "a", "b")))

        // Empty prefix, extensions don't contain the expected value
        assertEquals(emptyList<Any>(), extender.intersect(emptyList(), listOf("a", "b", "c")))

        // Matching prefix at level 0, extensions contain expected value "y"
        assertEquals(listOf("y"), extender.intersect(listOf("x"), listOf("y", "z")))

        // Matching prefix at level 0, extensions don't contain expected value
        assertEquals(emptyList<Any>(), extender.intersect(listOf("x"), listOf("a", "b")))

        // Non-matching prefix
        assertEquals(emptyList<Any>(), extender.intersect(listOf("wrong"), listOf("x", "y")))
    }

    @Test
    fun `test createFromPrefixExtender in GenericJoin`() {
        // Create a scenario with 3 levels where we use createFromPrefixExtender
        // Level 0: values 1, 2, 3
        // Level 1: values "a", "b"
        // Level 2: values "x", "y"

        val level0Extender = PrefixExtender.createSingleLevel(listOf(1, 2, 3), 0)
        val level1Extender = PrefixExtender.createSingleLevel(listOf("a", "b"), 1)
        val level2Extender = PrefixExtender.createSingleLevel(listOf("x", "y"), 2)

        // This extender constrains: level 0 = 2, level 2 = "x"
        val constraintExtender = PrefixExtender.createFromPrefixExtender(listOf(0, 2), listOf(2, "x"))

        val extenders = listOf(level0Extender, level1Extender, level2Extender, constraintExtender)
        val join = GenericJoin(extenders, 3)
        val result = join.join()

        // Only tuples where level 0 = 2 and level 2 = "x" should remain
        // Level 1 can be "a" or "b"
        val expected = listOf(
            listOf(2, "a", "x"),
            listOf(2, "b", "x")
        )
        assertEquals(expected, result)
    }

    // Tests for createFromPrefixesExtender (multiple prefixes)

    @Test
    fun `test createFromPrefixesExtender participates in correct levels`() {
        val extender = PrefixExtender.createFromPrefixesExtender(
            listOf(0, 2),
            listOf(listOf("a", "b"), listOf("x", "y"))
        )

        assertEquals(true, extender.participatesInLevel(0))
        assertEquals(false, extender.participatesInLevel(1))
        assertEquals(true, extender.participatesInLevel(2))
        assertEquals(false, extender.participatesInLevel(3))
    }

    @Test
    fun `test createFromPrefixesExtender count with matching prefixes`() {
        // Extender accepts two prefixes: ["a", "b"] or ["x", "y"] at levels [0, 2]
        val extender = PrefixExtender.createFromPrefixesExtender(
            listOf(0, 2),
            listOf(listOf("a", "b"), listOf("x", "y"))
        )

        // Empty prefix - both prefixes match
        assertEquals(2, extender.count(emptyList()))

        // Prefix with "a" at level 0 - only first prefix matches
        assertEquals(1, extender.count(listOf("a")))

        // Prefix with "x" at level 0 - only second prefix matches
        assertEquals(1, extender.count(listOf("x")))

        // Prefix with non-matching value at level 0
        assertEquals(0, extender.count(listOf("z")))

        // Full match for first prefix
        assertEquals(1, extender.count(listOf("a", "anything", "b")))

        // Full match for second prefix
        assertEquals(1, extender.count(listOf("x", "anything", "y")))

        // Partial match at level 0 but mismatch at level 2
        assertEquals(0, extender.count(listOf("a", "anything", "y")))
    }

    @Test
    fun `test createFromPrefixesExtender propose with matching prefixes`() {
        val extender = PrefixExtender.createFromPrefixesExtender(
            listOf(0, 2),
            listOf(listOf("a", "b"), listOf("x", "y"))
        )

        // Empty prefix - propose first values from both prefixes (distinct)
        assertEquals(listOf("a", "x"), extender.propose(emptyList()))

        // Prefix with "a" at level 0 - propose "b" from first prefix
        assertEquals(listOf("b"), extender.propose(listOf("a")))

        // Prefix with "x" at level 0 - propose "y" from second prefix
        assertEquals(listOf("y"), extender.propose(listOf("x")))

        // Non-matching prefix
        assertEquals(emptyList<Any>(), extender.propose(listOf("z")))

        // Prefix with "a" at level 0, anything at level 1 - propose "b"
        assertEquals(listOf("b"), extender.propose(listOf("a", "anything")))
    }

    @Test
    fun `test createFromPrefixesExtender intersect with matching prefixes`() {
        val extender = PrefixExtender.createFromPrefixesExtender(
            listOf(0, 2),
            listOf(listOf("a", "b"), listOf("x", "y"))
        )

        // Empty prefix, extensions contain both expected values
        assertEquals(listOf("a", "x"), extender.intersect(emptyList(), listOf("a", "x", "z")))

        // Empty prefix, extensions contain only one expected value
        assertEquals(listOf("a"), extender.intersect(emptyList(), listOf("a", "z")))

        // Empty prefix, extensions don't contain expected values
        assertEquals(emptyList<Any>(), extender.intersect(emptyList(), listOf("z", "w")))

        // Matching prefix at level 0 for first prefix
        assertEquals(listOf("b"), extender.intersect(listOf("a"), listOf("b", "y", "z")))

        // Matching prefix at level 0 for second prefix
        assertEquals(listOf("y"), extender.intersect(listOf("x"), listOf("b", "y", "z")))

        // Non-matching prefix
        assertEquals(emptyList<Any>(), extender.intersect(listOf("z"), listOf("a", "b", "x", "y")))
    }

    @Test
    fun `test createFromPrefixesExtender in GenericJoin`() {
        // Create a scenario with 2 levels
        // Level 0: values "Ivan", "Petr", "Bob"
        // Level 1: values "Ivanov", "Petrov", "Smith"

        val level0Extender = PrefixExtender.createSingleLevel(listOf("Ivan", "Petr", "Bob"), 0)
        val level1Extender = PrefixExtender.createSingleLevel(listOf("Ivanov", "Petrov", "Smith"), 1)

        // This extender constrains to only allow: ["Ivan", "Ivanov"] or ["Petr", "Petrov"]
        val constraintExtender = PrefixExtender.createFromPrefixesExtender(
            listOf(0, 1),
            listOf(listOf("Ivan", "Ivanov"), listOf("Petr", "Petrov"))
        )

        val extenders = listOf(level0Extender, level1Extender, constraintExtender)
        val join = GenericJoin(extenders, 2)
        val result = join.join()

        // Only the two valid combinations should remain
        val expected = listOf(
            listOf("Ivan", "Ivanov"),
            listOf("Petr", "Petrov")
        )
        assertEquals(expected, result)
    }

    @Test
    fun `test createFromPrefixesExtender with single prefix behaves like createFromPrefixExtender`() {
        val singleExtender = PrefixExtender.createFromPrefixExtender(listOf(0, 1), listOf("a", "b"))
        val multiExtender = PrefixExtender.createFromPrefixesExtender(listOf(0, 1), listOf(listOf("a", "b")))

        // Both should behave identically
        assertEquals(singleExtender.count(emptyList()), multiExtender.count(emptyList()))
        assertEquals(singleExtender.count(listOf("a")), multiExtender.count(listOf("a")))
        assertEquals(singleExtender.count(listOf("z")), multiExtender.count(listOf("z")))

        assertEquals(singleExtender.propose(emptyList()), multiExtender.propose(emptyList()))
        assertEquals(singleExtender.propose(listOf("a")), multiExtender.propose(listOf("a")))
        assertEquals(singleExtender.propose(listOf("z")), multiExtender.propose(listOf("z")))

        assertEquals(
            singleExtender.intersect(emptyList(), listOf("a", "b", "c")),
            multiExtender.intersect(emptyList(), listOf("a", "b", "c"))
        )
    }
}
