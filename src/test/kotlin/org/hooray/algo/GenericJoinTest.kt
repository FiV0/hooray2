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
        // First extender: even numbers up to 12
        val evenExtender = object : PrefixExtender {
            private val evens = listOf(2, 4, 6, 8, 10, 12)

            override fun count(prefix: Prefix): Int = evens.size

            override fun propose(prefix: Prefix): List<Extension> = evens

            override fun extend(prefix: Prefix, extensions: List<Extension>): List<Extension>
                    = extensions.filter { ext -> evens.contains(ext) }
        }

        // Second extender: numbers divisible by 3 up to 12
        val divisibleByThreeExtender = object : PrefixExtender {
            private val divByThree = listOf(3, 6, 9, 12)

            override fun count(prefix: Prefix): Int = divByThree.size

            override fun propose(prefix: Prefix): List<Extension> = divByThree

            override fun extend(prefix: Prefix, extensions: List<Extension>): List<Extension>
                    = extensions.filter { ext -> divByThree.contains(ext) }
        }

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
}
