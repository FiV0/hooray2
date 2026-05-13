package org.hooray.incremental.stream

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Test

class IndexSpecTest {

    @Test
    fun `identical specs are equal`() {
        val a = IndexSpec<List<Any>, Any, Any>(
            name = "aev",
            keyLevels = listOf(1, 0),
            valueLevels = listOf(2)
        )
        val b = IndexSpec<List<Any>, Any, Any>(
            name = "aev",
            keyLevels = listOf(1, 0),
            valueLevels = listOf(2)
        )
        assertEquals(a, b)
        assertEquals(a.hashCode(), b.hashCode())
    }

    @Test
    fun `differing keyLevels makes specs unequal`() {
        val a = IndexSpec<List<Any>, Any, Any>(
            name = "x",
            keyLevels = listOf(0, 1),
            valueLevels = listOf(2)
        )
        val b = a.copy(keyLevels = listOf(1, 0))
        assertNotEquals(a, b)
    }

    @Test
    fun `differing name makes specs unequal`() {
        val a = IndexSpec<List<Any>, Any, Any>(
            name = "aev",
            keyLevels = listOf(0),
            valueLevels = listOf(1, 2)
        )
        val b = a.copy(name = "ave")
        assertNotEquals(a, b)
    }

    @Test
    fun `fixedPrefix defaults to empty list`() {
        val spec = IndexSpec<List<Any>, Any, Any>(
            name = "x",
            keyLevels = listOf(0),
            valueLevels = listOf(1)
        )
        assertEquals(emptyList<Any>(), spec.fixedPrefix)
    }

    @Test
    fun `differing fixedPrefix makes specs unequal`() {
        val a = IndexSpec<List<Any>, Any, Any>(
            name = "x",
            keyLevels = listOf(0),
            valueLevels = listOf(1),
            fixedPrefix = emptyList()
        )
        val b = a.copy(fixedPrefix = listOf<Any>("a"))
        assertNotEquals(a, b)
    }

    @Test
    fun `toString includes the spec name`() {
        val spec = IndexSpec<List<Any>, Any, Any>(
            name = "ave",
            keyLevels = listOf(2, 1),
            valueLevels = listOf(0)
        )
        assert(spec.toString().contains("ave"))
    }
}
