package org.hooray

import clojure.lang.Keyword
import clojure.lang.PersistentVector
import clojure.lang.Symbol
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class UniversalComparatorTest {

    @Test
    fun `test null comparisons`() {
        assertEquals(0, UniversalComparator.compare(null, null))
    }

    @Test
    fun `test null comes before other types`() {
        assertTrue(UniversalComparator.compare(null, 1) < 0)
        assertTrue(UniversalComparator.compare(null, "string") < 0)
        assertTrue(UniversalComparator.compare(null, listOf(1, 2)) < 0)
        assertTrue(UniversalComparator.compare(null, mapOf("a" to 1)) < 0)
    }

    @Test
    fun `test number comparisons`() {
        assertEquals(0, UniversalComparator.compare(5, 5))
        assertTrue(UniversalComparator.compare(3, 5) < 0)
        assertTrue(UniversalComparator.compare(5, 3) > 0)
        assertTrue(UniversalComparator.compare(5L, 5) == 0)
        assertTrue(UniversalComparator.compare(3.14, 3.15) < 0)
    }

    @Test
    fun `test string comparisons`() {
        assertEquals(0, UniversalComparator.compare("hello", "hello"))
        assertTrue(UniversalComparator.compare("abc", "xyz") < 0)
        assertTrue(UniversalComparator.compare("xyz", "abc") > 0)
        assertTrue(UniversalComparator.compare("", "a") < 0)
    }

    @Test
    fun `test symbol comparisons`() {
        val sym1 = Symbol.intern("foo")
        val sym2 = Symbol.intern("foo")
        val sym3 = Symbol.intern("bar")

        assertEquals(0, UniversalComparator.compare(sym1, sym2))
        assertTrue(UniversalComparator.compare(sym3, sym1) < 0)
        assertTrue(UniversalComparator.compare(sym1, sym3) > 0)
    }

    @Test
    fun `test keyword comparisons`() {
        val kw1 = Keyword.intern("foo")
        val kw2 = Keyword.intern("foo")
        val kw3 = Keyword.intern("bar")

        assertEquals(0, UniversalComparator.compare(kw1, kw2))
        assertTrue(UniversalComparator.compare(kw3, kw1) < 0)
        assertTrue(UniversalComparator.compare(kw1, kw3) > 0)
    }

    @Test
    fun `test list comparisons - equal lists`() {
        val list1 = listOf(1, 2, 3)
        val list2 = listOf(1, 2, 3)
        assertEquals(0, UniversalComparator.compare(list1, list2))
    }

    @Test
    fun `test list comparisons - different sizes`() {
        val list1 = listOf(1, 2)
        val list2 = listOf(1, 2, 3)
        assertTrue(UniversalComparator.compare(list1, list2) < 0)
        assertTrue(UniversalComparator.compare(list2, list1) > 0)
    }

    @Test
    fun `test list comparisons - different elements`() {
        val list1 = listOf(1, 2, 3)
        val list2 = listOf(1, 2, 4)
        assertTrue(UniversalComparator.compare(list1, list2) < 0)
        assertTrue(UniversalComparator.compare(list2, list1) > 0)
    }

    @Test
    fun `test list comparisons - nested lists`() {
        val list1 = listOf(1, listOf(2, 3))
        val list2 = listOf(1, listOf(2, 3))
        val list3 = listOf(1, listOf(2, 4))

        assertEquals(0, UniversalComparator.compare(list1, list2))
        assertTrue(UniversalComparator.compare(list1, list3) < 0)
    }

    @Test
    fun `test persistent vector comparisons`() {
        val vec1 = PersistentVector.create(1, 2, 3)
        val vec2 = PersistentVector.create(1, 2, 3)
        val vec3 = PersistentVector.create(1, 2, 4)

        assertEquals(0, UniversalComparator.compare(vec1, vec2))
        assertTrue(UniversalComparator.compare(vec1, vec3) < 0)
        assertTrue(UniversalComparator.compare(vec3, vec1) > 0)
    }

    @Test
    fun `test map comparisons - equal maps`() {
        val map1 = mapOf("a" to 1, "b" to 2)
        val map2 = mapOf("a" to 1, "b" to 2)
        assertEquals(0, UniversalComparator.compare(map1, map2))
    }

    @Test
    fun `test map comparisons - different sizes`() {
        val map1 = mapOf("a" to 1)
        val map2 = mapOf("a" to 1, "b" to 2)
        assertTrue(UniversalComparator.compare(map1, map2) < 0)
        assertTrue(UniversalComparator.compare(map2, map1) > 0)
    }

    @Test
    fun `test map comparisons - different keys`() {
        val map1 = mapOf("a" to 1, "b" to 2)
        val map2 = mapOf("a" to 1, "c" to 2)
        assertTrue(UniversalComparator.compare(map1, map2) < 0)
        assertTrue(UniversalComparator.compare(map2, map1) > 0)
    }

    @Test
    fun `test map comparisons - different values`() {
        val map1 = mapOf("a" to 1, "b" to 2)
        val map2 = mapOf("a" to 1, "b" to 3)
        assertTrue(UniversalComparator.compare(map1, map2) < 0)
        assertTrue(UniversalComparator.compare(map2, map1) > 0)
    }

    @Test
    fun `test map comparisons - insertion order independent`() {
        val map1 = mapOf("b" to 2, "a" to 1)
        val map2 = mapOf("a" to 1, "b" to 2)
        assertEquals(0, UniversalComparator.compare(map1, map2))
    }

    @Test
    fun `test map comparisons - nested maps`() {
        val map1 = mapOf("a" to mapOf("x" to 1))
        val map2 = mapOf("a" to mapOf("x" to 1))
        val map3 = mapOf("a" to mapOf("x" to 2))

        assertEquals(0, UniversalComparator.compare(map1, map2))
        assertTrue(UniversalComparator.compare(map1, map3) < 0)
    }

    @Test
    fun `test type discrimination - numbers before strings`() {
        assertTrue(UniversalComparator.compare(42, "hello") < 0)
        assertTrue(UniversalComparator.compare("hello", 42) > 0)
    }

    @Test
    fun `test type discrimination - strings before symbols`() {
        val sym = Symbol.intern("foo")
        assertTrue(UniversalComparator.compare("hello", sym) < 0)
        assertTrue(UniversalComparator.compare(sym, "hello") > 0)
    }

    @Test
    fun `test type discrimination - symbols before keywords`() {
        val sym = Symbol.intern("foo")
        val kw = Keyword.intern("bar")
        assertTrue(UniversalComparator.compare(sym, kw) < 0)
        assertTrue(UniversalComparator.compare(kw, sym) > 0)
    }

    @Test
    fun `test type discrimination - keywords before lists`() {
        val kw = Keyword.intern("foo")
        val list = listOf(1, 2, 3)
        assertTrue(UniversalComparator.compare(kw, list) < 0)
        assertTrue(UniversalComparator.compare(list, kw) > 0)
    }

    @Test
    fun `test type discrimination - lists before persistent vectors`() {
        val list = listOf(1, 2, 3)
        val vec = PersistentVector.create(1, 2, 3)
        assertTrue(UniversalComparator.compare(vec, list) < 0)
        assertTrue(UniversalComparator.compare(list, vec) > 0)
    }

    @Test
    fun `test type discrimination - persistent vectors before maps`() {
        val vec = PersistentVector.create(1, 2, 3)
        val map = mapOf("a" to 1)
        assertTrue(UniversalComparator.compare(vec, map) < 0)
        assertTrue(UniversalComparator.compare(map, vec) > 0)
    }

    @Test
    fun `test unsupported type throws exception`() {
        val unsupported = Any()
        assertThrows<IllegalArgumentException> {
            UniversalComparator.compare(unsupported, unsupported)
        }
    }

    @Test
    fun `test complex nested structure`() {
        val complex1 = listOf(
            mapOf("name" to "Alice", "age" to 30),
            listOf(1, 2, 3)
        )
        val complex2 = listOf(
            mapOf("name" to "Alice", "age" to 30),
            listOf(1, 2, 3)
        )
        val complex3 = listOf(
            mapOf("name" to "Bob", "age" to 30),
            listOf(1, 2, 3)
        )

        assertEquals(0, UniversalComparator.compare(complex1, complex2))
        assertTrue(UniversalComparator.compare(complex1, complex3) < 0)
    }

    @Test
    fun `test empty collections`() {
        val emptyList1 = listOf<Any>()
        val emptyList2 = listOf<Any>()
        val nonEmptyList = listOf(1)

        assertEquals(0, UniversalComparator.compare(emptyList1, emptyList2))
        assertTrue(UniversalComparator.compare(emptyList1, nonEmptyList) < 0)

        val emptyMap1 = mapOf<String, Any>()
        val emptyMap2 = mapOf<String, Any>()
        val nonEmptyMap = mapOf("a" to 1)

        assertEquals(0, UniversalComparator.compare(emptyMap1, emptyMap2))
        assertTrue(UniversalComparator.compare(emptyMap1, nonEmptyMap) < 0)
    }
}
