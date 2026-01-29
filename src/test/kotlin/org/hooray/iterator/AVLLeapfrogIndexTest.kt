package org.hooray.iterator

import clojure.data.avl.AVLMap
import clojure.data.avl.AVLSet
import clojure.java.api.Clojure
import clojure.lang.IFn
import clojure.lang.Symbol
import org.hooray.UniversalComparator
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.assertThrows

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AVLLeapfrogIndexTest {

    private lateinit var sortedSetBy: IFn
    private lateinit var sortedMapBy: IFn

    @BeforeAll
    fun setup() {
        val require = Clojure.`var`("clojure.core", "require")
        require.invoke(Clojure.read("clojure.data.avl"))
        sortedSetBy = Clojure.`var`("clojure.data.avl", "sorted-set-by")
        sortedMapBy = Clojure.`var`("clojure.data.avl", "sorted-map-by")
    }

    private fun createAVLSet(vararg values: Any): AVLSet {
        val args = listOf(UniversalComparator) + values.toList()
        return sortedSetBy.applyTo(clojure.lang.RT.seq(args)) as AVLSet
    }

    private fun createAVLMap(vararg kvPairs: Any): AVLMap {
        val args = listOf(UniversalComparator) + kvPairs.toList()
        return sortedMapBy.applyTo(clojure.lang.RT.seq(args)) as AVLMap
    }

    @Test
    fun `test AVL set iterator basic iteration`() {
        val avlSet = createAVLSet(2, 4, 6, 8, 10)
        val index = AVLIndex.AVLSetIndex(avlSet)
        val varOrder = listOf(Symbol.intern("x"))
        val vars = setOf(Symbol.intern("x"))

        val leapfrogIndex = AVLLeapfrogIndex(index, varOrder, vars)

        val results = mutableListOf<Any>()
        while (!leapfrogIndex.atEnd()) {
            results.add(leapfrogIndex.key())
            leapfrogIndex.next()
        }

        assertEquals(listOf(2, 4, 6, 8, 10), results)
    }

    @Test
    fun `test AVL set iterator with seek`() {
        val avlSet = createAVLSet(2, 4, 6, 8, 10, 12)
        val index = AVLIndex.AVLSetIndex(avlSet)
        val varOrder = listOf(Symbol.intern("x"))
        val vars = setOf(Symbol.intern("x"))

        val leapfrogIndex = AVLLeapfrogIndex(index, varOrder, vars)

        leapfrogIndex.seek(7)

        val results = mutableListOf<Any>()
        while (!leapfrogIndex.atEnd()) {
            results.add(leapfrogIndex.key())
            leapfrogIndex.next()
        }

        assertEquals(listOf(8, 10, 12), results)
    }

    @Test
    fun `test AVL set iterator atEnd after exhaustion`() {
        val avlSet = createAVLSet(1, 2, 3)
        val index = AVLIndex.AVLSetIndex(avlSet)
        val varOrder = listOf(Symbol.intern("x"))
        val vars = setOf(Symbol.intern("x"))

        val leapfrogIndex = AVLLeapfrogIndex(index, varOrder, vars)

        assertFalse(leapfrogIndex.atEnd())
        leapfrogIndex.next()
        assertFalse(leapfrogIndex.atEnd())
        leapfrogIndex.next()
        assertFalse(leapfrogIndex.atEnd())
        leapfrogIndex.next()
        assertTrue(leapfrogIndex.atEnd())
    }

    @Test
    fun `test AVL set iterator key() throws when atEnd`() {
        val avlSet = createAVLSet(1)
        val index = AVLIndex.AVLSetIndex(avlSet)
        val varOrder = listOf(Symbol.intern("x"))
        val vars = setOf(Symbol.intern("x"))

        val leapfrogIndex = AVLLeapfrogIndex(index, varOrder, vars)

        leapfrogIndex.next()
        assertTrue(leapfrogIndex.atEnd())

        assertThrows<IllegalStateException> {
            leapfrogIndex.key()
        }
    }

    @Test
    fun `test AVL set iterator reinit`() {
        val avlSet = createAVLSet(1, 2, 3, 4)
        val index = AVLIndex.AVLSetIndex(avlSet)
        val varOrder = listOf(Symbol.intern("x"))
        val vars = setOf(Symbol.intern("x"))

        val leapfrogIndex = AVLLeapfrogIndex(index, varOrder, vars)

        // Consume first two elements
        assertEquals(1, leapfrogIndex.key())
        leapfrogIndex.next()
        assertEquals(2, leapfrogIndex.key())
        leapfrogIndex.next()

        // Reinit should reset to the beginning
        leapfrogIndex.reinit()

        val results = mutableListOf<Any>()
        while (!leapfrogIndex.atEnd()) {
            results.add(leapfrogIndex.key())
            leapfrogIndex.next()
        }

        assertEquals(listOf(1, 2, 3, 4), results)
    }

    @Test
    fun `test empty AVL set`() {
        val avlSet = createAVLSet()
        val index = AVLIndex.AVLSetIndex(avlSet)
        val varOrder = listOf(Symbol.intern("x"))
        val vars = setOf(Symbol.intern("x"))

        val leapfrogIndex = AVLLeapfrogIndex(index, varOrder, vars)

        assertTrue(leapfrogIndex.atEnd())
    }

    @Test
    fun `test seek past all elements`() {
        val avlSet = createAVLSet(1, 2, 3)
        val index = AVLIndex.AVLSetIndex(avlSet)
        val varOrder = listOf(Symbol.intern("x"))
        val vars = setOf(Symbol.intern("x"))

        val leapfrogIndex = AVLLeapfrogIndex(index, varOrder, vars)

        leapfrogIndex.seek(100)
        assertTrue(leapfrogIndex.atEnd())
    }

    @Test
    fun `test next() returns Unit when atEnd`() {
        val avlSet = createAVLSet(1)
        val index = AVLIndex.AVLSetIndex(avlSet)
        val varOrder = listOf(Symbol.intern("x"))
        val vars = setOf(Symbol.intern("x"))

        val leapfrogIndex = AVLLeapfrogIndex(index, varOrder, vars)

        assertEquals(1, leapfrogIndex.key())
        val result = leapfrogIndex.next()

        assertTrue(leapfrogIndex.atEnd())
        assertEquals(Unit, result)
    }

    @Test
    fun `test seek to exact match`() {
        val avlSet = createAVLSet(2, 4, 6, 8, 10)
        val index = AVLIndex.AVLSetIndex(avlSet)
        val varOrder = listOf(Symbol.intern("x"))
        val vars = setOf(Symbol.intern("x"))

        val leapfrogIndex = AVLLeapfrogIndex(index, varOrder, vars)

        leapfrogIndex.seek(6)
        assertFalse(leapfrogIndex.atEnd())
        assertEquals(6, leapfrogIndex.key())
    }

    @Test
    fun `test seek between elements`() {
        val avlSet = createAVLSet(2, 4, 6, 8, 10)
        val index = AVLIndex.AVLSetIndex(avlSet)
        val varOrder = listOf(Symbol.intern("x"))
        val vars = setOf(Symbol.intern("x"))

        val leapfrogIndex = AVLLeapfrogIndex(index, varOrder, vars)

        leapfrogIndex.seek(5)
        assertFalse(leapfrogIndex.atEnd())
        assertEquals(6, leapfrogIndex.key())
    }

    // ============================================================
    // AVL Map Tests
    // ============================================================

    @Test
    fun `test AVL map iterator basic iteration`() {
        val avlMap = createAVLMap(1, "a", 2, "b", 3, "c")
        val index = AVLIndex.AVLMapIndex(avlMap)
        val varOrder = listOf(Symbol.intern("x"), Symbol.intern("y"))
        val vars = setOf(Symbol.intern("x"), Symbol.intern("y"))

        val leapfrogIndex = AVLLeapfrogIndex(index, varOrder, vars)

        val results = mutableListOf<Any>()
        while (!leapfrogIndex.atEnd()) {
            results.add(leapfrogIndex.key())
            leapfrogIndex.next()
        }

        assertEquals(listOf(1, 2, 3), results)
    }

    @Test
    fun `test AVL map iterator with seek`() {
        val avlMap = createAVLMap(2, "a", 4, "b", 6, "c", 8, "d", 10, "e")
        val index = AVLIndex.AVLMapIndex(avlMap)
        val varOrder = listOf(Symbol.intern("x"), Symbol.intern("y"))
        val vars = setOf(Symbol.intern("x"), Symbol.intern("y"))

        val leapfrogIndex = AVLLeapfrogIndex(index, varOrder, vars)

        leapfrogIndex.seek(5)

        val results = mutableListOf<Any>()
        while (!leapfrogIndex.atEnd()) {
            results.add(leapfrogIndex.key())
            leapfrogIndex.next()
        }

        assertEquals(listOf(6, 8, 10), results)
    }

    @Test
    fun `test AVL map iterator atEnd after exhaustion`() {
        val avlMap = createAVLMap(1, "a", 2, "b", 3, "c")
        val index = AVLIndex.AVLMapIndex(avlMap)
        val varOrder = listOf(Symbol.intern("x"), Symbol.intern("y"))
        val vars = setOf(Symbol.intern("x"), Symbol.intern("y"))

        val leapfrogIndex = AVLLeapfrogIndex(index, varOrder, vars)

        assertFalse(leapfrogIndex.atEnd())
        leapfrogIndex.next()
        assertFalse(leapfrogIndex.atEnd())
        leapfrogIndex.next()
        assertFalse(leapfrogIndex.atEnd())
        leapfrogIndex.next()
        assertTrue(leapfrogIndex.atEnd())
    }

    @Test
    fun `test AVL map iterator key() throws when atEnd`() {
        val avlMap = createAVLMap(1, "a")
        val index = AVLIndex.AVLMapIndex(avlMap)
        val varOrder = listOf(Symbol.intern("x"), Symbol.intern("y"))
        val vars = setOf(Symbol.intern("x"), Symbol.intern("y"))

        val leapfrogIndex = AVLLeapfrogIndex(index, varOrder, vars)

        leapfrogIndex.next()
        assertTrue(leapfrogIndex.atEnd())

        assertThrows<IllegalStateException> {
            leapfrogIndex.key()
        }
    }

    @Test
    fun `test AVL map iterator reinit`() {
        val avlMap = createAVLMap(1, "a", 2, "b", 3, "c", 4, "d")
        val index = AVLIndex.AVLMapIndex(avlMap)
        val varOrder = listOf(Symbol.intern("x"), Symbol.intern("y"))
        val vars = setOf(Symbol.intern("x"), Symbol.intern("y"))

        val leapfrogIndex = AVLLeapfrogIndex(index, varOrder, vars)

        // Consume first two elements
        assertEquals(1, leapfrogIndex.key())
        leapfrogIndex.next()
        assertEquals(2, leapfrogIndex.key())
        leapfrogIndex.next()

        // Reinit should reset to the beginning
        leapfrogIndex.reinit()

        val results = mutableListOf<Any>()
        while (!leapfrogIndex.atEnd()) {
            results.add(leapfrogIndex.key())
            leapfrogIndex.next()
        }

        assertEquals(listOf(1, 2, 3, 4), results)
    }

    @Test
    fun `test empty AVL map`() {
        val avlMap = createAVLMap()
        val index = AVLIndex.AVLMapIndex(avlMap)
        val varOrder = listOf(Symbol.intern("x"), Symbol.intern("y"))
        val vars = setOf(Symbol.intern("x"), Symbol.intern("y"))

        val leapfrogIndex = AVLLeapfrogIndex(index, varOrder, vars)

        assertTrue(leapfrogIndex.atEnd())
    }

    @Test
    fun `test AVL map seek past all elements`() {
        val avlMap = createAVLMap(1, "a", 2, "b", 3, "c")
        val index = AVLIndex.AVLMapIndex(avlMap)
        val varOrder = listOf(Symbol.intern("x"), Symbol.intern("y"))
        val vars = setOf(Symbol.intern("x"), Symbol.intern("y"))

        val leapfrogIndex = AVLLeapfrogIndex(index, varOrder, vars)

        leapfrogIndex.seek(100)
        assertTrue(leapfrogIndex.atEnd())
    }

    @Test
    fun `test AVL map next() returns Unit when atEnd`() {
        val avlMap = createAVLMap(1, "a")
        val index = AVLIndex.AVLMapIndex(avlMap)
        val varOrder = listOf(Symbol.intern("x"), Symbol.intern("y"))
        val vars = setOf(Symbol.intern("x"), Symbol.intern("y"))

        val leapfrogIndex = AVLLeapfrogIndex(index, varOrder, vars)

        assertEquals(1, leapfrogIndex.key())
        val result = leapfrogIndex.next()

        assertTrue(leapfrogIndex.atEnd())
        assertEquals(Unit, result)
    }

    @Test
    fun `test AVL map seek to exact match`() {
        val avlMap = createAVLMap(2, "a", 4, "b", 6, "c", 8, "d", 10, "e")
        val index = AVLIndex.AVLMapIndex(avlMap)
        val varOrder = listOf(Symbol.intern("x"), Symbol.intern("y"))
        val vars = setOf(Symbol.intern("x"), Symbol.intern("y"))

        val leapfrogIndex = AVLLeapfrogIndex(index, varOrder, vars)

        leapfrogIndex.seek(6)
        assertFalse(leapfrogIndex.atEnd())
        assertEquals(6, leapfrogIndex.key())
    }

    @Test
    fun `test AVL map seek between elements`() {
        val avlMap = createAVLMap(2, "a", 4, "b", 6, "c", 8, "d", 10, "e")
        val index = AVLIndex.AVLMapIndex(avlMap)
        val varOrder = listOf(Symbol.intern("x"), Symbol.intern("y"))
        val vars = setOf(Symbol.intern("x"), Symbol.intern("y"))

        val leapfrogIndex = AVLLeapfrogIndex(index, varOrder, vars)

        leapfrogIndex.seek(5)
        assertFalse(leapfrogIndex.atEnd())
        assertEquals(6, leapfrogIndex.key())
    }

    @Test
    fun `test AVL map with nested sets - openLevel and closeLevel`() {
        // Create a map: 1 -> {a, b, c}, 2 -> {d, e}
        val innerSet1 = createAVLSet("a", "b", "c")
        val innerSet2 = createAVLSet("d", "e")
        val avlMap = createAVLMap(1, innerSet1, 2, innerSet2)

        val index = AVLIndex.AVLMapIndex(avlMap)
        val varOrder = listOf(Symbol.intern("x"), Symbol.intern("y"))
        val vars = setOf(Symbol.intern("x"), Symbol.intern("y"))

        val leapfrogIndex = AVLLeapfrogIndex(index, varOrder, vars)

        // At level 0, should see keys 1, 2
        assertEquals(0, leapfrogIndex.level())
        assertEquals(1, leapfrogIndex.key())

        // Open level to see nested set
        leapfrogIndex.openLevel(listOf(1))
        assertEquals(1, leapfrogIndex.level())

        // Should now iterate through nested set {a, b, c}
        val innerResults = mutableListOf<Any>()
        while (!leapfrogIndex.atEnd()) {
            innerResults.add(leapfrogIndex.key())
            leapfrogIndex.next()
        }
        assertEquals(listOf("a", "b", "c"), innerResults)

        // Close level back to 0
        leapfrogIndex.closeLevel()
        assertEquals(0, leapfrogIndex.level())

        // Move to next key at level 0
        leapfrogIndex.next()
        assertEquals(2, leapfrogIndex.key())
    }

    @Test
    fun `test AVL map participatesInLevel`() {
        val avlMap = createAVLMap(1, "a", 2, "b")
        val index = AVLIndex.AVLMapIndex(avlMap)
        val varOrder = listOf(Symbol.intern("x"), Symbol.intern("y"))
        val vars = setOf(Symbol.intern("x"), Symbol.intern("y"))

        val leapfrogIndex = AVLLeapfrogIndex(index, varOrder, vars)

        // Both x and y are in the variable set
        assertTrue(leapfrogIndex.participatesInLevel(0))
        assertTrue(leapfrogIndex.participatesInLevel(1))
    }

    @Test
    fun `test AVL map maxLevel`() {
        val avlMap = createAVLMap(1, "a", 2, "b")
        val index = AVLIndex.AVLMapIndex(avlMap)
        val varOrder = listOf(Symbol.intern("x"), Symbol.intern("y"))
        val vars = setOf(Symbol.intern("x"), Symbol.intern("y"))

        val leapfrogIndex = AVLLeapfrogIndex(index, varOrder, vars)

        assertEquals(2, leapfrogIndex.maxLevel())
    }
}
