package org.hooray.facts

import clojure.lang.Keyword
import clojure.lang.PersistentVector
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class SortKernelTest {

    /**
     * Oracle for the sort kernel: distinct `(group, value)` pairs in sorted order, the
     * lists sealed per distinct group, and each input position's group rewritten to the
     * output index of its pair. `distinct()` is equals-based, so the oracle only holds
     * on domains where `equals ⟺ compare == 0` (as here — see TestSupport).
     */
    private fun oracle(
        items: Terms,
        groups: IntArray,
        indexs: IntArray,
    ): Pair<List<List<Any?>>, IntArray> {
        val pairs = (0 until groups.size).map {
            Pair(groups[it], items[indexs[it]])
        }
        val distinct = pairs.distinct()
            .sortedWith(compareBy<Pair<Int, Any?>> { it.first }.thenBy(termOrder) { it.second })
        val lists = distinct.groupBy { it.first }.toSortedMap().values.map { group -> group.map { it.second } }
        val itemIndex = distinct.withIndex().associate { (index, pair) -> pair to index }
        val rewritten = IntArray(groups.size) { itemIndex.getValue(pairs[it]) }
        return Pair(lists, rewritten)
    }

    private fun checkAgainstOracle(items: Terms, groups: IntArray) {
        val indexs = IntArray(groups.size) { it }
        val (expectedLists, expectedGroups) = oracle(items, groups, indexs)

        val notLastGroups = groups.copyOf()
        val layer = Kernels.sort(items, notLastGroups, indexs, false)
        assertEquals(expectedLists, layerToLists(layer))
        assertArrayEquals(expectedGroups, notLastGroups)

        val lastGroups = groups.copyOf()
        val layerLast = Kernels.sort(items, lastGroups, indexs, true)
        assertEquals(expectedLists, layerToLists(layerLast))
        // The groups write-back is suppressed for the last layer.
        assertArrayEquals(groups, lastGroups)
    }

    @Test
    fun `sort matches the oracle on uniformly typed data`() {
        val next = lcg(3)
        val items = Terms()
        val count = 200
        repeat(count) { items.pushItem(next() % 8) }
        val groups = IntArray(count) { (next() % 8).toInt() }
        checkAgainstOracle(items, groups)
    }

    @Test
    fun `sort matches the oracle on mixed type data`() {
        val next = lcg(4)
        val items = Terms()
        val count = 200
        repeat(count) {
            items.pushItem(
                when ((next() % 5).toInt()) {
                    0 -> null
                    1 -> next() % 8
                    2 -> "s${next() % 8}"
                    3 -> Keyword.intern("k${next() % 8}")
                    else -> PersistentVector.create(List((next() % 3).toInt()) { next() % 4 })
                }
            )
        }
        val groups = IntArray(count) { (next() % 5).toInt() }
        checkAgainstOracle(items, groups)
    }

    @Test
    fun `numerically equal cross-type items deduplicate to the earliest`() {
        val items = Terms()
        items.pushItem(1.0)
        items.pushItem(1)
        items.pushItem(1L)
        val groups = IntArray(3)
        val layer = Kernels.sort(items, groups, IntArray(3) { it }, false)
        assertEquals(listOf(listOf<Any?>(1.0)), layerToLists(layer))
        assertArrayEquals(intArrayOf(0, 0, 0), groups)
    }

    @Test
    fun `empty input yields one empty list`() {
        val layer = Kernels.sort(Terms(), IntArray(0), IntArray(0), false)
        assertEquals(1, layer.size)
        assertEquals(0, layer.itemCount)
    }
}
