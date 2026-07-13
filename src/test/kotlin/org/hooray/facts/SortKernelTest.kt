package org.hooray.facts

import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class SortKernelTest {

    /**
     * Oracle for the sort kernels: distinct `(group, value)` pairs in sorted order, the
     * lists sealed per distinct group, and each input position's group rewritten to the
     * output index of its pair.
     */
    private fun oracle(
        items: Terms,
        groups: IntArray,
        indexs: IntArray,
    ): Pair<List<List<List<Int>>>, IntArray> {
        val pairs = (0 until groups.size).map {
            Pair(groups[it], bytesToInts(items.itemToByteArray(indexs[it])))
        }
        val distinct = pairs.distinct()
            .sortedWith(compareBy<Pair<Int, List<Int>>> { it.first }.thenBy(termOrder) { it.second })
        val lists = distinct.groupBy { it.first }.toSortedMap().values.map { group -> group.map { it.second } }
        val itemIndex = distinct.withIndex().associate { (index, pair) -> pair to index }
        val rewritten = IntArray(groups.size) { itemIndex.getValue(pairs[it]) }
        return Pair(lists, rewritten)
    }

    private fun checkAgainstOracle(items: Terms, groups: IntArray, sorter: (Terms, IntArray, IntArray, Boolean) -> Layer) {
        val indexs = IntArray(groups.size) { it }
        val (expectedLists, expectedGroups) = oracle(items, groups, indexs)

        val notLastGroups = groups.copyOf()
        val layer = sorter(items, notLastGroups, indexs, false)
        assertEquals(expectedLists, layerToLists(layer))
        assertArrayEquals(expectedGroups, notLastGroups)

        val lastGroups = groups.copyOf()
        val layerLast = sorter(items, lastGroups, indexs, true)
        assertEquals(expectedLists, layerToLists(layerLast))
        // The groups write-back is suppressed for the last layer.
        assertArrayEquals(groups, lastGroups)
    }

    @Test
    fun `colSort matches the oracle on mixed width data`() {
        val next = lcg(3)
        val items = Terms()
        val count = 200
        repeat(count) {
            val len = (next() % 5).toInt()
            items.pushItem(ByteArray(len) { (next() % 4).toByte() })
        }
        val groups = IntArray(count) { (next() % 8).toInt() }
        checkAgainstOracle(items, groups, Kernels::colSort)
    }

    @Test
    fun `colSort matches the oracle on width one data`() {
        val next = lcg(4)
        val items = Terms()
        val count = 150
        repeat(count) { items.pushItem(byteArrayOf((next() % 8).toByte())) }
        assertEquals(1, items.bounds.strided())
        val groups = IntArray(count) { (next() % 5).toInt() }
        checkAgainstOracle(items, groups, Kernels::colSort)
    }

    @Test
    fun `u32Sort matches the oracle and the generic path on width four data`() {
        val next = lcg(5)
        val items = Terms()
        val count = 300
        repeat(count) {
            items.pushU32BE(((next() shl 17) xor next()).toInt())
        }
        assertEquals(4, items.bounds.strided())
        val groups = IntArray(count) { (next() % 8).toInt() }
        checkAgainstOracle(items, groups, Kernels::u32Sort)

        // The dispatched and generic paths agree, both in output and groups rewrite.
        val indexs = IntArray(count) { it }
        val radixGroups = groups.copyOf()
        val genericGroups = groups.copyOf()
        val radix = Kernels.sort(items, radixGroups, indexs, false)
        val generic = Kernels.colSort(items, genericGroups, indexs, false)
        assertEquals(layerToLists(generic), layerToLists(radix))
        assertArrayEquals(genericGroups, radixGroups)
    }

    @Test
    fun `empty input yields one empty list`() {
        val layer = Kernels.colSort(Terms(), IntArray(0), IntArray(0), false)
        assertEquals(1, layer.size)
        assertEquals(0, layer.itemCount)
    }
}
