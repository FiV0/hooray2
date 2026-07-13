package org.hooray.facts

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class UnionKernelTest {

    private fun layerOfLists(vararg lists: List<Any?>): Layer {
        val layer = Layer()
        for (list in lists) {
            for (item in list) layer.values.pushItem(item)
            layer.bounds.push(layer.values.size)
        }
        return layer
    }

    private fun assertReport(report: Long, tag: Int, first: Int, second: Int) {
        assertEquals(tag, ReportQueue.tag(report))
        assertEquals(first, ReportQueue.first(report))
        assertEquals(second, ReportQueue.second(report))
    }

    @Test
    fun `both arm merges lists and refills run reports`() {
        val layer0 = layerOfLists(listOf(1L, 3L))
        val layer1 = layerOfLists(listOf(2L, 3L, 4L))
        val reports = ReportQueue()
        reports.pushBoth(0, 0)

        val merged = Kernels.union(layer0, layer1, reports, next = true)

        assertEquals(listOf(listOf<Any?>(1L, 2L, 3L, 4L)), layerToLists(merged))
        assertEquals(4, reports.size)
        assertReport(reports.pop(), ReportQueue.TAG_THIS, 0, 1) // the run [1]
        assertReport(reports.pop(), ReportQueue.TAG_THAT, 0, 1) // the run [2]
        assertReport(reports.pop(), ReportQueue.TAG_BOTH, 1, 1) // the match 3
        assertReport(reports.pop(), ReportQueue.TAG_THAT, 2, 3) // the tail [4]
    }

    @Test
    fun `gallop covers runs of consecutive items`() {
        val layer0 = layerOfLists(listOf(1L, 2L, 3L, 4L, 5L))
        val layer1 = layerOfLists(listOf(4L))
        val reports = ReportQueue()
        reports.pushBoth(0, 0)

        val merged = Kernels.union(layer0, layer1, reports, next = true)

        assertEquals(listOf(listOf<Any?>(1L, 2L, 3L, 4L, 5L)), layerToLists(merged))
        assertEquals(3, reports.size)
        assertReport(reports.pop(), ReportQueue.TAG_THIS, 0, 3) // the run [1, 2, 3]
        assertReport(reports.pop(), ReportQueue.TAG_BOTH, 3, 0) // the match 4
        assertReport(reports.pop(), ReportQueue.TAG_THIS, 4, 5) // the tail [5]
    }

    @Test
    fun `this arm copies whole lists and reports their item range`() {
        val layer0 = layerOfLists(
            listOf(1L),
            listOf(2L, 3L),
        )
        val layer1 = layerOfLists(listOf(9L))
        val reports = ReportQueue()
        reports.pushThis(0, 2)

        val merged = Kernels.union(layer0, layer1, reports, next = true)

        assertEquals(listOf(listOf<Any?>(1L), listOf<Any?>(2L, 3L)), layerToLists(merged))
        assertEquals(1, reports.size)
        assertReport(reports.pop(), ReportQueue.TAG_THIS, 0, 3)
    }

    @Test
    fun `without next the queue is left empty`() {
        val layer0 = layerOfLists(listOf(1L, 3L))
        val layer1 = layerOfLists(listOf(2L))
        val reports = ReportQueue()
        reports.pushBoth(0, 0)

        val merged = Kernels.union(layer0, layer1, reports, next = false)

        assertEquals(listOf(listOf<Any?>(1L, 2L, 3L)), layerToLists(merged))
        assertEquals(0, reports.size)
    }
}
