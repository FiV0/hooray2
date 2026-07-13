package org.hooray.facts

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class FactLSMTest {

    /** Exactly `count` distinct single-column facts, disjoint per `from`. */
    private fun distinctFacts(from: Int, count: Int): List<TestFact> =
        (from until from + count).map { listOf<Any?>(it.toLong()) }

    @Test
    fun `push maintains the doubling invariant`() {
        val lsm = FactLSM()
        lsm.push(forestOf(distinctFacts(0, 5)))
        lsm.push(forestOf(distinctFacts(10, 4)))
        // 5 < 2 * 4, so the two collapse into one layer.
        assertEquals(1, lsm.contents().size)
        assertEquals(9, lsm.contents()[0].len())

        lsm.push(forestOf(distinctFacts(20, 3)))
        // 9 >= 2 * 3, so both layers stay.
        assertEquals(2, lsm.contents().size)
        val sizes = lsm.contents().map { it.len() }
        assertTrue(sizes[0] >= 2 * sizes[1], "layers must double in size: $sizes")
    }

    @Test
    fun `flatten merges to the union of all facts`() {
        val xs = longTuples(100, 2, 3)
        val ys = longTuples(100, 2, 4)
        val lsm = FactLSM()
        lsm.push(forestOf(xs))
        lsm.push(forestOf(ys))
        assertEquals(xs.toSet() union ys.toSet(), extractLsm(lsm))
        assertTrue(lsm.isEmpty(), "flatten takes the merged layer")
    }

    @Test
    fun `tidyThrough merges the layers below the size`() {
        val lsm = FactLSM()
        lsm.push(forestOf(distinctFacts(0, 12)))
        lsm.push(forestOf(distinctFacts(50, 3)))
        assertEquals(2, lsm.contents().size)

        // The second-smallest layer (12) is not below 4: nothing merges.
        lsm.tidyThrough(4)
        assertEquals(2, lsm.contents().size)

        // The second-smallest layer is below 13: everything merges.
        lsm.tidyThrough(13)
        assertEquals(1, lsm.contents().size)
        assertEquals(15, lsm.contents()[0].len())
    }

    @Test
    fun `pushing mismatched arities throws`() {
        val lsm = FactLSM()
        lsm.push(forestOf(listOf(listOf<Any?>(1L))))
        assertThrows(IllegalArgumentException::class.java) {
            lsm.push(forestOf(listOf(listOf<Any?>(1L, 2L))))
        }
    }
}
