package org.hooray.incremental.stream

import org.hooray.incremental.IndexedZSet
import org.hooray.incremental.IntegerWeight
import org.hooray.incremental.ResultZSet
import org.hooray.incremental.ZSet
import org.hooray.incremental.ZSetIndices
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test

class CircuitTest {
    private fun emptyIndices(): ZSetIndices =
        ZSetIndices(
            IndexedZSet.empty(IntegerWeight.ZERO, IntegerWeight.ONE),
            IndexedZSet.empty(IntegerWeight.ZERO, IntegerWeight.ONE)
        )

    @Test
    fun `input handle buffers exposes and clears pending input`() {
        val handle = InputHandle<ZSetIndices>()
        val input = emptyIndices()

        handle.set(input)

        assertEquals(input, handle.current())

        handle.clear()

        assertNull(handle.current())
    }

    @Test
    fun `step without pending input returns empty result`() {
        val circuit = Circuit(CircuitSpec(input = InputHandle()))

        assertEquals(ZSet.empty<Any>(), circuit.step())
    }

    @Test
    fun `step input is equivalent to setting the input handle before step`() {
        val direct = Circuit(CircuitSpec(input = InputHandle()))
        val viaHandle = Circuit(CircuitSpec(input = InputHandle()))
        val input = emptyIndices()

        val directResult: ResultZSet = direct.step(input)
        viaHandle.input.set(input)
        val handleResult: ResultZSet = viaHandle.step()

        assertEquals(directResult, handleResult)
        assertNull(direct.input.current())
        assertNull(viaHandle.input.current())
    }
}
