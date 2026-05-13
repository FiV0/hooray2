package org.hooray.incremental.stream

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class InputHandleTest {

    @Test
    fun `takeOrEmpty without set returns default`() {
        val handle = InputHandle<String>()
        assertEquals("EMPTY", handle.takeOrEmpty("EMPTY"))
    }

    @Test
    fun `set then takeOrEmpty returns the set value and clears the slot`() {
        val handle = InputHandle<String>()
        handle.set("hello")
        assertEquals("hello", handle.takeOrEmpty("EMPTY"))
        // second take returns default — slot was cleared
        assertEquals("EMPTY", handle.takeOrEmpty("EMPTY"))
    }

    @Test
    fun `double set keeps the last value`() {
        val handle = InputHandle<String>()
        handle.set("first")
        handle.set("second")
        assertEquals("second", handle.takeOrEmpty("EMPTY"))
    }

    @Test
    fun `clear empties the slot before take`() {
        val handle = InputHandle<String>()
        handle.set("hello")
        handle.clear()
        assertEquals("EMPTY", handle.takeOrEmpty("EMPTY"))
    }

    @Test
    fun `handle holds nullable T values distinctly from empty`() {
        val handle = InputHandle<String?>()
        // empty -> default
        assertEquals("D", handle.takeOrEmpty("D"))
        // explicit null should be returned, not the default
        handle.set(null)
        assertEquals(null, handle.takeOrEmpty("D"))
        // now slot is cleared again
        assertEquals("D", handle.takeOrEmpty("D"))
    }
}
