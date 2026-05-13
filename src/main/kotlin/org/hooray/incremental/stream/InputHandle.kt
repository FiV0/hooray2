package org.hooray.incremental.stream

/**
 * External write boundary for a Circuit's input value.
 *
 * Holds at most one pending value at a time: `set` overwrites; `clear`
 * empties the slot; `takeOrEmpty` returns the pending value (or a caller-
 * supplied default) and clears the slot. A boxed slot lets the handle
 * distinguish "no value set" from "set to null" when T is nullable.
 */
class InputHandle<T> {
    private data class Box<T>(val value: T)

    private var slot: Box<T>? = null

    fun set(value: T) {
        slot = Box(value)
    }

    fun clear() {
        slot = null
    }

    fun takeOrEmpty(default: T): T {
        val box = slot
        slot = null
        return if (box == null) default else box.value
    }
}
