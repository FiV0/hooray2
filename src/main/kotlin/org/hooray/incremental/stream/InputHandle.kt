package org.hooray.incremental.stream

class InputHandle<T> {
    private var pending: T? = null

    fun set(value: T) {
        pending = value
    }

    fun clear() {
        pending = null
    }

    fun current(): T? = pending

    internal fun take(): T? {
        val value = pending
        pending = null
        return value
    }
}
