package org.hooray.facts

/**
 * A growable [IntArray].
 *
 * The backing [array] is exposed (read-only setter) so hot kernels can index it directly;
 * callers must only read positions below [size].
 */
internal class IntList(initialCapacity: Int = 16) {
    var array = IntArray(initialCapacity)
        private set
    var size = 0
        private set

    fun push(value: Int) {
        if (size == array.size) array = array.copyOf(maxOf(2 * array.size, 4))
        array[size] = value
        size += 1
    }

    operator fun get(index: Int): Int = array[index]

    operator fun set(index: Int, value: Int) {
        array[index] = value
    }

    fun pop(): Int {
        size -= 1
        return array[size]
    }

    fun isEmpty(): Boolean = size == 0

    fun clear() {
        size = 0
    }

    fun truncate(newSize: Int) {
        size = newSize
    }

    fun toIntArray(): IntArray = array.copyOf(size)
}
