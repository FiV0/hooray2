package org.hooray.facts

/**
 * A growable [ByteArray].
 *
 * The backing [array] is exposed (read-only setter) so hot kernels can index it directly;
 * callers must only read positions below [size].
 */
internal class ByteList(initialCapacity: Int = 32) {
    var array = ByteArray(initialCapacity)
        private set
    var size = 0
        private set

    fun push(value: Byte) {
        ensureCapacity(size + 1)
        array[size] = value
        size += 1
    }

    fun extendFrom(src: ByteArray, from: Int, until: Int) {
        val added = until - from
        if (added <= 0) return
        ensureCapacity(size + added)
        System.arraycopy(src, from, array, size, added)
        size += added
    }

    operator fun get(index: Int): Byte = array[index]

    fun clear() {
        size = 0
    }

    private fun ensureCapacity(needed: Int) {
        if (needed > array.size) {
            var capacity = maxOf(2 * array.size, 4)
            while (capacity < needed) capacity *= 2
            array = array.copyOf(capacity)
        }
    }
}
