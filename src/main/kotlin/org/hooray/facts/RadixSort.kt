package org.hooray.facts

/**
 * Least-significant-byte radix sort for Long keys with an optional parallel Int payload.
 *
 * Mirrors datatoad's `radix_sort::lsb_range` on byte-array rows: byte `index` of a key is
 * `(key ushr ((7 - index) * 8)) and 0xFF`, so byte 0 is the most significant and the sort
 * orders keys as unsigned 64-bit values. Only byte positions in `[lowerByte, upperByte)`
 * participate, and positions where all keys agree are skipped.
 */
internal object RadixSort {

    fun lsbRange(keys: LongArray, payload: IntArray?, lowerByte: Int, upperByte: Int) {
        val counts = Array(8) { IntArray(256) }
        for (key in keys) {
            for (index in lowerByte until upperByte) {
                counts[index][byteOf(key, index)] += 1
            }
        }
        val active = BooleanArray(8)
        for (index in lowerByte until upperByte) {
            var distinct = 0
            for (count in counts[index]) if (count > 0) distinct += 1
            active[index] = distinct > 1
        }

        var src = keys
        var dst = LongArray(keys.size)
        var srcPayload = payload
        var dstPayload = if (payload != null) IntArray(payload.size) else null

        var passes = 0
        for (index in 7 downTo 0) {
            if (!active[index]) continue
            val count = counts[index]
            var total = 0
            for (byte in 0 until 256) {
                val c = count[byte]
                count[byte] = total
                total += c
            }
            for (i in src.indices) {
                val key = src[i]
                val target = count[byteOf(key, index)]
                count[byteOf(key, index)] = target + 1
                dst[target] = key
                if (srcPayload != null) dstPayload!![target] = srcPayload[i]
            }
            val tmp = src; src = dst; dst = tmp
            if (payload != null) {
                val tmpPayload = srcPayload; srcPayload = dstPayload; dstPayload = tmpPayload
            }
            passes += 1
        }

        // An odd number of scatter passes leaves the result in the temp arrays.
        if (passes % 2 == 1) {
            System.arraycopy(src, 0, keys, 0, keys.size)
            if (payload != null) System.arraycopy(srcPayload!!, 0, payload, 0, payload.size)
        }
    }

    private fun byteOf(key: Long, index: Int): Int =
        ((key ushr ((7 - index) * 8)) and 0xFF).toInt()
}
