package org.hooray.facts

/**
 * A queue of union alignment reports, mirroring datatoad's `VecDeque<Report>`.
 *
 * A sequence of reports describes an ordered traversal of the lists of two layers:
 * `This`/`That` carry an index range exclusive to one input, and `Both` a pair of
 * matching indexes, one in each input. Each report is packed into one Long — a 2-bit
 * tag and two 31-bit non-negative values — to keep the union kernel's hot loop free
 * of allocation.
 */
internal class ReportQueue {
    private var buffer = LongArray(16) // capacity always a power of two
    private var head = 0
    private var count = 0

    val size: Int get() = count

    fun isEmpty(): Boolean = count == 0

    fun pushThis(lower: Int, upper: Int) = push(pack(TAG_THIS, lower, upper))

    fun pushThat(lower: Int, upper: Int) = push(pack(TAG_THAT, lower, upper))

    fun pushBoth(index0: Int, index1: Int) = push(pack(TAG_BOTH, index0, index1))

    fun pop(): Long {
        val report = buffer[head]
        head = (head + 1) and (buffer.size - 1)
        count -= 1
        return report
    }

    private fun push(report: Long) {
        if (count == buffer.size) grow()
        buffer[(head + count) and (buffer.size - 1)] = report
        count += 1
    }

    private fun grow() {
        val next = LongArray(2 * buffer.size)
        val mask = buffer.size - 1
        for (i in 0 until count) next[i] = buffer[(head + i) and mask]
        buffer = next
        head = 0
    }

    companion object {
        const val TAG_THIS = 0
        const val TAG_THAT = 1
        const val TAG_BOTH = 2

        private fun pack(tag: Int, first: Int, second: Int): Long =
            (tag.toLong() shl 62) or (first.toLong() shl 31) or second.toLong()

        fun tag(report: Long): Int = (report ushr 62).toInt()

        fun first(report: Long): Int = ((report ushr 31) and 0x7FFF_FFFFL).toInt()

        fun second(report: Long): Int = (report and 0x7FFF_FFFFL).toInt()
    }
}
