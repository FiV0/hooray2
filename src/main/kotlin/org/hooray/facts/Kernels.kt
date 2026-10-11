package org.hooray.facts

/**
 * Layer-at-a-time kernels, port of datatoad's `facts::trie::layers` (plus `gallop`).
 *
 * Each kernel works on the values of one layer and communicates with adjacent layers
 * only through type-erased state: report queues, flattened `(lower, upper)` bounds
 * pairs in an `IntArray [l0, u0, l1, u1, …]`, and grouping identifiers. Where datatoad
 * threads a rotating `VecDeque<bool>` through `retain_items`, these kernels use a
 * per-item input array and a per-list output array instead.
 */
internal object Kernels {

    private fun interface IntComparator {
        fun compare(a: Int, b: Int): Int
    }

    /**
     * Sorts the items of a column at `indexs` subject to the grouping/ordering of
     * `groups`, minting a new item for each distinct `(group, item)` pair and a new list
     * for each distinct group, rewriting `groups` with output item indexes unless `last`.
     *
     * Mirrors Rust's generic `col_sort` (trie.rs:1184-1209); datatoad's byte-width
     * dispatch (radix and fixed-width paths) does not apply to object items.
     */
    fun sort(items: Terms, groups: IntArray, indexs: IntArray, last: Boolean): Layer {
        require(groups.size == indexs.size)
        val output = Layer()
        val n = groups.size
        if (n == 0) {
            // Rust pushes the final list bound even for empty input.
            output.bounds.push(0)
            return output
        }

        val order = IntArray(n) { it }
        val comparator = IntComparator { a, b ->
            val byGroup = groups[a].compareTo(groups[b])
            if (byGroup != 0) byGroup else items.compareItems(indexs[a], items, indexs[b])
        }
        // A stable sort by (group, value) matches Rust's unstable sort of
        // (group, value, position) triples: ties keep their position order.
        mergeSort(order, IntArray(n), 0, n, comparator)

        // Mint a new item for each distinct (group, value); seal a list per distinct group.
        // In-place groups rewrite is safe: `order` is a bijection, and each slot is read
        // (for the boundary tests) before it is written, at its own step.
        var prev = order[0]
        var prevGroup = groups[prev]
        output.values.pushItemFrom(items, indexs[prev])
        if (!last) groups[prev] = 0
        for (p in 1 until n) {
            val element = order[p]
            val group = groups[element]
            if (prevGroup != group) {
                output.bounds.push(output.values.size)
                output.values.pushItemFrom(items, indexs[element])
            } else if (!items.itemsEqual(indexs[element], items, indexs[prev])) {
                output.values.pushItemFrom(items, indexs[element])
            }
            if (!last) groups[element] = output.values.size - 1
            prev = element
            prevGroup = group
        }
        output.bounds.push(output.values.size)
        return output
    }

    /**
     * Merges two layers using the alignment information in `reports`, refilling the
     * queue with next-layer reports iff `next` (Rust `union`, trie.rs:987-1059).
     */
    fun union(layer0: Layer, layer1: Layer, reports: ReportQueue, next: Boolean): Layer {
        val output = Layer()
        repeat(reports.size) {
            val report = reports.pop()
            when (ReportQueue.tag(report)) {
                ReportQueue.TAG_THIS -> {
                    val lower0 = ReportQueue.first(report)
                    val upper0 = ReportQueue.second(report)
                    output.extendFromSelf(layer0, lower0, upper0)
                    if (next) reports.pushThis(layer0.listLower(lower0), layer0.listUpper(upper0 - 1))
                }
                ReportQueue.TAG_THAT -> {
                    val lower1 = ReportQueue.first(report)
                    val upper1 = ReportQueue.second(report)
                    output.extendFromSelf(layer1, lower1, upper1)
                    if (next) reports.pushThat(layer1.listLower(lower1), layer1.listUpper(upper1 - 1))
                }
                else -> {
                    val index0 = ReportQueue.first(report)
                    val index1 = ReportQueue.second(report)
                    var lower0 = layer0.listLower(index0)
                    val upper0 = layer0.listUpper(index0)
                    var lower1 = layer1.listLower(index1)
                    val upper1 = layer1.listUpper(index1)

                    // Scour the intersecting range for matches.
                    while (lower0 < upper0 && lower1 < upper1) {
                        val cmp = layer0.values.compareItems(lower0, layer1.values, lower1)
                        when {
                            cmp < 0 -> {
                                val start = lower0
                                lower0 = gallopLess(layer0.values, lower0 + 1, upper0, layer1.values, lower1)
                                if (next) reports.pushThis(start, lower0)
                                output.values.extendFromRange(layer0.values, start, lower0)
                            }
                            cmp == 0 -> {
                                if (next) reports.pushBoth(lower0, lower1)
                                output.values.extendFromRange(layer0.values, lower0, lower0 + 1)
                                lower0 += 1
                                lower1 += 1
                            }
                            else -> {
                                val start = lower1
                                lower1 = gallopLess(layer1.values, lower1 + 1, upper1, layer0.values, lower0)
                                if (next) reports.pushThat(start, lower1)
                                output.values.extendFromRange(layer1.values, start, lower1)
                            }
                        }
                    }
                    if (lower0 < upper0) {
                        output.values.extendFromRange(layer0.values, lower0, upper0)
                        if (next) reports.pushThis(lower0, upper0)
                    }
                    if (lower1 < upper1) {
                        output.values.extendFromRange(layer1.values, lower1, upper1)
                        if (next) reports.pushThat(lower1, upper1)
                    }
                    output.bounds.push(output.values.size)
                }
            }
        }
        return output
    }

    /**
     * Intersects pairs of lists aligned by `both0`/`both1`, producing aligned item
     * indexes (Rust `intersection`, trie.rs:1069-1111).
     */
    fun intersect(layer0: Layer, layer1: Layer, both0: IntList, both1: IntList): Pair<IntList, IntList> {
        require(both0.size == both1.size)
        val match0 = IntList()
        val match1 = IntList()
        for (p in 0 until both0.size) {
            var lower0 = layer0.listLower(both0[p])
            val upper0 = layer0.listUpper(both0[p])
            var lower1 = layer1.listLower(both1[p])
            val upper1 = layer1.listUpper(both1[p])
            while (lower0 < upper0 && lower1 < upper1) {
                val cmp = layer0.values.compareItems(lower0, layer1.values, lower1)
                when {
                    cmp < 0 -> lower0 = gallopLess(layer0.values, lower0 + 1, upper0, layer1.values, lower1)
                    cmp == 0 -> {
                        match0.push(lower0)
                        match1.push(lower1)
                        lower0 += 1
                        lower1 += 1
                    }
                    else -> lower1 = gallopLess(layer1.values, lower1 + 1, upper1, layer0.values, lower0)
                }
            }
        }
        return Pair(match0, match1)
    }

    /**
     * Retains the list ranges in `bounds`, advancing each pair in place from list
     * bounds to item bounds (Rust `retain_lists`, trie.rs:1118-1130). Ranges must be
     * non-empty.
     */
    fun retainLists(layer: Layer, bounds: IntArray): Layer {
        val output = Layer()
        var p = 0
        while (p < bounds.size) {
            val lower = bounds[p]
            val upper = bounds[p + 1]
            output.extendFromSelf(layer, lower, upper)
            bounds[p] = layer.listLower(lower)
            bounds[p + 1] = layer.listUpper(upper - 1)
            p += 2
        }
        return output
    }

    /**
     * Retains the items flagged in `retainIn` (one per item), filling `retainOut` (one
     * per list) with whether each list kept any item; lists that kept none disappear
     * from the output (Rust `retain_items`, trie.rs:1137-1159).
     */
    fun retainItems(layer: Layer, retainIn: BooleanArray, retainOut: BooleanArray): Layer {
        require(retainIn.size == layer.itemCount)
        require(retainOut.size == layer.size)
        val output = Layer()
        for (list in 0 until layer.size) {
            for (item in layer.listLower(list) until layer.listUpper(list)) {
                if (retainIn[item]) output.values.pushItemFrom(layer.values, item)
            }
            if (output.values.size > output.bounds.last()) {
                output.bounds.push(output.values.size)
                retainOut[list] = true
            } else {
                retainOut[list] = false
            }
        }
        return output
    }

    /**
     * Compacts `active` to the item indexes whose value equals the corresponding
     * element of `other[aligned]` (Rust `filter_items`, trie.rs:1176-1180).
     */
    fun filterItems(items: Terms, active: IntList, other: Terms, aligned: IntArray) {
        require(active.size == aligned.size)
        var kept = 0
        for (j in 0 until active.size) {
            if (items.itemsEqual(active[j], other, aligned[j])) {
                active[kept] = active[j]
                kept += 1
            }
        }
        active.truncate(kept)
    }

    /**
     * Advances `(lower, upper]` bounds on lists to the corresponding bounds on items
     * (Rust `advance_bounds`, trie.rs:1162-1173).
     */
    fun advanceBounds(layer: Layer, bounds: IntArray) {
        val stride = layer.bounds.strided()
        if (stride >= 0) {
            if (stride > 1) {
                for (i in bounds.indices) bounds[i] *= stride
            }
        } else {
            var p = 0
            while (p < bounds.size) {
                val lower = bounds[p]
                val upper = bounds[p + 1]
                bounds[p] = layer.listLower(lower)
                bounds[p + 1] = layer.listUpper(upper - 1)
                p += 2
            }
        }
    }

    /** Advances bounds through each layer in turn (Rust `trie::advance_bounds`, trie.rs:78-80). */
    fun advanceBounds(layers: List<Layer>, bounds: IntArray) {
        for (layer in layers) advanceBounds(layer, bounds)
    }

    /**
     * Advances `lower` to just past the last item of `items` in `[lower, upper)` that is
     * less than item `otherIndex` of `other` (Rust `gallop`, mod.rs:620-639, with the
     * only predicate the trie kernels use).
     */
    fun gallopLess(items: Terms, lowerStart: Int, upper: Int, other: Terms, otherIndex: Int): Int {
        var lower = lowerStart
        if (lower < upper && items.compareItems(lower, other, otherIndex) < 0) {
            var step = 1
            while (lower + step < upper && items.compareItems(lower + step, other, otherIndex) < 0) {
                lower += step
                step = step shl 1
            }
            step = step shr 1
            while (step > 0) {
                if (lower + step < upper && items.compareItems(lower + step, other, otherIndex) < 0) {
                    lower += step
                }
                step = step shr 1
            }
            lower += 1
        }
        return lower
    }

    /** Stable merge sort of `array[from, until)` to avoid boxed comparators. */
    private fun mergeSort(array: IntArray, temp: IntArray, from: Int, until: Int, comparator: IntComparator) {
        if (until - from <= 16) {
            insertionSort(array, from, until, comparator)
            return
        }
        val mid = (from + until) ushr 1
        mergeSort(array, temp, from, mid, comparator)
        mergeSort(array, temp, mid, until, comparator)
        if (comparator.compare(array[mid - 1], array[mid]) <= 0) return
        var i = from
        var j = mid
        var k = from
        while (i < mid && j < until) {
            temp[k++] = if (comparator.compare(array[i], array[j]) <= 0) array[i++] else array[j++]
        }
        while (i < mid) temp[k++] = array[i++]
        while (j < until) temp[k++] = array[j++]
        System.arraycopy(temp, from, array, from, until - from)
    }

    private fun insertionSort(array: IntArray, from: Int, until: Int, comparator: IntComparator) {
        for (i in from + 1 until until) {
            val value = array[i]
            var j = i - 1
            while (j >= from && comparator.compare(array[j], value) > 0) {
                array[j + 1] = array[j]
                j -= 1
            }
            array[j + 1] = value
        }
    }
}
