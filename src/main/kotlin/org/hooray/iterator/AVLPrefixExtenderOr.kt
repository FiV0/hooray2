package org.hooray.iterator

import org.hooray.UniversalComparator
import org.hooray.algo.Extension
import org.hooray.algo.Prefix
import java.util.*

class AVLPrefixExtenderOr(children: List<AVLPrefixExtender>): GenericPrefixExtenderOr(children) {

    private fun mergeSortedLists(lists: List<List<Extension>>): List<Extension> {
        val priorityQueue = PriorityQueue<Pair<Int, Int>>(
            { a, b -> UniversalComparator.compare(lists[a.first][a.second], lists[b.first][b.second]) })
        val result = mutableListOf<Extension>()
        for (i in lists.indices) {
            if (lists[i].isNotEmpty()) {
                priorityQueue.add(Pair(i, 0))
            }
        }
        while (priorityQueue.isNotEmpty()) {
            val (listIndex, elementIndex) = priorityQueue.poll()
            result.add(lists[listIndex][elementIndex])
            val nextElementIndex = elementIndex + 1
            if (nextElementIndex < lists[listIndex].size) {
                priorityQueue.add(Pair(listIndex, nextElementIndex))
            }
        }
        return result
    }

    override fun propose(prefix: Prefix): List<Extension> {
        val results = mutableListOf<List<Extension>>()
        for (child in children) {
            results.add(child.propose(prefix))
        }
        return mergeSortedLists(results)
    }

    override fun extend(prefix: Prefix, extensions: List<Extension>): List<Extension> {
        val results = mutableListOf<List<Extension>>()
        for (child in children) {
            results.add(child.extend(prefix, extensions))
        }
        return mergeSortedLists(results)
    }
}