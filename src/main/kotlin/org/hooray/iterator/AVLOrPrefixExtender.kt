package org.hooray.iterator

import org.hooray.UniversalComparator
import org.hooray.algo.Extension
import org.hooray.algo.Prefix
import java.util.*

class AVLOrPrefixExtender(children: List<AVLPrefixExtender>, totalLevels: Int): GenericOrPrefixExtender(children, totalLevels) {

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
        val nodes = nodesForPrefix(prefix)
        val results = mutableListOf<List<Extension>>()
        for ((idx, child) in children.withIndex()) {
            val node = nodes[idx] ?: continue
            val childProposals = child.propose(prefix)
            for (proposal in childProposals) {
                node.insert(proposal)
            }
            results.add(childProposals)
        }
        return mergeSortedLists(results)
    }

    override fun intersect(prefix: Prefix, extensions: List<Extension>): List<Extension> {
        val nodes = nodesForPrefix(prefix)
        val results = mutableListOf<List<Extension>>()
        for ((idx, child) in children.withIndex()) {
            val node = nodes[idx] ?: continue
            val childExtensions = child.intersect(prefix, extensions)
            for (extension in childExtensions) {
                node.insert(extension)
            }
            results.add(childExtensions)
        }
        return mergeSortedLists(results)
    }
}
