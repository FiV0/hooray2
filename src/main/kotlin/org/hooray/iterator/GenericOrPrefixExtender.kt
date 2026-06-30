package org.hooray.iterator

import org.hooray.algo.Extension
import org.hooray.algo.Prefix
import org.hooray.algo.PrefixExtender
import org.hooray.util.Trie

internal fun saturatingSum(values: Iterable<Int>): Int {
    var total = 0
    for (value in values) {
        if (Int.MAX_VALUE - total <= value) {
            return Int.MAX_VALUE
        }
        total += value
    }
    return total
}

open class GenericOrPrefixExtender(val children: List<PrefixExtender>, totalLevels: Int) : PrefixExtender {

    private val childTries = List(children.size) { Trie<Any>() }
    private val levelSet: Set<Int>

    init {
        check(children.isNotEmpty()) { "At least one child extender is required" }
        levelSet = (0 until totalLevels).filter { level -> children.first().participatesInLevel(level) }.toSet()
    }

    private fun extractRelevantPrefix(prefix: Prefix) = prefix.filterIndexed { index, _ -> levelSet.contains(index) }

    protected fun nodeForChild(childIndex: Int, prefix: Prefix): Trie.Node<Any>? =
        childTries[childIndex].trieNodeFor(extractRelevantPrefix(prefix))

    override fun count(prefix: Prefix): Int {
        val nodes = children.indices.map { nodeForChild(it, prefix) }
        return saturatingSum(children.mapIndexed { index, extender -> if (nodes[index] != null) extender.count(prefix) else 0})
    }

    override fun propose(prefix: Prefix): List<Extension> {
        val nodes = children.indices.map { nodeForChild(it, prefix) }
        val childProposals = children.mapIndexed { index, extender -> if (nodes[index] != null) extender.propose(prefix) else emptyList()}
        for ((idx, proposals) in childProposals.withIndex()) {
            val node = nodes[idx] ?: continue
            for (proposal in proposals) {
                node.insert(proposal)
            }
        }
        // TODO the distinct call can likely be optimized to avoid large intermediate lists
        return childProposals.flatten().distinct()
    }

    override fun intersect(prefix: Prefix, extensions: List<Extension>): List<Extension> {
        val nodes = children.indices.map { nodeForChild(it, prefix) }
        val result = mutableListOf<Extension>()
        for ((idx, child) in children.withIndex()) {
            val node = nodes[idx] ?: continue
            val childExtensions = child.intersect(prefix, extensions)
            for (extension in childExtensions) {
                node.insert(extension)
            }
            result.addAll(childExtensions)
        }
        return result.distinct()
    }

    override fun participatesInLevel(level: Int): Boolean = levelSet.contains(level)
}
