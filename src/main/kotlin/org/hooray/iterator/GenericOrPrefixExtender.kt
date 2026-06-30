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

open class GenericOrPrefixExtender(val children: List<PrefixExtender>) : PrefixExtender {

    private val childTries = List(children.size) { Trie<Any>() }

    init {
        check(children.isNotEmpty()) { "At least one child extender is required" }
    }

    override fun count(prefix: Prefix): Int {
        val nodes = childTries.map { it.trieNodeFor(prefix) }
        return saturatingSum(children.mapIndexed { index, extender -> if (nodes[index] != null) extender.count(prefix) else 0})
    }

    override fun propose(prefix: Prefix): List<Extension> {
        val nodes = childTries.map { it.trieNodeFor(prefix) }
        val childProposals = children.mapIndexed { index, extender -> if (nodes[index] != null) extender.propose(prefix) else null }
        for ((idx, proposals) in childProposals.withIndex()) {
            if (proposals != null) {
                val node = nodes[idx] ?: error("Node should not be null here")
                for (proposal in proposals) {
                    node.insert(proposal)
                }
            }
        }
        // TODO the distinct call can likely be optimized to avoid large intermediate lists
        return childProposals.filterNotNull().flatten().distinct()
    }

    override fun intersect(prefix: Prefix, extensions: List<Extension>): List<Extension> {
        val nodes = childTries.map { it.trieNodeFor(prefix) }
        val result = mutableListOf<Extension>()
        for ((idx, child) in children.withIndex()) {
            if (nodes[idx] == null) continue
            val childExtensions = child.intersect(prefix, extensions)
            for (extension in childExtensions) {
                val node = nodes[idx] ?: error("Node should not be null here")
                node.insert(extension)
            }
            result.addAll(childExtensions)
        }
        return result.distinct()
    }

    // All or clauses have the same variables, hence participate in the same levels
    override fun participatesInLevel(level: Int) = children.first().participatesInLevel(level)
}
