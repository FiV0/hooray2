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
    private val variableLevels: List<Long>
    private val levelSet: Set<Long>

    init {
        check(children.isNotEmpty()) { "At least one child extender is required" }
        variableLevels = children.flatMap { it.variableLevels() }.distinct().sorted()
        // Participation levels are a subset of the variable levels
        levelSet = variableLevels.filter { level -> children.first().participatesInLevel(level) }.toSet()
    }

    // The child tries are keyed by the values at all variable levels, not just the participation
    // levels: a child's decision at a participation level may depend on variables bound at levels
    // where the OR was never consulted (e.g. a predicate argument bound by a triple pattern).
    // At those levels every value passes, so missing nodes are created on the fly; at
    // participation levels a missing node means the branch rejected that value.
    protected fun nodesForPrefix(prefix: Prefix): List<Trie.Node<Any>?> =
        childTries.map { trie ->
            var node: Trie.Node<Any>? = trie.trieNodeFor(emptyList())
            for (level in variableLevels) {
                if (level >= prefix.size || node == null) break
                node = if (levelSet.contains(level)) node.children[prefix[level.toInt()]] else node.insert(prefix[level.toInt()])
            }
            node
        }

    override fun count(prefix: Prefix): Int {
        val nodes = nodesForPrefix(prefix)
        return saturatingSum(children.mapIndexed { index, extender -> if (nodes[index] != null) extender.count(prefix) else 0 })
    }

    override fun propose(prefix: Prefix): List<Extension> {
        val nodes = nodesForPrefix(prefix)
        val childProposals = children.mapIndexed { index, extender -> if (nodes[index] != null) extender.propose(prefix) else emptyList() }
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
        val nodes = nodesForPrefix(prefix)
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

    override fun variableLevels(): List<Long> = variableLevels

    override fun participatesInLevel(level: Long): Boolean = levelSet.contains(level)
}
