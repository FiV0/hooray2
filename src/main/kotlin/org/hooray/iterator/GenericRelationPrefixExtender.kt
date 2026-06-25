package org.hooray.iterator

import org.hooray.algo.Extension
import org.hooray.algo.Prefix
import org.hooray.algo.PrefixExtender
import org.hooray.algo.ResultTuple

class GenericRelationPrefixExtender(
    relation: List<ResultTuple>,
    private val levels: List<Int>
) : PrefixExtender {

    private class TrieNode {
        val children: MutableMap<Any, TrieNode> = linkedMapOf()
    }

    private val levelSet: Set<Int>
    private val root = TrieNode()

    init {
        require(levels.isNotEmpty()) { "At least one level is required" }
        require(levels.toSet().size == levels.size) { "Levels must be unique" }
        require(relation.all { it.size == levels.size }) {
            "Every relation tuple must have the same size as levels"
        }

        levelSet = levels.toSet()

        for (tuple in relation) {
            var node = root
            for (value in tuple) {
                node = node.children.getOrPut(value) { TrieNode() }
            }
        }
    }

    private fun trieNodeFor(prefix: Prefix): TrieNode? {
        var node = root
        for (level in levels) {
            if (level >= prefix.size) {
                break
            }
            val value = prefix[level]
            node = node.children[value] ?: return null
        }
        return node
    }

    override fun count(prefix: Prefix): Int =
        trieNodeFor(prefix)?.children?.size ?: 0

    override fun propose(prefix: Prefix): List<Extension> =
        trieNodeFor(prefix)?.children?.keys?.toList() ?: emptyList()

    override fun intersect(prefix: Prefix, extensions: List<Extension>): List<Extension> {
        val validExtensions = trieNodeFor(prefix)?.children?.keys ?: return emptyList()
        return extensions.filter { validExtensions.contains(it) }
    }

    override fun participatesInLevel(level: Int): Boolean =
        levelSet.contains(level)
}
