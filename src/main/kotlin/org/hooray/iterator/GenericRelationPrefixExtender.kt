package org.hooray.iterator

import org.hooray.algo.Extension
import org.hooray.algo.Prefix
import org.hooray.algo.PrefixExtender
import org.hooray.algo.ResultTuple
import org.hooray.util.Trie

class GenericRelationPrefixExtender(
    private val levels: List<Int>,
    relation: List<ResultTuple>
) : PrefixExtender {

    private val levelSet: Set<Int>
    private val trie = Trie<Any>()

    init {
        require(levels.isNotEmpty()) { "At least one level is required" }
        require(levels == levels.sorted()) { "Levels must be sorted ascending" }
        require(levels.toSet().size == levels.size) { "Levels must be unique" }
        require(relation.all { it.size == levels.size }) {
            "Every relation tuple must have the same size as levels"
        }

        levelSet = levels.toSet()

        for (tuple in relation) {
            trie.insert(tuple)
        }
    }

    private fun trieNodeFor(prefix: Prefix): Trie.Node<Any>? =
        trie.trieNodeFor(
            levels.takeWhile { level -> level < prefix.size }.map { level -> prefix[level] }
        )

    override fun count(prefix: Prefix): Int {
        val node = trieNodeFor(prefix) ?: return 0
        // There can only be a single match
        if (prefix.size > levels.last()) return 1
        return node.children.size
    }

    override fun propose(prefix: Prefix): List<Extension> =
        trieNodeFor(prefix)?.children?.keys?.toList() ?: emptyList()

    override fun intersect(prefix: Prefix, extensions: List<Extension>): List<Extension> {
        val validExtensions = trieNodeFor(prefix)?.children ?: return emptyList()
        return extensions.filter { validExtensions.containsKey(it) }
    }

    override fun participatesInLevel(level: Int): Boolean =
        levelSet.contains(level)
}
