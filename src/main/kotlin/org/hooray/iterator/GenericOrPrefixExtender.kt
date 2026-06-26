package org.hooray.iterator

import org.hooray.algo.Extension
import org.hooray.algo.GenericJoin
import org.hooray.algo.Prefix
import org.hooray.algo.PrefixExtender

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

    init {
        check(children.isNotEmpty()) { "At least one child extender is required" }
    }

    override fun count(prefix: Prefix): Int =
        saturatingSum(children.map { it.count(prefix) })

    private fun levelsThrough(prefix: Prefix): List<Int> = (0..prefix.size).toList()

    private fun materializeRelation(prefix: Prefix, contextExtender: PrefixExtender): GenericRelationPrefixExtender {
        val levels = levelsThrough(prefix)
        val relation = children
            .flatMap { child -> GenericJoin(listOf(child, contextExtender), prefix.size + 1).join() }
            .distinct()
        return GenericRelationPrefixExtender(levels, relation)
    }

    private fun materializeForPropose(prefix: Prefix): GenericRelationPrefixExtender {
        val prefixLevels = (0 until prefix.size).toList()
        val prefixExtender = PrefixExtender.createFromPrefixExtender(prefixLevels, prefix)
        return materializeRelation(prefix, prefixExtender)
    }

    private fun materializeForIntersect(prefix: Prefix, extensions: List<Extension>): GenericRelationPrefixExtender {
        val prefixAndExtensionsExtender = PrefixExtender.createPrefixAndExtensionsExtender(prefix, extensions)
        return materializeRelation(prefix, prefixAndExtensionsExtender)
    }

    override fun propose(prefix: Prefix): List<Extension> =
        materializeForPropose(prefix).propose(prefix)

    override fun intersect(prefix: Prefix, extensions: List<Extension>): List<Extension> {
        if (extensions.isEmpty()) return emptyList()
        return materializeForIntersect(prefix, extensions).intersect(prefix, extensions)
    }

    // All or clauses have the same variables, hence participate in the same levels
    override fun participatesInLevel(level: Int) = children.any { it.participatesInLevel(level) }
}
