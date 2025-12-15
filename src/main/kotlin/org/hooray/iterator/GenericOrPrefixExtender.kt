package org.hooray.iterator

import org.hooray.algo.Extension
import org.hooray.algo.Prefix
import org.hooray.algo.PrefixExtender

open class GenericOrPrefixExtender(val children: List<PrefixExtender>) : PrefixExtender {

    init {
        check(children.isNotEmpty()) { "At least one child extender is required" }
    }

    override fun count(prefix: Prefix) = children.sumOf { it.count(prefix) }

    // TODO the distinct call can likely be optimized to avoid large intermediate lists
    override fun propose(prefix: Prefix) = children.flatMap { it.propose(prefix) }.distinct()

    override fun intersect(prefix: Prefix, extensions: List<Extension>): List<Extension> {
        val result = mutableListOf<Extension>()
        for (child in children) {
            val childExtensions = child.intersect(prefix, extensions)
            result.addAll(childExtensions)
        }
        return result.distinct()
    }

    // All or clauses have the same variables, hence participate in the same levels
    override fun participatesInLevel(level: Int) = children.first().participatesInLevel(level)
}