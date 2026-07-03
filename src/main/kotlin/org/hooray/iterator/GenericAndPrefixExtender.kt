package org.hooray.iterator

import org.hooray.algo.Extension
import org.hooray.algo.Prefix
import org.hooray.algo.PrefixExtender

open class GenericAndPrefixExtender(val children: List<PrefixExtender>) : PrefixExtender {

    val variableLevels: List<Long>

    init {
        check(children.isNotEmpty()) { "At least one child extender is required" }
        variableLevels = children.flatMap { it.variableLevels() }.distinct().sorted()
    }

    private fun participants(prefix: Prefix): List<PrefixExtender> =
        children.filter { it.participatesInLevel(prefix.size.toLong()) }

    override fun count(prefix: Prefix) = participants(prefix).minOf { it.count(prefix) }

    override fun propose(prefix: Prefix) : List<Extension> {
        val participants = participants(prefix)
        val minChild = participants.minBy { it.count(prefix) }
        var extensions = minChild.propose(prefix)
        for (child in participants) {
            if (child != minChild) {
                extensions = child.intersect(prefix, extensions)
            }
        }
        return extensions
    }

    override fun intersect(prefix: Prefix, extensions: List<Extension>): List<Extension> {
        var currentExtensions = extensions
        val participants = participants(prefix).sortedBy { it.count(prefix) }
        for (child in participants) {
            currentExtensions = child.intersect(prefix, currentExtensions)
        }
        return currentExtensions
    }

    override fun variableLevels(): List<Long> = variableLevels

    override fun participatesInLevel(level: Long) = children.any { it.participatesInLevel(level) }
}