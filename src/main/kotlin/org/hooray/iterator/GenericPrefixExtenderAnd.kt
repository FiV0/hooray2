package org.hooray.iterator

import org.hooray.algo.Extension
import org.hooray.algo.Prefix
import org.hooray.algo.PrefixExtender

open class GenericPrefixExtenderAnd(val children: List<PrefixExtender>) : PrefixExtender {

    init {
        check(children.isNotEmpty()) { "At least one child extender is required" }
    }

    override fun count(prefix: Prefix) = children.minOf { it.count(prefix) }

    override fun propose(prefix: Prefix) : List<Extension> {
        val nextLevel =  prefix.size
        val participants = children.filter { it.participatesInLevel(nextLevel) }
        val minChild = participants.minBy { it.count(prefix) }
        var extensions = minChild.propose(prefix)
        for (child in participants) {
            if (child != minChild) {
                extensions = child.extend(prefix, extensions)
            }
        }
        return extensions
    }

    override fun extend(prefix: Prefix, extensions: List<Extension>): List<Extension> {
        var currentExtensions = extensions
        val nextLevel = prefix.size
        val participants = children.filter { it.participatesInLevel(nextLevel) }.sortedBy { it.count(prefix) }
        for (child in participants) {
            currentExtensions = child.extend(prefix, currentExtensions)
        }
        return currentExtensions
    }

    override fun participatesInLevel(level: Int) = children.any { it.participatesInLevel(level) }
}