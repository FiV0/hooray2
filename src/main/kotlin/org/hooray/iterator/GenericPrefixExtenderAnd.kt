package org.hooray.iterator

import org.hooray.algo.Extension
import org.hooray.algo.GenericJoin
import org.hooray.algo.Prefix
import org.hooray.algo.PrefixExtender

open class GenericPrefixExtenderAnd(val children: List<GenericPrefixExtender>) : PrefixExtender, LevelParticipation {

    init {
        check(children.isNotEmpty()) { "At least one child extender is required" }
    }

    override fun count(prefix: Prefix) = children.minOf { it.count(prefix) }

    override fun propose(prefix: Prefix) =  TODO()

    override fun extend(prefix: Prefix, extensions: List<Extension>): List<Extension> {
        TODO()
    }

    override fun participatesInLevel(level: Int) = children.any { it.participatesInLevel(level) }
}

