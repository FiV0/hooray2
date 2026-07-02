package org.hooray.iterator

import org.hooray.algo.Extension
import org.hooray.algo.GenericJoin
import org.hooray.algo.Prefix
import org.hooray.algo.PrefixExtender

// The level NOT participates in is the level at which all variables appearing in its children are bound,
// i.e. the last of its variable levels.
class GenericNotPrefixExtender(val children: List<PrefixExtender>, val levels: List<Long>): PrefixExtender {

    init {
        require(levels.isNotEmpty()) { "At least one level is required" }
        require(levels == levels.sorted()) { "Levels must be sorted ascending" }
    }

    override fun count(prefix: Prefix): Int = Int.MAX_VALUE

    // If propose is called on NOT it means that the variable was not bound outside of NOT
    override fun propose(prefix: Prefix): List<Extension> {
       throw IllegalStateException("Propose should not be called on NOT prefix extender")
    }

    override fun intersect(prefix: Prefix, extensions: List<Extension>): List<Extension> {
        // TODO make this more efficient by restricting to prefixes that actually appear in children
        val prefixAndExtensionsExtender = PrefixExtender.createPrefixAndExtensionsExtender(prefix, extensions)
        val extensionsToRemove: Set<Extension> = GenericJoin(children + prefixAndExtensionsExtender, prefix.size + 1).join().map { resultTuple -> resultTuple.last() }.toHashSet()

        return extensions.filterNot { ext -> extensionsToRemove.contains(ext) }

//        for (extension in extensions) {
//            val resultTuple = prefix + extension
//            val tuplePrefixExtender = PrefixExtender.createTupleExtender(resultTuple)
//            val join = GenericJoin(children + tuplePrefixExtender, prefix.size + 1)
//            join.join().takeIf { it.isEmpty() }?.let {
//                filteredExtensions.add(extension)
//            }
//        }
//        return filteredExtensions
    }

    override fun variableLevels(): List<Long> = levels

    override fun participatesInLevel(level: Int) = level.toLong() == levels.last()
}