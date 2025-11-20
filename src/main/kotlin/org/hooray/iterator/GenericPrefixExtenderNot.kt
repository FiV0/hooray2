package org.hooray.iterator

import org.hooray.algo.Extension
import org.hooray.algo.GenericJoin
import org.hooray.algo.Prefix
import org.hooray.algo.PrefixExtender

// The level NOT participates is the level after all variables appearing in its children are bound.
// For now this is just the maximum level
class GenericPrefixExtenderNot(val children: List<PrefixExtender>, val level: Int): PrefixExtender {
    override fun count(prefix: Prefix): Int = Int.MAX_VALUE

    override fun propose(prefix: Prefix): List<Extension> = throw UnsupportedOperationException("NOT clause can not propose!")

    override fun extend(prefix: Prefix, extensions: List<Extension>): List<Extension> {
        val filteredExtensions = mutableListOf<Extension>()
        for (extension in extensions) {
            val resultTuple = prefix + extension
            val tuplePrefixExtender = PrefixExtender.createTupleExtender(prefix + extension)
            val join = GenericJoin(children + tuplePrefixExtender, resultTuple.size)
            join.join().takeIf { it.isEmpty() }?.let {
                filteredExtensions.add(extension)
            }
        }
        return filteredExtensions
    }

    override fun participatesInLevel(level: Int) = this.level == level
}