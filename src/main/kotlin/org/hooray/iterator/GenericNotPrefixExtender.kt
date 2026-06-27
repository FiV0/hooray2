package org.hooray.iterator

import org.hooray.algo.Extension
import org.hooray.algo.GenericJoin
import org.hooray.algo.Prefix
import org.hooray.algo.PrefixExtender
import org.hooray.engine.BindingSet
import org.hooray.engine.ExecPattern
import org.hooray.engine.Proposal

// The level NOT participates in is the level after all variables appearing in its children are bound.
// For now this is just the maximum level
class GenericNotPrefixExtender(val children: List<PrefixExtender>, val level: Int): PrefixExtender, ExecPattern {
    override val idx: Int
        get() = TODO("Not yet implemented")

    override val variables: Set<Any>
        get() = TODO("Not yet implemented")

    override fun count(
        input: BindingSet,
        introduces: List<Any>,
        proposals: List<Proposal>,
    ): List<Proposal> = TODO("Not yet implemented")

    override fun propose(
        input: BindingSet,
        introduces: List<Any>,
        targetVariables: List<Any>,
    ): BindingSet = TODO("Not yet implemented")

    override fun validate(input: BindingSet): BindingSet = TODO("Not yet implemented")

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

    override fun participatesInLevel(level: Int) = this.level == level
}
