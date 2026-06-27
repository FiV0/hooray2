package org.hooray.iterator

import org.hooray.algo.Extension
import org.hooray.algo.Prefix
import org.hooray.algo.PrefixExtender
import org.hooray.engine.BindingSet
import org.hooray.engine.ExecPattern
import org.hooray.engine.Proposal
import org.hooray.engine.Variable

open class GenericAndPrefixExtender(val children: List<PrefixExtender>) : PrefixExtender, ExecPattern {
    override val idx: Int
        get() = TODO("Not yet implemented")

    override val variables: Set<Variable>
        get() = TODO("Not yet implemented")

    override fun count(
        input: BindingSet,
        introduces: List<Variable>,
        proposals: List<Proposal>,
    ): List<Proposal> = TODO("Not yet implemented")

    override fun propose(
        input: BindingSet,
        introduces: List<Variable>,
        targetVariables: List<Variable>,
    ): BindingSet = TODO("Not yet implemented")

    override fun validate(
        input: BindingSet,
        introduces: List<Variable>,
        targetVariables: List<Variable>,
    ): BindingSet = TODO("Not yet implemented")

    init {
        check(children.isNotEmpty()) { "At least one child extender is required" }
    }

    private fun participants(prefix: Prefix): List<PrefixExtender> =
        children.filter { it.participatesInLevel(prefix.size) }

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

    override fun participatesInLevel(level: Int) = children.any { it.participatesInLevel(level) }
}
