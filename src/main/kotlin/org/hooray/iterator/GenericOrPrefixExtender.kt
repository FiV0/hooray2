package org.hooray.iterator

import org.hooray.algo.Extension
import org.hooray.algo.Prefix
import org.hooray.algo.PrefixExtender
import org.hooray.engine.BindingSet
import org.hooray.engine.ExecPattern
import org.hooray.engine.Proposal
import org.hooray.engine.Variable

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

open class GenericOrPrefixExtender(val children: List<PrefixExtender>) : PrefixExtender, ExecPattern {
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

    override fun count(prefix: Prefix): Int =
        saturatingSum(children.map { it.count(prefix) })

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
