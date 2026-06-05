package org.hooray.iterator

import org.hooray.algo.Extension
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

    // A branch may only contribute extensions for prefixes that are consistent with that branch's
    // own bindings on every already-joined level. The flat prefix does not record which branch
    // produced an earlier binding, so without this check a value bound by one branch could be
    // extended by another branch's constraints (losing branch identity across levels).
    //
    // We re-validate each previously-joined level by intersecting the single bound value against the
    // child, always using a prefix sized exactly to that level. This keeps every leaf extender
    // queried at its own level, so terminal set-backed extenders are never indexed with a deeper
    // prefix (which would throw).
    protected fun branchAlive(child: PrefixExtender, prefix: Prefix): Boolean {
        for (level in prefix.indices) {
            if (child.participatesInLevel(level)) {
                val subPrefix = prefix.subList(0, level)
                if (child.intersect(subPrefix, listOf(prefix[level])).isEmpty()) {
                    return false
                }
            }
        }
        return true
    }

    protected fun aliveChildren(prefix: Prefix): List<PrefixExtender> =
        children.filter { branchAlive(it, prefix) }

    override fun count(prefix: Prefix): Int =
        saturatingSum(children.map { it.count(prefix) })

    // TODO the distinct call can likely be optimized to avoid large intermediate lists
    override fun propose(prefix: Prefix) = aliveChildren(prefix).flatMap { it.propose(prefix) }.distinct()

    override fun intersect(prefix: Prefix, extensions: List<Extension>): List<Extension> {
        val result = mutableListOf<Extension>()
        for (child in aliveChildren(prefix)) {
            val childExtensions = child.intersect(prefix, extensions)
            result.addAll(childExtensions)
        }
        return result.distinct()
    }

    // All or clauses have the same variables, hence participate in the same levels
    override fun participatesInLevel(level: Int) = children.first().participatesInLevel(level)
}
