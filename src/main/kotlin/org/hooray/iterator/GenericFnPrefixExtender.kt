package org.hooray.iterator

import org.hooray.algo.Extension
import org.hooray.algo.Prefix
import org.hooray.algo.PrefixExtender
import org.hooray.engine.BindingSet
import org.hooray.engine.ExecPattern
import org.hooray.engine.Proposal

typealias Fn1<A, R> = (A) -> R
typealias Fn2<A, B, R> = (A, B) -> R

class GenericFnPrefixExtender(val levels: List<Int>, val outputLevel: Int, val fn: Any) : PrefixExtender, ExecPattern {
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

    init {
        require(levels.size in 1..2) { "Hooray only supports unary and binary functions for now." }
    }

    @Suppress("UNCHECKED_CAST")
    private fun applyFn(prefix: Prefix): Extension {
        return when (levels.size) {
            1 -> {
                val f = fn as Fn1<Any, Any>
                f(prefix[levels[0]])
            }
            2 -> {
                val f = fn as Fn2<Any, Any, Any>
                f(prefix[levels[0]], prefix[levels[1]])
            }
            else -> throw IllegalStateException("Unreachable")
        }
    }

    override fun count(prefix: Prefix): Int = 1

    override fun propose(prefix: Prefix): List<Extension> = listOf(applyFn(prefix))

    override fun intersect(prefix: Prefix, extensions: List<Extension>): List<Extension> {
        val result = applyFn(prefix)
        return if (extensions.contains(result)) listOf(result) else emptyList()
    }

    override fun participatesInLevel(level: Int) = level == outputLevel
}
