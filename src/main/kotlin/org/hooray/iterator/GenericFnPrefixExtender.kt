package org.hooray.iterator

import org.hooray.algo.Extension
import org.hooray.algo.Prefix
import org.hooray.algo.PrefixExtender

typealias Fn1<A, R> = (A) -> R
typealias Fn2<A, B, R> = (A, B) -> R

class GenericFnPrefixExtender(val levels: List<Int>, val outputLevel: Int, val fn: Any) : PrefixExtender {

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
