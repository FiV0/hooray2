package org.hooray.iterator

import org.hooray.algo.Extension
import org.hooray.algo.Prefix
import org.hooray.algo.PrefixExtender

class GenericFnPrefixExtender(val levels: List<Int>, val outputLevel: Int, val fn: Any) : PrefixExtender {

    private val participationLevel: Int
    private val proposesOutput: Boolean

    init {
        require(levels.size in 1..2) { "Hooray only supports unary and binary functions for now." }
        require(levels == levels.sorted()) { "Function argument levels must be sorted ascending." }
        require(levels.toSet().size == levels.size) { "Function argument levels must be unique." }
        require(outputLevel !in levels) { "Function output level must differ from its argument levels." }

        participationLevel = maxOf(levels.last(), outputLevel)
        proposesOutput = outputLevel == participationLevel
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

    override fun count(prefix: Prefix): Int = if (proposesOutput) 1 else Int.MAX_VALUE

    override fun propose(prefix: Prefix): List<Extension> {
        check(proposesOutput) { "A function cannot propose when its output is already bound" }
        return listOf(applyFn(prefix))
    }

    override fun intersect(prefix: Prefix, extensions: List<Extension>): List<Extension> {
        return if (proposesOutput) {
            val result = applyFn(prefix)
            if (extensions.contains(result)) listOf(result) else emptyList()
        } else {
            extensions.filter { extension ->
                val completedPrefix = prefix + extension
                completedPrefix[outputLevel] == applyFn(completedPrefix)
            }
        }
    }

    override fun participatesInLevel(level: Int) = level == participationLevel
}
