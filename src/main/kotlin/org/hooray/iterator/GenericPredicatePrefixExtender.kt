package org.hooray.iterator

import org.hooray.algo.Extension
import org.hooray.algo.Prefix
import org.hooray.algo.PrefixExtender

typealias Predicate1<A> = (A) -> Boolean
typealias Predicate2<A, B> = (A, B) -> Boolean

class GenericPredicatePrefixExtender(val levels: List<Int>, val predicate: Any) : PrefixExtender {

    init {
        require(levels.size in 1..2) { "Hooray only supports unary and binary predicates for now." }
    }

    override fun count(prefix: Prefix): Int = Int.MAX_VALUE

    override fun propose(prefix: Prefix): List<Extension> {
        throw IllegalStateException("Propose should not be called on predicate prefix extender")
    }

    @Suppress("UNCHECKED_CAST")
    override fun intersect(prefix: Prefix, extensions: List<Extension>): List<Extension> {
        return when (levels.size) {
            1 -> {
                val pred = predicate as Predicate1<Any>
                extensions.filter { ext -> pred(ext) }
            }
            2 -> {
                val pred = predicate as Predicate2<Any, Any>
                val arg1 = prefix[levels[0]]
                extensions.filter { ext -> pred(arg1, ext) }
            }
            else -> throw IllegalStateException("Unreachable")
        }
    }

    override fun participatesInLevel(level: Int) = level == levels.last()
}
