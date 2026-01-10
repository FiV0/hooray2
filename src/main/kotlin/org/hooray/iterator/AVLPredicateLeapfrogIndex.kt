package org.hooray.iterator

import org.hooray.algo.FilterLeapfrogIndex
import org.hooray.algo.ResultTuple

class AVLPredicateLeapfrogIndex(
    val levels: List<Int>,
    val predicate: Any
) : FilterLeapfrogIndex {

    init {
        require(levels.size in 1..2) { "Hooray only supports unary and binary predicates for now." }
    }

    override fun participatesInLevel(level: Int) = level == levels.last()

    @Suppress("UNCHECKED_CAST")
    override fun accept(tuple: ResultTuple): Boolean {
        return when (levels.size) {
            1 -> {
                val pred = predicate as Predicate1<Any>
                pred(tuple[levels[0]])
            }
            2 -> {
                val pred = predicate as Predicate2<Any, Any>
                pred(tuple[levels[0]], tuple[levels[1]])
            }
            else -> throw IllegalStateException("Unreachable")
        }
    }
}
