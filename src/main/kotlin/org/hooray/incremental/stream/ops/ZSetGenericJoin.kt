package org.hooray.incremental.stream.ops

import kotlinx.collections.immutable.persistentListOf
import org.hooray.algo.ResultTuple
import org.hooray.incremental.IntegerWeight
import org.hooray.incremental.ResultZSet
import org.hooray.incremental.ZSet
import org.hooray.incremental.ZSetPrefixExtender

fun zSetGenericJoin(extenders: List<ZSetPrefixExtender>, levels: Int): ResultZSet {
    var prefixes = ZSet.singleton<ResultTuple>(persistentListOf())

    for (level in 0 until levels) {
        val participating = extenders.filter { it.participatesInLevel(level) }
        require(participating.isNotEmpty()) {
            "No extenders participate in level $level, cannot perform join"
        }

        var nextPrefixes = ZSet.empty<ResultTuple>()
        for ((prefix, prefixWeight) in prefixes.entries()) {
            val minIndex = participating.indices.minBy { participating[it].count(prefix) }
            var extensions = participating[minIndex].propose(prefix)

            for (i in participating.indices) {
                if (i != minIndex) {
                    extensions = participating[i].intersect(prefix, extensions)
                }
            }

            for ((extension, extensionWeight) in extensions.entries()) {
                val nextPrefix = prefix + extension
                val weight = prefixWeight.multiply(extensionWeight)
                nextPrefixes = nextPrefixes.add(ZSet.singleton(nextPrefix, weight))
            }
        }

        prefixes = nextPrefixes
        if (prefixes.isEmpty()) break
    }

    return prefixes
}

fun permuteToCanonical(termResult: ResultZSet, variableOrder: List<Int>, levels: Int): ResultZSet {
    if (variableOrder == (0 until levels).toList()) return termResult

    val result = mutableMapOf<ResultTuple, IntegerWeight>()
    for ((tuple, weight) in termResult.entries()) {
        val canonical = MutableList<Any?>(levels) { null }
        for ((variableOrderIndex, canonicalIndex) in variableOrder.withIndex()) {
            canonical[canonicalIndex] = tuple[variableOrderIndex]
        }
        @Suppress("UNCHECKED_CAST")
        val canonicalTuple = canonical as ResultTuple

        result.merge(canonicalTuple, weight) { left, right ->
            val sum = left.add(right)
            if (sum.isZero()) null else sum
        }
    }
    return ZSet.fromMap(result)
}
