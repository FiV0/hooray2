package org.hooray.algo

import org.hooray.UniversalComparator
import org.hooray.iterator.LevelParticipation

typealias Prefix = ResultTuple
typealias Extension = Any

// generic-join based on
// http://www.frankmcsherry.org/dataflow/relational/join/2015/04/11/genericjoin.html
// https://arxiv.org/abs/1310.3314

interface PrefixExtender : LevelParticipation {
    fun count(prefix: Prefix): Int
    fun propose(prefix: Prefix) : List<Extension>
    fun extend(prefix: Prefix, extensions: List<Extension>) : List<Extension>

    companion object {
        @JvmStatic
        fun createSingleLevel(values: List<Int>, participatesInLevel: Int): PrefixExtender {
            val sortedValues = values.sortedWith(UniversalComparator)
            return object : PrefixExtender {
                override fun count(prefix: Prefix): Int = sortedValues.size

                override fun propose(prefix: Prefix): List<Extension> = sortedValues

                override fun extend(prefix: Prefix, extensions: List<Extension>): List<Extension>
                // TODO this doesn't make use of the fact that sortedValues is sorted
                        = extensions.filter { ext -> sortedValues.contains(ext) }

                override fun participatesInLevel(level: Int) = level == participatesInLevel
            }
        }

        fun createTupleExtender(tuple: ResultTuple): PrefixExtender {
            return object : PrefixExtender {
                private fun isPrefixMatching(prefix: Prefix): Boolean =
                    prefix.size <= tuple.size && tuple.take(prefix.size) == prefix

                override fun count(prefix: Prefix): Int = if (isPrefixMatching(prefix)) 1 else 0

                override fun propose(prefix: Prefix): List<Extension> =
                    if (isPrefixMatching(prefix)) listOf(tuple[prefix.size]) else emptyList()

                override fun extend(prefix: Prefix, extensions: List<Extension>): List<Extension> =
                    if (isPrefixMatching(prefix) && extensions.contains(tuple[prefix.size])) listOf(tuple[prefix.size]) else emptyList()

                override fun participatesInLevel(level: Int) = level < tuple.size
            }
        }
    }
}

fun applyExtensions(prefix: Prefix, extensions: List<Extension>) : List<ResultTuple> {
    val result = mutableListOf<ResultTuple>()
    for(ext in extensions) {
        val newTuple = prefix.toMutableList()
        newTuple.add(ext)
        result.add(newTuple)
    }
    return result
}

class GenericSingleJoin(val extenders : List<PrefixExtender>, val prefixes: List<Prefix>) : Join<ResultTuple> {

    init {
        require(extenders.isNotEmpty()) { "At least one extender is required" }
    }

    override fun join(): List<ResultTuple> {
        val results = mutableListOf<ResultTuple>()
        for (prefix in prefixes) {
            val minIndex = extenders.indices.minBy { extenders[it].count(prefix) }
            var extensions = extenders[minIndex].propose(prefix)
            for (i in extenders.indices) {
                if (i != minIndex) {
                    extensions = extenders[i].extend(prefix, extensions)
                }
            }
            results.addAll(applyExtensions(prefix, extensions))
        }
        return results
    }
}

class GenericJoin(val extenders: List<PrefixExtender>, levels: Int) : Join<ResultTuple> {

    private val extenderSets : List<List<PrefixExtender>> = List(levels) { level ->
        val participants = mutableListOf<PrefixExtender>()
        for (extender in extenders) {
            if (extender.participatesInLevel(level)) {
                participants.add(extender)
            }
        }
        participants
    }

    override fun join(): List<ResultTuple> {
        var prefixes: List<Prefix> = listOf(emptyList())

        for (extenderSet in extenderSets) {
            val singleJoin = GenericSingleJoin(extenderSet, prefixes)
            val newTuples = singleJoin.join()
            prefixes = newTuples
        }
        return prefixes
    }
}

interface ResultTupleFilter : LevelParticipation {
    fun filter(results: List<ResultTuple>): Boolean
}