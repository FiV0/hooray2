package org.hooray.algo

import kotlinx.collections.immutable.plus
import kotlinx.collections.immutable.persistentListOf
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
    fun intersect(prefix: Prefix, extensions: List<Extension>) : List<Extension>

    companion object {
        @JvmStatic
        fun createSingleLevel(values: List<Any>, participatesInLevel: Int): PrefixExtender {
            val sortedValues = values.sortedWith(UniversalComparator)
            return object : PrefixExtender {
                override fun count(prefix: Prefix): Int = sortedValues.size

                override fun propose(prefix: Prefix): List<Extension> = sortedValues

                override fun intersect(prefix: Prefix, extensions: List<Extension>): List<Extension>
                // TODO this doesn't make use of the fact that sortedValues is sorted
                        = extensions.filter { ext -> sortedValues.contains(ext) }

                override fun participatesInLevel(level: Int) = level == participatesInLevel
            }
        }

        @JvmStatic
        fun createFromPrefixExtender(participatingLevels: List<Int>, partialPrefix: Prefix): PrefixExtender {
            val levelSet = participatingLevels.toSet()
            return object : PrefixExtender {
                private fun isPrefixMatching(prefix: Prefix): Boolean {
                    var i = 0
                    for (level in participatingLevels) {
                        if (level >= prefix.size) break
                        if (partialPrefix[i] != prefix[level]) {
                            return false
                        }
                        i++
                    }
                    return true
                }

                override fun count(prefix: Prefix): Int =
                    if (isPrefixMatching(prefix)) 1 else 0

                override fun propose(prefix: Prefix): List<Extension> {
                    var i = 0
                    for (level in participatingLevels) {
                        if (level >= prefix.size) break
                        if (partialPrefix[i] != prefix[level]) {
                            return emptyList()
                        }
                        i++
                    }
                    return listOf(partialPrefix[i])
                }

                override fun intersect(prefix: Prefix, extensions: List<Extension>): List<Extension> {
                    var i = 0
                    for (level in participatingLevels) {
                        if (level >= prefix.size) break
                        if (partialPrefix[i] != prefix[level]) {
                            return emptyList()
                        }
                        i++
                    }
                    if (!extensions.contains(partialPrefix[i])) {
                        return emptyList()
                    }
                    return listOf(partialPrefix[i])
                }

                override fun participatesInLevel(level: Int) = levelSet.contains(level)
            }
        }

        @JvmStatic
        fun createFromPrefixesExtender(participatingLevels: List<Int>, partialPrefixes: List<Prefix>): PrefixExtender {
            val levelSet = participatingLevels.toSet()

            return object : PrefixExtender {
                // TODO the matchingPrefixes and nextLevelIndex could get unified for efficiency
                private fun matchingPrefixes(prefix: Prefix): List<Prefix> {
                    return partialPrefixes.filter { partialPrefix ->
                        var i = 0
                        var matches = true
                        for (level in participatingLevels) {
                            if (level >= prefix.size) break
                            if (partialPrefix[i] != prefix[level]) {
                                matches = false
                                break
                            }
                            i++
                        }
                        matches
                    }
                }

                private fun nextLevelIndex(prefix: Prefix): Int {
                    var i = 0
                    for (level in participatingLevels) {
                        if (level >= prefix.size) break
                        i++
                    }
                    return i
                }

                override fun count(prefix: Prefix): Int = matchingPrefixes(prefix).size

                override fun propose(prefix: Prefix): List<Extension> {
                    val idx = nextLevelIndex(prefix)
                    return matchingPrefixes(prefix).map { it[idx] }.distinct()
                }

                override fun intersect(prefix: Prefix, extensions: List<Extension>): List<Extension> {
                    val idx = nextLevelIndex(prefix)
                    val validExtensions = matchingPrefixes(prefix).map { it[idx] }.toSet()
                    return extensions.filter { validExtensions.contains(it) }
                }

                override fun participatesInLevel(level: Int) = levelSet.contains(level)
            }
        }

        fun createPrefixAndExtensionsExtender(fixedPrefix: Prefix, fixedExtensions: List<Extension>): PrefixExtender {
            val extensionSet = fixedExtensions.toHashSet()
            return object : PrefixExtender {
                private fun isPrefixMatching(prefix: Prefix): Boolean =
                    prefix.size <= fixedPrefix.size && fixedPrefix.take(prefix.size) == prefix

                override fun count(prefix: Prefix): Int =
                    if (isPrefixMatching(prefix))
                        if (prefix.size < fixedPrefix.size) 1 else fixedExtensions.size
                    else 0

                override fun propose(prefix: Prefix): List<Extension> =
                    if (isPrefixMatching(prefix))
                        if (prefix.size < fixedPrefix.size) listOf(fixedPrefix[prefix.size]) else fixedExtensions
                    else
                        emptyList()

                override fun intersect(prefix: Prefix, extensions: List<Extension>): List<Extension> =
                    if (isPrefixMatching(prefix))
                        if (prefix.size < fixedPrefix.size)
                            if (extensions.contains(fixedPrefix[prefix.size]))
                                listOf(fixedPrefix[prefix.size])
                            else
                                emptyList()
                        else
                            extensions.filter { ext -> extensionSet.contains(ext) }
                    else
                        emptyList()

                override fun participatesInLevel(level: Int) = level <= fixedPrefix.size
            }
        }
    }
}

fun applyExtensions(prefix: Prefix, extensions: List<Extension>) : List<ResultTuple> {
    val result = mutableListOf<ResultTuple>()
    for(ext in extensions) {
        result.add(prefix + ext)
    }
    return result
}

class GenericSingleJoin(val extenders : List<PrefixExtender>, val prefixes: List<Prefix>) : Join<Prefix> {

    init {
        require(extenders.isNotEmpty()) { "At least one extender is required" }
    }

    override fun join(): List<Prefix> {
        val results = mutableListOf<Prefix>()
        for (prefix in prefixes) {
            // For every prefix find the extender that proposes the least extensions
            val minIndex = extenders.indices.minBy { extenders[it].count(prefix) }
            // Propose extensions from that extender
            var extensions = extenders[minIndex].propose(prefix)
            // Intersect with all other extenders
            for (i in extenders.indices) {
                if (i != minIndex) {
                    extensions = extenders[i].intersect(prefix, extensions)
                }
            }
            results.addAll(applyExtensions(prefix, extensions))
        }
        return results
    }
}

class GenericJoin(val extenders: List<PrefixExtender>, levels: Int) : Join<ResultTuple> {

    // Precompute which extenders participate in which levels
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
        var prefixes: List<Prefix> = listOf(persistentListOf())

        // For every level, perform a single join with the extenders participating in that level
        for (extenderSet in extenderSets) {
            val singleJoin = GenericSingleJoin(extenderSet, prefixes)
            prefixes = singleJoin.join()
        }
        // After all levels are processed, the prefixes are the result tuples
        return prefixes
    }
}