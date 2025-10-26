package org.hooray.algo

typealias Prefix = ResultTuple
typealias Extension = Any

// generic-join based on
// http://www.frankmcsherry.org/dataflow/relational/join/2015/04/11/genericjoin.html
// https://arxiv.org/abs/1310.3314

interface PrefixExtender {
    fun count(prefix: Prefix): Int
    fun propose(prefix: Prefix) : List<Extension>
    fun extend(prefix: Prefix, extensions: List<Extension>) : List<Extension>
}

class GenericSingleJoin(val extenders : List<PrefixExtender>, val prefixes: List<Prefix>) : Join<ResultTuple> {

    init {
        require(extenders.isNotEmpty()) { "At least one extender is required" }
    }

    private fun applyExtensions(prefix: Prefix, extensions: List<Extension>) : List<ResultTuple> {
        val result = mutableListOf<ResultTuple>()
        for(ext in extensions) {
            val newTuple = prefix.toMutableList()
            newTuple.add(ext)
            result.add(newTuple)
        }
        return result
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

class GenericJoin(val extenders: List<List<PrefixExtender>>) : Join<ResultTuple> {
    override fun join(): List<ResultTuple> {
        var prefixes: List<Prefix> = listOf(emptyList())
        for (extenderSet in extenders) {
            val singleJoin = GenericSingleJoin(extenderSet, prefixes)
            val newTuples = singleJoin.join()
            prefixes = newTuples
        }
        return prefixes
    }
}