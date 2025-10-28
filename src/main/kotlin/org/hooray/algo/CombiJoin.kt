package org.hooray.algo

import org.hooray.UniversalComparator

// A combination of Leapfrog on every level but overall strategy of GenericJoin

interface CombiJoinExtender {
    fun getIterators(prefix: Prefix) : LeapfrogIterator
}

class CombiSingleJoin(private val iterators: List<LeapfrogIterator>) {

    init {
        require(iterators.isNotEmpty()) { "Must have at least one iterator" }
    }

    fun collect():  List<Extension> {
        val results = mutableListOf<Extension>()
        var currentIteratorIndex = 0

        if (iterators[currentIteratorIndex].atEnd()) {
            return results
        }

        var maxKey = iterators[currentIteratorIndex].key()
        var startIndex = 0
        currentIteratorIndex = (currentIteratorIndex + 1) % iterators.size

        while (true) {
            val currentIterator = iterators[currentIteratorIndex]
            if (currentIteratorIndex == startIndex) {
                results.add(maxKey)
                currentIterator.next()
                if( currentIterator.atEnd()) {
                    return results
                }
                maxKey = currentIterator.key()
                startIndex = currentIteratorIndex
            } else {

                // Seek to at least maxKey
                currentIterator.seek(maxKey)

                if (currentIterator.atEnd()) {
                    return results
                }

                val currentKey = currentIterator.key()

                assert(UniversalComparator.compare(currentKey, maxKey) >= 0) { "Current key should be >= maxKey after seek" }

                if (UniversalComparator.compare(currentKey, maxKey) > 0) {
                    maxKey = currentKey
                    startIndex = currentIteratorIndex
                }
            }

            currentIteratorIndex = (currentIteratorIndex + 1) % iterators.size
        }
    }
}

class CombiJoin (val extenders: List<List<CombiJoinExtender>>): Join<ResultTuple> {

    override fun join(): List<ResultTuple> {
        var prefixes: List<Prefix> = listOf(emptyList())
        for (extenderSet in extenders) {
            val newPrefixes = mutableListOf<Prefix>()
            for (prefix in prefixes) {
                val singleJoin = CombiSingleJoin(extenderSet.map { it.getIterators(prefix) } )
                val newExtensions = singleJoin.collect()
                val newTuples = applyExtensions(prefix, newExtensions)
                newPrefixes.addAll(newTuples)
            }
            prefixes = newPrefixes
        }
        return prefixes
    }
}