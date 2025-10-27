package org.hooray.algo

import org.hooray.UniversalComparator
import java.util.Stack

// leapfrog-trie-join based on
// https://arxiv.org/abs/1210.0481

interface LeapfrogIterator {
    fun seek(key: Any)
    fun next(): Any
    fun key(): Any
    fun atEnd(): Boolean
}

interface LayeredIndex {
    fun openLevel()
    fun closeLevel()
    fun level() : Int
    fun maxLevel(): Int
}

interface LeapfrogIndex : LeapfrogIterator, LayeredIndex {
    fun participatesInLevel(level: Int): Boolean
}

class LeapfrogSingleJoin(private val iterators: List<LeapfrogIterator>) {
    private var currentIteratorIndex = 0

    init {
        require(iterators.isNotEmpty()) { "Must have at least one iterator" }
    }

    fun next() {
        iterators[currentIteratorIndex].next()
    }

    fun search(candidateTuple: MutableList<Any>) : Boolean {
        if (iterators[currentIteratorIndex].atEnd()) {
            return false
        }

        var maxKey = iterators[currentIteratorIndex].key()
        var startIndex = currentIteratorIndex

        while (true) {
            // Move to next iterator in round-robin fashion
            currentIteratorIndex = (currentIteratorIndex + 1) % iterators.size
            val currentIterator = iterators[currentIteratorIndex]

            // Seek to at least maxKey
            currentIterator.seek(maxKey)

            if (currentIterator.atEnd()) {
                return false
            }

            val currentKey = currentIterator.key()

            assert(UniversalComparator.compare(currentKey, maxKey) >= 0) { "Current key should be >= maxKey after seek" }

            if (UniversalComparator.compare(currentKey , maxKey) > 0) {
                maxKey = currentKey
                startIndex = currentIteratorIndex
            } else if (currentIteratorIndex == startIndex) {
                candidateTuple.add(currentKey)
                return true
            }
        }
    }
}

class LeapfrogJoin(val indexes: List<LeapfrogIndex>, val levels: Int) : Join<ResultTuple>  {
    val participants : List<List<LeapfrogIndex>>
    val singleJoinStack : Stack<LeapfrogSingleJoin> = Stack()

    init {
        require(indexes.isNotEmpty()) { "At least one index is required" }
        require(indexes.all { it.maxLevel() > 0 }) { "All indices must have at least one level!" }
        participants = List(levels) { i ->
            val participants = mutableListOf<LeapfrogIndex>()
            for(index in indexes) {
                if(index.participatesInLevel(i)) {
                    participants.add(index)
                }
            }
            participants
        }
    }

    override fun join(): List<ResultTuple> {
        val results = mutableListOf<ResultTuple>()

        val candidateTuple = mutableListOf<Any>()
        singleJoinStack.push(LeapfrogSingleJoin(participants[0]))

        while (singleJoinStack.isNotEmpty()) {
            val currentJoin = singleJoinStack.peek()
            val level = singleJoinStack.size - 1
            assert(level == candidateTuple.size) { "Level should always match candidate size. Level $level, candidate size ${candidateTuple.size}" }
            if (currentJoin.search(candidateTuple)) {
                if (level == levels - 1) {
                    results.add(candidateTuple.toList())
                    candidateTuple.removeLast()
                    currentJoin.next()
                } else {
                    participants[level + 1].map { it.openLevel() }
                    singleJoinStack.push(LeapfrogSingleJoin(participants[level + 1]))
                }
            } else {
                participants[level].filter { it.level() > 0 }.map { it.closeLevel() }
                singleJoinStack.pop()
                if (singleJoinStack.isNotEmpty()) {
                    singleJoinStack.peek().next()
                    candidateTuple.removeLast()
                }
            }
        }
        return results
    }
}