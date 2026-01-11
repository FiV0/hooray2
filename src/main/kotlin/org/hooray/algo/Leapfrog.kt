package org.hooray.algo

import kotlinx.collections.immutable.toPersistentList
import org.hooray.UniversalComparator
import org.hooray.iterator.LevelParticipation
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
    fun openLevel(prefix: List<Any>)
    fun closeLevel()
    fun level() : Int
    fun maxLevel(): Int
    fun reinit()
}

interface FilterLeapfrogIndex : LevelParticipation {
    fun accept(tuple: ResultTuple) : Boolean
}

interface LeapfrogIndex : LeapfrogIterator, LayeredIndex, LevelParticipation {
    companion object {
        @JvmStatic
        fun createSingleLevel(values: List<Int>, maxLevels: Int = 1): LeapfrogIndex {
            val sortedValues = values.sortedWith(UniversalComparator)
            return object : LeapfrogIndex {
                private var currentIndex = 0
                private var currentLevel = 0

                override fun seek(key: Any) {
                    // Find the first value >= key
                    while (currentIndex < sortedValues.size && UniversalComparator.compare(sortedValues[currentIndex], key) < 0) {
                        currentIndex++
                    }
                }

                override fun next(): Any {
                    if (currentIndex < sortedValues.size) {
                        currentIndex++
                    }
                    return if (atEnd()) Unit else sortedValues[currentIndex]
                }

                override fun key(): Any {
                    return if (atEnd()) throw IllegalStateException("At end") else sortedValues[currentIndex]
                }

                override fun atEnd(): Boolean {
                    return currentIndex >= sortedValues.size
                }

                override fun openLevel(prefix: List<Any>) {
                    currentLevel++
                }

                override fun closeLevel() {
                    currentLevel--
                    currentIndex = 0
                }

                override fun reinit() {
                    currentIndex = 0
                    currentLevel = 0
                }

                override fun level(): Int {
                    return currentLevel
                }

                override fun maxLevel(): Int {
                    return maxLevels
                }

                override fun participatesInLevel(level: Int): Boolean {
                    return level < maxLevels
                }
            }
        }

        @JvmStatic
        fun createFromTuple(tuple: ResultTuple): LeapfrogIndex {
            return object : LeapfrogIndex {
                private var currentLevel = 0
                private var pastValue = false

                override fun seek(key: Any) {
                    val value = tuple[currentLevel]
                    pastValue = UniversalComparator.compare(key, value) > 0
                }

                override fun next(): Any {
                    pastValue = true
                    return Unit
                }

                override fun key(): Any {
                    if (pastValue) throw IllegalStateException("At end")
                    return tuple[currentLevel]
                }

                override fun atEnd(): Boolean = pastValue

                override fun openLevel(prefix: List<Any>) {
                    currentLevel++
                    pastValue = false
                }

                override fun closeLevel() {
                    currentLevel--
                    pastValue = false
                }

                override fun reinit() {
                    currentLevel = 0
                    pastValue = false
                }

                override fun level(): Int = currentLevel

                override fun maxLevel(): Int = tuple.size

                override fun participatesInLevel(level: Int): Boolean = level < tuple.size
            }
        }

        @JvmStatic
        fun createAtLevel(values: List<Int>, targetLevel: Int, maxLevels: Int): LeapfrogIndex {
            require(targetLevel < maxLevels) { "targetLevel must be less than maxLevels" }
            val sortedValues = values.sortedWith(UniversalComparator)
            return object : LeapfrogIndex {
                private var currentIndex = 0
                private var currentLevel = 0

                override fun seek(key: Any) {
                    while (currentIndex < sortedValues.size && UniversalComparator.compare(sortedValues[currentIndex], key) < 0) {
                        currentIndex++
                    }
                }

                override fun next(): Any {
                    if (currentIndex < sortedValues.size) {
                        currentIndex++
                    }
                    return if (atEnd()) Unit else sortedValues[currentIndex]
                }

                override fun key(): Any {
                    return if (atEnd()) throw IllegalStateException("At end") else sortedValues[currentIndex]
                }

                override fun atEnd(): Boolean {
                    return currentIndex >= sortedValues.size
                }

                override fun openLevel(prefix: List<Any>) {
                    currentLevel = prefix.size  // We're opening to level = prefix.size
                    if (currentLevel == targetLevel) {
                        currentIndex = 0
                    }
                }

                override fun closeLevel() {
                    currentLevel--
                }

                override fun reinit() {
                    currentIndex = 0
                    currentLevel = 0
                }

                override fun level(): Int {
                    return currentLevel
                }

                override fun maxLevel(): Int {
                    return maxLevels
                }

                override fun participatesInLevel(level: Int): Boolean {
                    return level == targetLevel
                }
            }
        }
    }
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
        currentIteratorIndex = (currentIteratorIndex + 1) % iterators.size


        while (true) {
            if (currentIteratorIndex == startIndex) {
                candidateTuple.add(maxKey)
                return true
            }

            // Move to next iterator in round-robin fashion
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
            }
            currentIteratorIndex = (currentIteratorIndex + 1) % iterators.size
        }
    }
}

class LeapfrogJoin @JvmOverloads constructor(
    val indexes: List<LeapfrogIndex>,
    val levels: Int,
    val filterIndexes: List<FilterLeapfrogIndex> = emptyList()
) : Join<ResultTuple>  {
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
                    val resultTuple = candidateTuple.toPersistentList()
                    if (filterIndexes.all { it.accept(resultTuple) }) {
                        results.add(resultTuple)
                    }
                    candidateTuple.removeLast()
                    currentJoin.next()
                } else {
                    participants[level + 1].map { it.openLevel(candidateTuple.toList()) }
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