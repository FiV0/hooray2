package org.hooray.algo

import kotlinx.collections.immutable.toPersistentList
import org.hooray.UniversalComparator
import org.hooray.iterator.LevelParticipation
import java.util.Stack
import java.util.TreeMap
import java.util.TreeSet

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

private sealed interface IndexLevel {
    data class MapLevel(val map: TreeMap<Any, IndexLevel>) : IndexLevel
    data class SetLevel(val set: TreeSet<Any>) : IndexLevel
}

interface LeapfrogIndex : LeapfrogIterator, LayeredIndex, LevelParticipation {
    companion object {
        @JvmStatic
        fun createSingleLevel(values: List<Int>, participatingLevel: Int = 0): LeapfrogIndex {
            val sortedValues = values.sortedWith(UniversalComparator)
            return object : LeapfrogIndex {
                private var currentIndex = 0

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

                override fun atEnd(): Boolean = currentIndex >= sortedValues.size

                override fun openLevel(prefix: List<Any>) {
                    currentIndex = 0
                }

                override fun closeLevel() {}

                override fun reinit() {
                    currentIndex = 0
                }

                override fun level(): Int = 0

                override fun maxLevel(): Int = 1

                override fun participatesInLevel(level: Int): Boolean = level == participatingLevel
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
        fun createFromPrefixLeapfrogIndex(participatingLevels: List<Int>, partialPrefix: Prefix): LeapfrogIndex {
            require(participatingLevels.size == partialPrefix.size) {
                "participatingLevels and partialPrefix must have same size"
            }
            val levelSet = participatingLevels.toSet()

            return object : LeapfrogIndex {
                private var currentLevelIndex = 0  // index into participatingLevels/partialPrefix
                private var pastValue = false

                // Check if prefix matches our partial prefix at participating levels
                private fun checkPrefixMatch(prefix: Prefix): Boolean {
                    for (i in 0..currentLevelIndex) {
                        if (i >= participatingLevels.size) break
                        val level = participatingLevels[i]
                        if (level >= prefix.size || partialPrefix[i] != prefix[level]) {
                            return false
                        }
                    }
                    return true
                }

                override fun seek(key: Any) {
                    if (currentLevelIndex >= partialPrefix.size) {
                        pastValue = true
                        return
                    }
                    val value = partialPrefix[currentLevelIndex]
                    pastValue = UniversalComparator.compare(key, value) > 0
                }

                override fun next(): Any {
                    pastValue = true
                    return Unit
                }

                override fun key(): Any {
                    if (pastValue || currentLevelIndex >= partialPrefix.size) {
                        throw IllegalStateException("At end")
                    }
                    return partialPrefix[currentLevelIndex]
                }

                override fun atEnd(): Boolean = pastValue || currentLevelIndex >= partialPrefix.size

                override fun openLevel(prefix: List<Any>) {
                    if (!checkPrefixMatch(prefix)) {
                        pastValue = true
                        return
                    }
                    currentLevelIndex++
                    pastValue = false
                }

                override fun closeLevel() {
                    currentLevelIndex--
                    pastValue = false
                }

                override fun reinit() {
                    currentLevelIndex = 0
                    pastValue = false
                }

                override fun level(): Int = currentLevelIndex

                override fun maxLevel(): Int = partialPrefix.size

                override fun participatesInLevel(level: Int): Boolean = levelSet.contains(level)
            }
        }

        @JvmStatic
        fun createFromPrefixesLeapfrogIndex(participatingLevels: List<Int>, partialPrefixes: List<Prefix>): LeapfrogIndex {
            require(participatingLevels.isNotEmpty()) { "participatingLevels cannot be empty" }
            require(partialPrefixes.isNotEmpty()) { "partialPrefixes cannot be empty" }
            require(partialPrefixes.all { it.size == participatingLevels.size }) {
                "All partial prefixes must have size equal to participatingLevels.size"
            }

            val levelSet = participatingLevels.toSet()

            // Build hierarchical structure recursively
            fun buildStructure(levelIndex: Int, prefixes: List<Prefix>): IndexLevel {
                if (levelIndex == participatingLevels.size - 1) {
                    // Leaf level - collect all values at this position
                    val values = TreeSet<Any>(UniversalComparator)
                    prefixes.forEach { prefix -> values.add(prefix[levelIndex]) }
                    return IndexLevel.SetLevel(values)
                } else {
                    // Intermediate level - group by current level value
                    val grouped = TreeMap<Any, MutableList<Prefix>>(UniversalComparator)
                    prefixes.forEach { prefix ->
                        grouped.computeIfAbsent(prefix[levelIndex]) { mutableListOf() }.add(prefix)
                    }

                    val map = TreeMap<Any, IndexLevel>(UniversalComparator)
                    grouped.forEach { (key, matchingPrefixes) ->
                        // Build next level structure recursively
                        map[key] = buildStructure(levelIndex + 1, matchingPrefixes)
                    }
                    return IndexLevel.MapLevel(map)
                }
            }

            val rootIndex = buildStructure(0, partialPrefixes)

            return object : LeapfrogIndex {
                private val indexStack = Stack<Triple<IndexLevel, Iterator<Any>, Any>>()
                private var currentIndex: IndexLevel = rootIndex
                private var currentIterator: Iterator<Any>? = null
                private var currentKey: Any? = null

                init {
                    when (rootIndex) {
                        is IndexLevel.MapLevel -> {
                            currentIterator = rootIndex.map.keys.iterator()
                            if (currentIterator!!.hasNext()) {
                                currentKey = currentIterator!!.next()
                            }
                        }
                        is IndexLevel.SetLevel -> {
                            currentIterator = rootIndex.set.iterator()
                            if (currentIterator!!.hasNext()) {
                                currentKey = currentIterator!!.next()
                            }
                        }
                    }
                }

                override fun seek(key: Any) {
                    when (val index = currentIndex) {
                        is IndexLevel.MapLevel -> {
                            val tailMap = index.map.tailMap(key)
                            currentIterator = tailMap.keys.iterator()
                            currentKey = if (currentIterator!!.hasNext()) {
                                currentIterator!!.next()
                            } else null
                        }
                        is IndexLevel.SetLevel -> {
                            val tailSet = index.set.tailSet(key)
                            currentIterator = tailSet.iterator()
                            currentKey = if (currentIterator!!.hasNext()) {
                                currentIterator!!.next()
                            } else null
                        }
                    }
                }

                override fun next(): Any {
                    currentKey = if (currentIterator!!.hasNext()) {
                        currentIterator!!.next()
                    } else null
                    return currentKey ?: Unit
                }

                override fun key(): Any {
                    return currentKey ?: throw IllegalStateException("At end")
                }

                override fun atEnd(): Boolean = currentKey == null

                override fun openLevel(prefix: List<Any>) {
                    if (currentKey == null) return

                    when (val index = currentIndex) {
                        is IndexLevel.MapLevel -> {
                            // Save current state
                            indexStack.push(Triple(index, currentIterator!!, currentKey!!))

                            // Descend to next level
                            val nextLevel = index.map[currentKey]
                            if (nextLevel != null) {
                                currentIndex = nextLevel
                                when (nextLevel) {
                                    is IndexLevel.MapLevel -> {
                                        currentIterator = nextLevel.map.keys.iterator()
                                        currentKey = if (currentIterator!!.hasNext()) {
                                            currentIterator!!.next()
                                        } else null
                                    }
                                    is IndexLevel.SetLevel -> {
                                        currentIterator = nextLevel.set.iterator()
                                        currentKey = if (currentIterator!!.hasNext()) {
                                            currentIterator!!.next()
                                        } else null
                                    }
                                }
                            } else {
                                currentKey = null
                            }
                        }
                        is IndexLevel.SetLevel -> {
                            // Cannot descend from a set (leaf level)
                            throw IllegalStateException("Cannot openLevel on a SetLevel")
                        }
                    }
                }

                override fun closeLevel() {
                    if (indexStack.isEmpty()) return

                    val frame = indexStack.pop()
                    currentIndex = frame.first
                    currentIterator = frame.second
                    currentKey = frame.third
                }

                override fun reinit() {
                    indexStack.clear()
                    currentIndex = rootIndex
                    when (rootIndex) {
                        is IndexLevel.MapLevel -> {
                            currentIterator = rootIndex.map.keys.iterator()
                            currentKey = if (currentIterator!!.hasNext()) {
                                currentIterator!!.next()
                            } else null
                        }
                        is IndexLevel.SetLevel -> {
                            currentIterator = rootIndex.set.iterator()
                            currentKey = if (currentIterator!!.hasNext()) {
                                currentIterator!!.next()
                            } else null
                        }
                    }
                }

                override fun level(): Int = indexStack.size

                override fun maxLevel(): Int = participatingLevels.size

                override fun participatesInLevel(level: Int): Boolean = levelSet.contains(level)
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