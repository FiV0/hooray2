package org.hooray.iterator

import clojure.lang.MapEntry
import clojure.lang.Symbol
import me.tonsky.persistent_sorted_set.APersistentSortedSet
import me.tonsky.persistent_sorted_set.Seq
import org.hooray.algo.LeapfrogIndex
import org.hooray.algo.LeapfrogIterator
import org.hooray.util.IPersistentSortedMap
import org.hooray.util.IPersistentSortedMapSeq
import java.util.*

@Suppress("UNCHECKED_CAST")
class BTreeLeapfrogIndex(private val index: BTreeIndex, val variableOrder: List<Symbol>, val variables: Set<Symbol>) : LeapfrogIndex {
    var level = 0
    var iteratorStack: Stack<LeapfrogIterator>

    init {
        level = 0
        iteratorStack = Stack<LeapfrogIterator>()
        when(index) {
            is BTreeIndex.BTreeSet -> iteratorStack.push(BTreeLeapfrogIteratorSet(index.set))
            is BTreeIndex.BTreeMap -> iteratorStack.push(BTreeLeapFrogIteratorMap(index.map))
        }
    }

    internal class BTreeLeapfrogIteratorSet(btreeSet: APersistentSortedSet<Any, Any>): LeapfrogIterator {
        var seq = btreeSet.seq() as Seq

        override fun seek(key: Any) {
            seq = seq.seek(key)
        }

        override fun next(): Any {
            seq = seq.next() as Seq
            return key()
        }

        override fun key(): Any = seq.first()

        override fun atEnd(): Boolean = seq.isEmpty()
    }

    internal class BTreeLeapFrogIteratorMap(btreeMap: IPersistentSortedMap): LeapfrogIterator {
        var seq = btreeMap.seq() as IPersistentSortedMapSeq?
        override fun seek(key: Any) {
            seq = seq?.seek(key) as IPersistentSortedMapSeq
        }

        override fun next(): Any {
            seq = seq?.next() as IPersistentSortedMapSeq
            return key()
        }

        override fun key(): Any = (seq?.first() as MapEntry).key()

        fun value(): Any = (seq?.first() as MapEntry).`val`()

        override fun atEnd(): Boolean = seq == null
    }

    override fun seek(key: Any) = iteratorStack.peek().seek(key)

    override fun next() = iteratorStack.peek().next()

    override fun key() = iteratorStack.peek().key()

    override fun atEnd() = iteratorStack.peek().atEnd()

    override fun openLevel() {
        val maxLevel = maxLevel()
        level++
        check(level < maxLevel) { "Cannot open level beyond max level $maxLevel" }
        when (val currentIndex = iteratorStack.peek() ) {
            is BTreeLeapFrogIteratorMap -> {
                when(val newIndex = currentIndex.value()) {
                    is IPersistentSortedMap -> iteratorStack.push(BTreeLeapFrogIteratorMap(newIndex))
                    is APersistentSortedSet<*, *> -> iteratorStack.push(BTreeLeapfrogIteratorSet(newIndex as APersistentSortedSet<Any, Any>))
                    else -> throw IllegalStateException("Unsupported value type in BTreeLeapFrogIteratorMap: ${newIndex.javaClass}")
                }
            }
            else -> throw IllegalStateException("Cannot open level on BTreeLeapfrogIteratorSet")
        }
    }

    override fun closeLevel() {
        check(level > 0) { "Cannot close level below 0" }
        iteratorStack.pop()
        level--
    }

    override fun reinit() {
        level = 0
        iteratorStack = Stack<LeapfrogIterator>()
        when(index) {
            is BTreeIndex.BTreeSet -> iteratorStack.push(BTreeLeapfrogIteratorSet(index.set))
            is BTreeIndex.BTreeMap -> iteratorStack.push(BTreeLeapFrogIteratorMap(index.map))
        }
    }

    override fun level() = level

    override fun maxLevel() = variableOrder.size

    override fun participatesInLevel(level: Int) = variables.contains(variableOrder[level])
}