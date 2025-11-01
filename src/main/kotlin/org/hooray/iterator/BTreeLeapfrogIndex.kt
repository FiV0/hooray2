package org.hooray.iterator

import clojure.lang.MapEntry
import clojure.lang.Symbol
import org.hooray.algo.LeapfrogIndex
import me.tonsky.persistent_sorted_set.IPersistentSortedSet
import me.tonsky.persistent_sorted_set.Seq
import org.hooray.algo.LeapfrogIterator
import org.hooray.util.IPersistentSortedMap
import org.hooray.util.IPersistentSortedMapSeq
import java.util.Stack

@Suppress("UNCHECKED_CAST")
class BTreeLeapfrogIndex(val index: Any, val variableOrder: List<Symbol>, val variables: Set<Symbol>) : LeapfrogIndex {
    var level = 0
    var iteratorStack: Stack<LeapfrogIterator>

    init {
        level = 0
        iteratorStack = Stack<LeapfrogIterator>()
        when(variables.size) {
            0 -> throw IllegalArgumentException("At least one variable must be present")
            1 -> iteratorStack.push(BTreeLeapfrogIteratorSet(index as IPersistentSortedSet<Any, Any>))
            else -> iteratorStack.push(BTreeLeapFrogIteratorMap(index as IPersistentSortedMap))
        }
    }

    internal class BTreeLeapfrogIteratorSet(btreeSet: IPersistentSortedSet<Any, Any>): LeapfrogIterator {
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

        override fun key(): Any = (seq?.first() as MapEntry).`val`()

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
        if(level + 1 == maxLevel) {
            iteratorStack.push(BTreeLeapfrogIteratorSet(index as IPersistentSortedSet<Any, Any>))
        } else {
            iteratorStack.push(BTreeLeapFrogIteratorMap(index as IPersistentSortedMap))
        }
    }

    override fun closeLevel() {
        check(level > 0) { "Cannot close level below 0" }
        iteratorStack.pop()
        level--
    }

    override fun level() = level

    override fun maxLevel() = variableOrder.size

    override fun participatesInLevel(level: Int) = variables.contains(variableOrder[level])

}