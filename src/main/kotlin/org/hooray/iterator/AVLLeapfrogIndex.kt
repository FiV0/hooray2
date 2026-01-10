package org.hooray.iterator

import clojure.data.avl.AVLMap
import clojure.data.avl.AVLSet
import clojure.data.avl.IAVLSeq
import clojure.lang.MapEntry
import clojure.lang.Symbol
import org.hooray.algo.LeapfrogIndex
import org.hooray.algo.LeapfrogIterator
import java.util.Stack

@Suppress("UNCHECKED_CAST")
class AVLLeapfrogIndex(private val index: AVLIndex , val variableOrder: List<Symbol>, val variables: Set<Symbol>) : LeapfrogIndex {
    var level = 0
    var iteratorStack: Stack<LeapfrogIterator>

    init {
        level = 0
        iteratorStack = Stack<LeapfrogIterator>()
        when(index) {
            is AVLIndex.AVLSetIndex -> iteratorStack.push(AVLLeapfrogIteratorSet(index.set))
            is AVLIndex.AVLMapIndex -> iteratorStack.push(AVLLeapFrogIteratorMap(index.map))
        }
    }

    internal class AVLLeapfrogIteratorSet(avlSet: AVLSet): LeapfrogIterator {
        var seq = avlSet.seq() as IAVLSeq

        override fun seek(key: Any) {
            seq = seq.seek(key)
        }

        override fun next(): Any {
            seq = seq.next() as IAVLSeq
            return key()
        }

        override fun key(): Any = seq.first()

        override fun atEnd(): Boolean = seq.isEmpty()
    }

    internal class AVLLeapFrogIteratorMap(avlMap: AVLMap): LeapfrogIterator {
        var seq = avlMap.seq() as IAVLSeq?
        override fun seek(key: Any) {
            seq = seq?.seek(key)
        }

        override fun next(): Any {
            seq = seq?.next() as IAVLSeq?
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
            is AVLLeapFrogIteratorMap -> {
                when(val newIndex = currentIndex.value()) {
                    is AVLMap -> iteratorStack.push(AVLLeapFrogIteratorMap(newIndex))
                    is AVLSet -> iteratorStack.push(AVLLeapfrogIteratorSet(newIndex))
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
            is AVLIndex.AVLSetIndex -> iteratorStack.push(AVLLeapfrogIteratorSet(index.set))
            is AVLIndex.AVLMapIndex -> iteratorStack.push(AVLLeapFrogIteratorMap(index.map))
        }
    }

    override fun level() = level

    override fun maxLevel() = variableOrder.size

    override fun participatesInLevel(level: Int) = variables.contains(variableOrder[level])
}