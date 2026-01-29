package org.hooray.iterator

import clojure.data.avl.AVLMap
import clojure.data.avl.AVLSet
import clojure.data.avl.IAVLSeq
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
            is AVLIndex.AVLMapIndex -> iteratorStack.push(AVLLeapfrogIteratorMap(index.map))
        }
    }

    internal class AVLLeapfrogIteratorSet(avlSet: AVLSet): LeapfrogIterator {
        var seq: IAVLSeq? = avlSet.seq() as IAVLSeq?

        override fun seek(key: Any) {
            seq = seq?.seek(key)
        }

        override fun next(): Any {
            seq = seq?.next() as IAVLSeq?
            return if (atEnd()) Unit else seq!!.first()
        }

        override fun key(): Any {
            return seq?.first() ?: throw IllegalStateException("At end")
        }

        override fun atEnd(): Boolean {
            // Cannot use isEmpty() as it throws NPE when seek goes past the end
            // This is a bug in clojure.data.avl seek which needs to get addressed
            return seq == null || seq!!.first() == null
        }
    }

    internal class AVLLeapfrogIteratorMap(avlMap: AVLMap): LeapfrogIterator {
        var seq = avlMap.seq() as IAVLSeq?
        override fun seek(key: Any) {
            seq = seq?.seek(key)
        }

        override fun next(): Any {
            seq = seq?.next() as IAVLSeq?
            return if (atEnd()) Unit else (seq!!.first() as Map.Entry<*, *>).key as Any
        }

        override fun key(): Any = (seq?.first() as? Map.Entry<*, *>)?.key ?: throw IllegalStateException("At end")

        fun value(): Any = (seq?.first() as? Map.Entry<*, *>)?.value ?: throw IllegalStateException("At end")

        override fun atEnd(): Boolean = seq == null || seq!!.first() == null
    }

    override fun seek(key: Any) = iteratorStack.peek().seek(key)

    override fun next() = iteratorStack.peek().next()

    override fun key() = iteratorStack.peek().key()

    override fun atEnd() = iteratorStack.peek().atEnd()

    override fun openLevel(prefix: List<Any>) {
        val maxLevel = maxLevel()
        level++
        check(level < maxLevel) { "Cannot open level beyond max level $maxLevel" }
        when (val currentIndex = iteratorStack.peek() ) {
            is AVLLeapfrogIteratorMap -> {
                when(val newIndex = currentIndex.value()) {
                    is AVLMap -> iteratorStack.push(AVLLeapfrogIteratorMap(newIndex))
                    is AVLSet -> iteratorStack.push(AVLLeapfrogIteratorSet(newIndex))
                    else -> throw IllegalStateException("Unsupported value type in AVLLeapfrogIteratorMap: ${newIndex.javaClass}")
                }
            }
            else -> throw IllegalStateException("Cannot open level on AVLLeapfrogIteratorSet")
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
            is AVLIndex.AVLMapIndex -> iteratorStack.push(AVLLeapfrogIteratorMap(index.map))
        }
    }

    override fun level() = level

    override fun maxLevel() = variableOrder.size

    override fun participatesInLevel(level: Int) = variables.contains(variableOrder[level])
}