package org.hooray.iterator

import org.hooray.UniversalComparator
import org.hooray.algo.LeapfrogIndex

class AVLNotLeapfrogIndex(
    private val positive: LeapfrogIndex,
    private val negative: List<LeapfrogIndex>,
    private val participationLevel: Int
) : LeapfrogIndex {

    init {
        require(negative.isEmpty() || negative.all { it.maxLevel() == positive.maxLevel() }) {
            "All indices must have the same maxLevel"
        }
    }

    init {
        advanceToNextValid()
    }

    private fun shouldExclude(key: Any): Boolean {
        if (negative.isEmpty()) return false

        return negative.all { child ->
            child.seek(key)
            !child.atEnd() && UniversalComparator.compare(child.key(), key) == 0
        }
    }

    private fun advanceToNextValid() {
        while (!positive.atEnd() && shouldExclude(positive.key())) {
            positive.next()
        }
    }

    override fun seek(key: Any) {
        positive.seek(key)
        advanceToNextValid()
    }

    override fun key(): Any {
        if (positive.atEnd()) {
            throw IllegalStateException("Cannot call key() when at end")
        }
        return positive.key()
    }

    override fun next(): Any {
        if (positive.atEnd()) {
            throw IllegalStateException("Cannot call next() when at end")
        }

        positive.next()
        advanceToNextValid()

        return if (positive.atEnd()) Unit else positive.key()
    }

    override fun atEnd(): Boolean = positive.atEnd()

    override fun openLevel() {
        positive.openLevel()
        negative.forEach { it.openLevel() }
        advanceToNextValid()
    }

    override fun closeLevel() {
        positive.closeLevel()
        negative.forEach { it.closeLevel() }
        advanceToNextValid()
    }

    override fun level(): Int = positive.level()

    override fun maxLevel(): Int = positive.maxLevel()

    override fun participatesInLevel(level: Int): Boolean {
        return if (level == participationLevel) true else positive.participatesInLevel(level)
    }
}
