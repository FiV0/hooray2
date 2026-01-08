package org.hooray.iterator

import org.hooray.UniversalComparator
import org.hooray.algo.LeapfrogIndex

class AVLOrLeapfrogIndex(private val children: List<LeapfrogIndex>) : LeapfrogIndex {

    init {
        require(children.isNotEmpty()) { "At least one child index is required" }
        require(children.all { it.maxLevel() == children.first().maxLevel() }) {
            "All child indices must have the same maxLevel"
        }
    }

    override fun seek(key: Any) {
        children.forEach { it.seek(key) }
    }

    override fun key(): Any {
        return children
            .filter { !it.atEnd() }
            .minOfWith(UniversalComparator) { it.key() }
    }

    override fun next(): Any {
        if (atEnd()) {
            throw IllegalStateException("Cannot call next() when all children are at end")
        }

        val minKey = key()

        children
            .filter { !it.atEnd() && UniversalComparator.compare(it.key(), minKey) == 0 }
            .forEach { it.next() }

        return if (atEnd()) Unit else key()
    }

    override fun atEnd(): Boolean = children.all { it.atEnd() }

    override fun openLevel() {
        children.forEach { it.openLevel() }
    }

    override fun closeLevel() {
        children.forEach { it.closeLevel() }
    }

    override fun level(): Int = children.first().level()

    override fun maxLevel(): Int = children.first().maxLevel()

    override fun participatesInLevel(level: Int): Boolean = children.first().participatesInLevel(level)
}
