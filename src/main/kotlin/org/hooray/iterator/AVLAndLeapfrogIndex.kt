package org.hooray.iterator

import org.hooray.UniversalComparator
import org.hooray.algo.LeapfrogIndex

class AVLAndLeapfrogIndex(private val children: List<LeapfrogIndex>) : LeapfrogIndex {

    init {
        require(children.isNotEmpty()) { "At least one child index is required" }
        require(children.all { it.maxLevel() == children.first().maxLevel() }) {
            "All child indices must have the same maxLevel"
        }
    }

    private var currentKey: Any? = null
    private var isAtEnd: Boolean = false

    init {
        findNextMatch()
    }

    private fun findNextMatch() {
        if (children.any { it.atEnd() }) {
            isAtEnd = true
            currentKey = null
            return
        }

        var maxKey = children.maxOfWith(UniversalComparator) { it.key() }

        while (true) {
            // Seek all children to at least maxKey
            children.forEach { it.seek(maxKey) }

            // Check if any child is now at end
            if (children.any { it.atEnd() }) {
                isAtEnd = true
                currentKey = null
                return
            }

            // Check if all children are at the same key
            val allMatch = children.all { UniversalComparator.compare(it.key(), maxKey) == 0 }

            if (allMatch) {
                currentKey = maxKey
                return
            }

            // Find new max and continue
            maxKey = children.maxOfWith(UniversalComparator) { it.key() }
        }
    }

    override fun seek(key: Any) {
        if (isAtEnd) return

        // Only seek if the target is greater than current position
        if (currentKey != null && UniversalComparator.compare(key, currentKey!!) <= 0) {
            return
        }

        children.forEach { it.seek(key) }
        findNextMatch()
    }

    override fun key(): Any {
        if (isAtEnd) {
            throw IllegalStateException("Cannot call key() when at end")
        }
        return currentKey!!
    }

    override fun next(): Any {
        if (isAtEnd) {
            throw IllegalStateException("Cannot call next() when at end")
        }

        // Advance all children past the current match
        children.forEach { it.next() }
        findNextMatch()

        return if (isAtEnd) Unit else currentKey!!
    }

    override fun atEnd(): Boolean = isAtEnd

    override fun openLevel() {
        children.forEach { it.openLevel() }
        isAtEnd = false
        currentKey = null
        findNextMatch()
    }

    override fun closeLevel() {
        children.forEach { it.closeLevel() }
        isAtEnd = false
        currentKey = null
        findNextMatch()
    }

    override fun reinit() {
        children.forEach { it.reinit() }
        isAtEnd = false
        currentKey = null
        findNextMatch()
    }

    override fun level(): Int = children.first().level()

    override fun maxLevel(): Int = children.first().maxLevel()

    override fun participatesInLevel(level: Int): Boolean = children.all { it.participatesInLevel(level) }
}
