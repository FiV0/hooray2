package org.hooray.iterator

import org.hooray.UniversalComparator
import org.hooray.algo.LeapfrogIndex

class AVLFnLeapfrogIndex(
    val inputLevels: List<Int>,
    val outputLevel: Int,
    val fn: Any
) : LeapfrogIndex {
    private var computedValue: Any? = null
    private var pastValue = false
    private var opened = false

    init {
        require(inputLevels.size in 1..2) { "Hooray only supports unary and binary functions for now." }
        require(inputLevels.all { it < outputLevel }) { "All input levels must be before output level." }
    }

    override fun participatesInLevel(level: Int): Boolean = level == outputLevel

    override fun openLevel(prefix: List<Any>) {
        computedValue = applyFn(prefix)
        pastValue = false
        opened = true
    }

    override fun closeLevel() {
        computedValue = null
        pastValue = false
        opened = false
    }

    override fun seek(key: Any) {
        pastValue = UniversalComparator.compare(key, computedValue!!) > 0
    }

    override fun next(): Any {
        pastValue = true
        return Unit
    }

    override fun key(): Any = computedValue!!

    override fun atEnd(): Boolean = pastValue

    override fun level(): Int = if (opened) 1 else 0

    override fun maxLevel(): Int = outputLevel + 1

    override fun reinit() {
        computedValue = null
        pastValue = false
        opened = false
    }

    @Suppress("UNCHECKED_CAST")
    private fun applyFn(prefix: List<Any>): Any {
        return when (inputLevels.size) {
            1 -> (fn as Fn1<Any, Any>)(prefix[inputLevels[0]])
            2 -> (fn as Fn2<Any, Any, Any>)(prefix[inputLevels[0]], prefix[inputLevels[1]])
            else -> throw IllegalStateException("Unreachable")
        }
    }
}
