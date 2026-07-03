package org.hooray.iterator

import org.hooray.UniversalComparator
import org.hooray.algo.FnLeapfrogIndex
import org.hooray.algo.Prefix

class AVLFnLeapfrogIndex(
    val inputLevels: List<Int>,
    val outputLevel: Int,
    val fn: Any
) : FnLeapfrogIndex {
    private var computedValue: Any? = null

    init {
        require(inputLevels.size in 1..2) { "Hooray only supports unary and binary functions for now." }
        require(inputLevels.all { it < outputLevel }) { "All input levels must be before output level." }
    }

    override fun participatesInLevel(level: Long): Boolean = level == outputLevel.toLong()

    override fun openLevel() {}

    override fun closeLevel() {
        computedValue = null
    }

    override fun seek(key: Any) {
        if (UniversalComparator.compare(key, computedValue!!) > 0) {
            computedValue = null
        }
    }

    override fun next(): Any {
        computedValue = null
        return Unit
    }

    override fun key(): Any = computedValue!!

    override fun atEnd(): Boolean = computedValue == null

    override fun level(): Int = if (computedValue != null) 0 else -1

    override fun maxLevel(): Int = outputLevel + 1

    override fun reinit() {
        computedValue = null
    }

    @Suppress("UNCHECKED_CAST")
    override fun applyFn(prefix: Prefix) {
        computedValue = when (inputLevels.size) {
            1 -> (fn as Fn1<Any, Any>)(prefix[inputLevels[0]])
            2 -> (fn as Fn2<Any, Any, Any>)(prefix[inputLevels[0]], prefix[inputLevels[1]])
            else -> throw IllegalStateException("Unreachable")
        }
    }
}
