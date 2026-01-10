package org.hooray.iterator

import org.hooray.algo.LeapfrogIndex
import org.hooray.algo.LeapfrogJoin
import org.hooray.algo.FilterLeapfrogIndex
import org.hooray.algo.ResultTuple

class AVLNotLeapfrogIndex(
    private val negatives: List<LeapfrogIndex>,
    private val participationLevel: Int
) : FilterLeapfrogIndex {

    override fun accept(tuple: ResultTuple): Boolean {
        if (negatives.isEmpty()) return true

        negatives.forEach { it.reinit() }
        val tupleIndex = LeapfrogIndex.createFromTuple(tuple)
        val join = LeapfrogJoin(negatives + tupleIndex, tuple.size)
        return join.join().isEmpty()
    }

    override fun participatesInLevel(level: Int) = level == participationLevel
}
