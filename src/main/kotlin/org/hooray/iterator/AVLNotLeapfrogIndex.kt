package org.hooray.iterator

import org.hooray.algo.LeapfrogIndex
import org.hooray.algo.LeapfrogJoin
import org.hooray.algo.NotLeapfrogIndex
import org.hooray.algo.ResultTuple

class AVLNotLeapfrogIndex(
    private val negatives: List<LeapfrogIndex>,
    private val participationLevel: Int
) : NotLeapfrogIndex {

    override fun checkNegation(positiveTuple: ResultTuple): Boolean {
        if (negatives.isEmpty()) return true

        negatives.forEach { it.reinit() }
        val tupleIndex = LeapfrogIndex.createFromTuple(positiveTuple)
        val join = LeapfrogJoin(negatives + tupleIndex, positiveTuple.size)
        return join.join().isEmpty()
    }

    override fun participatesInLevel(level: Int) = level == participationLevel
}
