package org.hooray.iterator

import org.hooray.algo.LeapfrogIndex
import org.hooray.algo.LeapfrogJoin
import org.hooray.algo.NotLeapfrogIndex
import org.hooray.algo.ResultTuple

class AVLNotLeapfrogIndex(
    private val negatives: List<LeapfrogIndex>,
    private val participationLevel: Int
) : NotLeapfrogIndex {

    override fun checkNegation(positiveTuples: List<ResultTuple>): List<ResultTuple> {
        if (negatives.isEmpty()) return positiveTuples

        val result = mutableListOf<ResultTuple>()

        for (tuple in positiveTuples) {
            val tupleIndex = LeapfrogIndex.createFromTuple(tuple)
            val join = LeapfrogJoin(negatives + tupleIndex, tuple.size)
            if (join.join().isEmpty()) {
                result.add(tuple)
            }
        }

        return result
    }

    override fun participatesInLevel(level: Int) = level == participationLevel
}
