package org.hooray.iterator

import org.hooray.algo.LeapfrogIndex
import org.hooray.algo.LeapfrogJoin
import org.hooray.algo.FilterLeapfrogIndex
import org.hooray.algo.ResultTuple

class AVLNotLeapfrogIndex(
    private val negatives: List<Any>,  // Can be LeapfrogIndex or FilterLeapfrogIndex
    private val participationLevel: Int
) : FilterLeapfrogIndex {

    override fun accept(tuple: ResultTuple): Boolean {
        if (negatives.isEmpty()) return true

        // Separate LeapfrogIndex from FilterLeapfrogIndex
        val indexes = negatives.filterIsInstance<LeapfrogIndex>()
        val filters = negatives.filterIsInstance<FilterLeapfrogIndex>()

        // Reinit all indexes
        indexes.forEach { it.reinit() }

        val tupleIndex = LeapfrogIndex.createFromTuple(tuple)
        val join = LeapfrogJoin(indexes + tupleIndex, tuple.size, filters)
        return join.join().isEmpty()
    }

    override fun participatesInLevel(level: Int) = level == participationLevel
}
