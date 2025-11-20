package org.hooray.iterator

import org.hooray.algo.GenericJoin
import org.hooray.algo.PrefixExtender
import org.hooray.algo.ResultTuple
import org.hooray.algo.ResultTupleFilter

open class GenericResultTupleRemover(val children: List<PrefixExtender>) : ResultTupleFilter , LevelParticipation {
    init {
        check(children.isNotEmpty()) { "At least one child extender is required" }
    }

    override fun filter(results: List<ResultTuple>): List<ResultTuple> {
        val filteredResults = mutableListOf<ResultTuple>()
        for (resultTuple in results) {
            val tuplePrefixExtender = PrefixExtender.createTupleExtender(resultTuple)
            val join = GenericJoin(children + tuplePrefixExtender, resultTuple.size)
            join.join().takeIf { it.isEmpty() }?.let {
                filteredResults.add(resultTuple)
            }
        }
        return filteredResults
    }

    override fun participatesInLevel(level: Int) = children.any { it.participatesInLevel(level) }
}