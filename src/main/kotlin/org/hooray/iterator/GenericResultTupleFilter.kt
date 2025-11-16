package org.hooray.iterator

import org.hooray.algo.ResultTuple
import org.hooray.algo.ResultTupleFilter

open class GenericResultTupleFilter(val children: List<GenericPrefixExtender>) : ResultTupleFilter , LevelParticipation {
    init {
        check(children.isNotEmpty()) { "At least one child extender is required" }
    }


    override fun filter(results: List<ResultTuple>): Boolean {
        TODO("Not yet implemented")
    }

    override fun participatesInLevel(level: Int): Boolean {
        TODO("Not yet implemented")
    }


}