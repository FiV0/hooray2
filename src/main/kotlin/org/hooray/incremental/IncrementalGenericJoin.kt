package org.hooray.incremental

import org.hooray.UniversalComparator
import org.hooray.algo.Extension
import org.hooray.algo.Prefix
import org.hooray.algo.ResultTuple
import org.hooray.iterator.LevelParticipation
import kotlin.collections.contains

interface IncrementalPrefixExtender : LevelParticipation {
    fun count(prefix: Prefix): Int
    fun receiveDelta(delta: IndexedZSet<Extension, *, IntegerWeight>)
    fun intersectDelta(otherDelta: IndexedZSet<Extension, *, IntegerWeight>): ZSet<Extension, IntegerWeight>


    fun intersect(prefix: Prefix, proposals: ZSet<Extension, IntegerWeight>): ZSet<Extension, IntegerWeight>  // all others filter


//    fun count(prefix: Prefix): Int
//    fun propose(prefix: Prefix) : ZSet<Extension, IntegerWeight>
//    fun extend(prefix: Prefix, extensions: ZSet<Extension, IntegerWeight>) : ZSet<Extension, IntegerWeight>

    companion object {
        @JvmStatic
        fun createSingleLevel(deltas: ZSet<Extension, IntegerWeight>, participatesInLevel: Int): IncrementalPrefixExtender {
            return object : IncrementalPrefixExtender {
                override fun receiveDelta(delta: IndexedZSet<Extension, *, IntegerWeight>) {
                    TODO("Not yet implemented")
                }

                override fun intersectDelta(otherDelta: IndexedZSet<Extension, *, IntegerWeight>): ZSet<Extension, IntegerWeight> {
                    TODO("Not yet implemented")
                }

                override fun count(prefix: Prefix): Int = deltas.size


                override fun intersect(prefix: Prefix, proposals: ZSet<Extension, IntegerWeight>): ZSet<Extension, IntegerWeight> {
                    TODO("Not yet implemented")
                }

                override fun participatesInLevel(level: Int) = level == participatesInLevel
            }
        }

//        fun createTupleExtender(tuple: ResultTuple): IncrementalPrefixExtender {
//            return object : IncrementalPrefixExtender {
//                private fun isPrefixMatching(prefix: Prefix): Boolean =
//                    prefix.size <= tuple.size && tuple.take(prefix.size) == prefix
//
//                override fun count(prefix: Prefix): Int = if (isPrefixMatching(prefix)) 1 else 0
//
//                override fun propose(prefix: Prefix): List<Extension> =
//                    if (isPrefixMatching(prefix)) listOf(tuple[prefix.size]) else emptyList()
//
//                override fun extend(prefix: Prefix, extensions: List<Extension>): List<Extension> =
//                    if (isPrefixMatching(prefix) && extensions.contains(tuple[prefix.size])) listOf(tuple[prefix.size]) else emptyList()
//
//                override fun participatesInLevel(level: Int) = level < tuple.size
//            }
//        }
    }
}


class IncrementalGenericJoin(val extenders: List<IncrementalPrefixExtender>, levels: Int): IncrementalJoin<Any> {

    override fun join(deltas: List<IndexedZSet<Extension, *, IntegerWeight>>): ZSet<Any, IntegerWeight> {
        for (i in extenders.indices) {
            extenders[i].receiveDelta(deltas[i])
        }
        TODO()
    }
}