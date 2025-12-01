package org.hooray.incremental.iterator

import org.hooray.algo.Extension
import org.hooray.incremental.IZSet
import org.hooray.incremental.IncrementalIndex
import org.hooray.incremental.IndexedZSet
import org.hooray.incremental.IntegerWeight
import org.hooray.incremental.ZSet
import org.hooray.incremental.ZSetPrefixExtender

sealed interface SealedIncrementalIndex {
    data class Indexed(val indexed: IndexedZSet<Any, IntegerWeight>) : SealedIncrementalIndex
    data class Simple(val simple: ZSet<Any, IntegerWeight>) : SealedIncrementalIndex
}

class GenericIncrementalIndex(intialAccumulated: SealedIncrementalIndex, val participatesInLevel: List<Int>): IncrementalIndex {
    var accumulatedZSet: IZSet<*, IntegerWeight, *>
    var deltaZSet: IZSet<*, IntegerWeight, *>

    init {
        accumulatedZSet = when (intialAccumulated) {
            is SealedIncrementalIndex.Indexed -> intialAccumulated.indexed
            is SealedIncrementalIndex.Simple -> intialAccumulated.simple
        }
        deltaZSet = IndexedZSet.empty<Any, IntegerWeight>(IntegerWeight.ZERO, IntegerWeight.ONE)
    }

    override fun receiveDelta(delta: IndexedZSet<*, IntegerWeight>) {
        deltaZSet = delta as IZSet<*, IntegerWeight, *>
    }

    @Suppress("UNCHECKED_CAST")
    override fun commit() {
        when (accumulatedZSet) {
            is IndexedZSet<*, IntegerWeight> -> {
                accumulatedZSet = (accumulatedZSet as IndexedZSet<Any, IntegerWeight>).add(deltaZSet as IndexedZSet<Any, IntegerWeight>)
            }
            is ZSet<*, IntegerWeight> -> {
                accumulatedZSet = (accumulatedZSet as ZSet<Any, IntegerWeight>).add(deltaZSet as ZSet<Any, IntegerWeight>)
            }
        }

    }

    @Suppress("UNCHECKED_CAST")
    private fun indexToPrefixExtender(zset: IZSet<*, IntegerWeight, *>) : ZSetPrefixExtender  {
        return when (zset) {
            is IndexedZSet<*, IntegerWeight> -> ZSetPrefixExtender.fromIndexedZSet(zset)
            // here we assume that simple ZSets are not prefix dependent
            is ZSet<*, IntegerWeight> -> ZSetPrefixExtender.fromZSet(zset as ZSet<Extension, IntegerWeight>)
            else -> throw IllegalArgumentException("Unsupported IZSet type ${zset::class}")
        }
    }

    override val delta: ZSetPrefixExtender
        get() = indexToPrefixExtender(deltaZSet)

    override val accumulated: ZSetPrefixExtender
        get() = indexToPrefixExtender(accumulatedZSet)

    override fun participatesInLevel(level: Int): Boolean = participatesInLevel.contains(level)
}