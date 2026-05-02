package org.hooray.incremental.iterator

import org.hooray.algo.Extension
import org.hooray.algo.Prefix
import org.hooray.incremental.IZSet
import org.hooray.incremental.IncrementalIndex
import org.hooray.incremental.IndexType
import org.hooray.incremental.IndexedZSet
import org.hooray.incremental.IntegerWeight
import org.hooray.incremental.ZSet
import org.hooray.incremental.ZSetIndices
import org.hooray.incremental.ZSetPrefixExtender
import org.hooray.incremental.getByType

class GenericIncrementalIndex(val indexType: IndexType,
                              val fixedPrefix: Prefix,
                              val participatesInLevel: List<Int>): IncrementalIndex {
    var accumulatedZSet: IZSet<*, IntegerWeight, *>
    var deltaZSet: IZSet<*, IntegerWeight, *>
    var empty: IZSet<*, IntegerWeight, *>

    init {
        accumulatedZSet = when (fixedPrefix.size) {
            0, 1 -> IndexedZSet.empty<Any, IntegerWeight>(IntegerWeight.ZERO, IntegerWeight.ONE)
            2 -> ZSet.empty()
            else -> throw IllegalArgumentException("Unsupported prefix size ${fixedPrefix.size} for GenericIncrementalIndex")
        }
        empty = accumulatedZSet
        deltaZSet = empty
    }

    override fun receiveDelta(delta: ZSetIndices) {
        deltaZSet = delta.getByType(indexType).getByPrefix(fixedPrefix) ?: empty
    }

    @Suppress("UNCHECKED_CAST")
    private fun addZSets(left: IZSet<*, IntegerWeight, *>, right: IZSet<*, IntegerWeight, *>): IZSet<*, IntegerWeight, *> {
        if (left.isEmpty()) return right
        if (right.isEmpty()) return left
        return when {
            left is IndexedZSet<*, *> && right is IndexedZSet<*, *> ->
                (left as IndexedZSet<Any, IntegerWeight>).add(right as IndexedZSet<Any, IntegerWeight>)
            left is ZSet<*, *> && right is ZSet<*, *> ->
                (left as ZSet<Any, IntegerWeight>).add(right as ZSet<Any, IntegerWeight>)
            else -> throw IllegalStateException("Cannot add ${left::class} and ${right::class}")
        }
    }

    @Suppress("UNCHECKED_CAST")
    override fun commit() {
        accumulatedZSet = addZSets(accumulatedZSet, deltaZSet)
    }

    @Suppress("UNCHECKED_CAST")
    private fun indexToPrefixExtender(zset: IZSet<*, IntegerWeight, *>) : ZSetPrefixExtender  {
        return when (zset) {
            is IndexedZSet<*, IntegerWeight> ->  {
                val prefixExtracter: (Prefix) -> Prefix = { prefix ->
                    // TODO make this fast
                    prefix.filterIndexed { index, _ -> participatesInLevel.contains(index) }
                }
                ZSetPrefixExtender.fromIndexedZSet(zset, prefixExtracter)
            }
            // here we assume that simple ZSets are not prefix dependent
            is ZSet<*, IntegerWeight> -> ZSetPrefixExtender.fromZSet(zset as ZSet<Extension, IntegerWeight>)
            else -> throw IllegalArgumentException("Unsupported IZSet type ${zset::class}")
        }
    }

    override val delta: ZSetPrefixExtender
        get() = indexToPrefixExtender(deltaZSet)

    override val accumulated: ZSetPrefixExtender
        get() = indexToPrefixExtender(accumulatedZSet)

    override val current: ZSetPrefixExtender
        get() = indexToPrefixExtender(addZSets(accumulatedZSet, deltaZSet))

    override fun participatesInLevel(level: Int): Boolean = participatesInLevel.contains(level)
}
