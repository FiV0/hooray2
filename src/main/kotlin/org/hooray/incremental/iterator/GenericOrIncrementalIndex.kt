package org.hooray.incremental.iterator

import org.hooray.algo.Extension
import org.hooray.algo.Prefix
import org.hooray.incremental.IncrementalIndex
import org.hooray.incremental.IntegerWeight
import org.hooray.incremental.ZSet
import org.hooray.incremental.ZSetIndices
import org.hooray.incremental.ZSetPrefixExtender

class GenericOrIncrementalIndex(val children: List<IncrementalIndex>) : IncrementalIndex {

    override fun receiveDelta(delta: ZSetIndices) = children.forEach { it.receiveDelta(delta) }


    override fun commit() = children.forEach { it.commit() }

    private fun fromChildrenZSetPrefixExtenders(children : List<ZSetPrefixExtender>) =
        object : ZSetPrefixExtender {
            override fun count(prefix: Prefix) = children.sumOf { it.count(prefix) }

            // ??? Is distinct really not needed here, as we do an explicit distinct call after the join
            override fun propose(prefix: Prefix): ZSet<Extension, IntegerWeight> = children
                .map { it.propose(prefix) }
                .reduce { acc, zset -> acc.add(zset) }


            override fun intersect(prefix: Prefix, extensions: ZSet<Extension, IntegerWeight>): ZSet<Extension, IntegerWeight> {
                var result = ZSet.empty<Extension>()
                for (child in children) {
                    val childExtensions = child.intersect(prefix, extensions)
                    result = result.add(childExtensions)
                }
                return result
            }
        }

    override val delta: ZSetPrefixExtender
        get() = fromChildrenZSetPrefixExtenders(children.map { it.delta })

    override val accumulated: ZSetPrefixExtender
        get() = fromChildrenZSetPrefixExtenders(children.map { it.accumulated })

    override fun participatesInLevel(level: Int) = children.first().participatesInLevel(level)
}