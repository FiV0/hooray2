package org.hooray.incremental

import org.hooray.algo.Extension
import org.hooray.algo.Prefix
import org.hooray.algo.ResultTuple
import org.hooray.iterator.LevelParticipation

interface IncrementalPrefixExtender : LevelParticipation {
    fun receiveDeltaIndex(delta: IndexedZSet<*, IntegerWeight>)
    fun updateZ1(): Unit
    fun count(prefix: Prefix): Int
    fun getDelta(prefix: Prefix): ZSet<Extension, IntegerWeight>
    fun intersectDelta(prefix: Prefix, otherDelta: ZSet<Extension, IntegerWeight>): ZSet<Extension, IntegerWeight>
    fun intersectZ1(prefix: Prefix, runningDelta: ZSet<Extension, IntegerWeight>): ZSet<Extension, IntegerWeight>
}

class IncrementalGenericJoin(val extenders: List<IncrementalPrefixExtender>, val levels: Int): IncrementalJoin<ResultTuple> {

    override fun join(deltas: List<IndexedZSet<Any, IntegerWeight>>): ZSet<ResultTuple, IntegerWeight> {
        var result: IndexedZSet<Any, IntegerWeight> = IndexedZSet.empty(IntegerWeight.ZERO, IntegerWeight.ONE)
        for (i in extenders.indices) {
            extenders[i].receiveDeltaIndex(deltas[i])
        }

        for (i in 0 until levels) {
            val participatingExtenders = extenders.filter { it.participatesInLevel(i) }
            result = result.extendLeaves { prefix, weight ->
                val minIndex = participatingExtenders.indices.minBy { participatingExtenders[it].count(prefix) }
                var runningDelta = extenders[minIndex].getDelta(prefix)

                // The algorithm of this inner loop is approximately:
                // for i = 1 to n:
                // compute Δ_{1..i} from Δ_{1..i-1}, Δᵢ and relation i

                //      result = Δ_{1..i-1} ⋈ Δᵢ
                //               + Δ_{1..i-1} ⋈ z⁻¹(i)
                //               + Δᵢ ⋈ z⁻¹(1) ⋈ z⁻¹(2) ⋈ ... ⋈ z⁻¹(i-1)  // WCOJ anchored by Δᵢ

                for (j in participatingExtenders.indices.filterNot { it == minIndex }) {
                    val extender = participatingExtenders[j]
                    val currentDelta = extender.getDelta(prefix)

                    // WCOJ anchored step in Δᵢ
                    var tempDelta = participatingExtenders[minIndex].intersectZ1(prefix, currentDelta)
                    for (k in 0 until j) {
                        if (k == minIndex) continue  // Already handled above
                        if (tempDelta.isEmpty()) break
                        tempDelta = participatingExtenders[k].intersectZ1(prefix, tempDelta)
                    }

                    runningDelta =
                    // Δ_{1..i-1} ⋈ Δᵢ
                    runningDelta.naturalJoin(currentDelta) +
                    // + Δ_{1..i-1} ⋈ z⁻¹(i)
                    extender.intersectZ1(prefix, runningDelta) +
                    // + Δᵢ ⋈ z⁻¹(1) ⋈ z⁻¹(2) ⋈ ... ⋈ z⁻¹(i-1)
                    tempDelta
                }

                // We multiply by the weight of the variables above.
                runningDelta.multiply(weight)
            }

        }

        for (i in extenders.indices) {
            extenders[i].updateZ1()
        }

        val resultMap = mutableMapOf<ResultTuple, IntegerWeight>()
        result.forEachLeaf { resultTuple, weight -> resultMap[resultTuple] = weight }
        return ZSet.fromMap(resultMap)
    }
}