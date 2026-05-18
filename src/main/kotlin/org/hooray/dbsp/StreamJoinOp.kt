package org.hooray.dbsp

import org.hooray.incremental.IntegerWeight
import org.hooray.incremental.ZSet

/**
 * Non-incremental equi-join of two z-sets on their leading [keyArity] columns.
 *
 * Two tuples match when their length-[keyArity] prefixes are equal. The result
 * tuple is the left tuple followed by the right tuple's non-key columns (the
 * shared join key appears once); weights are multiplied — multiset (bag) join.
 * `keyArity == 0` makes every pair match, yielding the Cartesian product.
 *
 * This joins whole z-sets, not deltas. It is the building block for the
 * composed reference join used to test [IncrementalJoinOp]; the standard
 * circuit itself uses the incremental operator.
 */
class StreamJoinOp(
    private val keyArity: Int,
    override val name: String = "stream-join",
) : BinaryOperator<TupleZSet, TupleZSet, TupleZSet> {

    override fun eval(left: TupleZSet, right: TupleZSet): TupleZSet {
        val rightByKey = HashMap<Tuple, MutableList<Map.Entry<Tuple, IntegerWeight>>>()
        for (entry in right.entries()) {
            rightByKey.getOrPut(entry.key.prefix(keyArity)) { mutableListOf() }.add(entry)
        }

        val result = HashMap<Tuple, IntegerWeight>()
        for (leftEntry in left.entries()) {
            val matches = rightByKey[leftEntry.key.prefix(keyArity)] ?: continue
            for (rightEntry in matches) {
                val joined = leftEntry.key.concat(rightEntry.key.drop(keyArity))
                val weight = leftEntry.value.multiply(rightEntry.value)
                val combined = result[joined]?.add(weight) ?: weight
                if (combined.isZero()) result.remove(joined) else result[joined] = combined
            }
        }
        return ZSet.fromMap(result)
    }
}
