package org.hooray.dbsp

import org.hooray.incremental.IntegerWeight
import org.hooray.incremental.ZSet

/**
 * Keeps only the tuples whose key satisfies [predicate]; weights are unchanged.
 *
 * Filtering is a *linear* operation — it distributes over z-set addition — so
 * the operator applied to a delta IS its own incremental form. Used to enforce
 * a triple pattern's constant `e`/`v` columns.
 */
class FilterOp(
    override val name: String = "filter",
    private val predicate: (Tuple) -> Boolean,
) : UnaryOperator<TupleZSet, TupleZSet> {

    override fun eval(input: TupleZSet): TupleZSet {
        val result = HashMap<Tuple, IntegerWeight>()
        for (entry in input.entries()) {
            if (predicate(entry.key)) {
                result[entry.key] = entry.value
            }
        }
        return ZSet.fromMap(result)
    }
}
