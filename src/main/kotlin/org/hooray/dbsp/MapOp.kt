package org.hooray.dbsp

import org.hooray.incremental.IntegerWeight
import org.hooray.incremental.ZSet
import java.util.function.Function

/**
 * Applies [transform] to every tuple key. When two input tuples map to the same
 * output tuple their weights are summed, and entries that cancel to zero are
 * dropped — preserving bag semantics.
 *
 * Like [FilterOp], `map` is *linear*, so the operator applied to a delta is its
 * own incremental form. Used for `:find` projection and for re-permuting an
 * intermediate join result so the next join key lands in leading columns.
 */
class MapOp(
    override val name: String = "map",
    private val transform: (Tuple) -> Tuple,
) : UnaryOperator<TupleZSet, TupleZSet> {

    override fun eval(input: TupleZSet): TupleZSet {
        val result = HashMap<Tuple, IntegerWeight>()
        for (entry in input.entries()) {
            val mapped = transform(entry.key)
            val combined = result[mapped]?.add(entry.value) ?: entry.value
            if (combined.isZero()) result.remove(mapped) else result[mapped] = combined
        }
        return ZSet.fromMap(result)
    }

    companion object {
        /**
         * A [MapOp] that reorders/projects columns: output column `i` is input
         * column `order[i]`. Built from a plain `IntArray` for easy Clojure interop.
         */
        @JvmStatic
        fun project(order: IntArray): MapOp = MapOp("project") { it.project(order) }

        /**
         * Builds a tuple map from a Java function for straightforward
         * Clojure interop.
         */
        @JvmStatic
        fun fromFunction(name: String, transform: Function<Tuple, Tuple>): MapOp {
            return MapOp(name) { tuple -> transform.apply(tuple) }
        }
    }
}
