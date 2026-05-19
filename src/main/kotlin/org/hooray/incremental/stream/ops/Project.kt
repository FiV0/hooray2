package org.hooray.incremental.stream.ops

import clojure.lang.PersistentVector
import org.hooray.algo.ResultTuple
import org.hooray.incremental.IntegerWeight
import org.hooray.incremental.ResultZSet
import org.hooray.incremental.ZSet
import org.hooray.incremental.stream.ProjectSpec

fun project(input: ResultZSet, spec: ProjectSpec): ResultZSet {
    val result = mutableMapOf<ResultTuple, IntegerWeight>()
    for ((tuple, weight) in input.entries()) {
        @Suppress("UNCHECKED_CAST")
        val projected = PersistentVector.create(
            spec.outputLevels.map { level -> tuple[level] }
        ) as ResultTuple
        result.merge(projected, weight) { left, right ->
            val sum = left.add(right)
            if (sum.isZero()) null else sum
        }
    }
    return ZSet.fromMap(result)
}
