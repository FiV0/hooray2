package org.hooray.dbsp

/**
 * Adds two z-set streams pointwise: `eval(a, b) = a + b`.
 *
 * Z-sets form an abelian group under addition, so `plus` is linear in both
 * arguments — it is its own incremental form.
 */
class PlusOp(override val name: String = "plus") :
    BinaryOperator<TupleZSet, TupleZSet, TupleZSet> {

    override fun eval(left: TupleZSet, right: TupleZSet): TupleZSet = left.add(right)
}

/** Subtracts two z-set streams pointwise: `eval(a, b) = a - b`. */
class MinusOp(override val name: String = "minus") :
    BinaryOperator<TupleZSet, TupleZSet, TupleZSet> {

    override fun eval(left: TupleZSet, right: TupleZSet): TupleZSet = left.subtract(right)
}

/** Difference over z-set streams: `eval(a, b) = a - b`. */
class DifferenceOp(override val name: String = "difference") :
    BinaryOperator<TupleZSet, TupleZSet, TupleZSet> {

    override fun eval(left: TupleZSet, right: TupleZSet): TupleZSet = left.subtract(right)
}
