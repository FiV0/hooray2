package org.hooray.dbsp

/**
 * Unit delay (z⁻¹): outputs the value it received on the previous step.
 *
 * The first step emits the empty z-set. `z1` is what lets a circuit reference a
 * stream's previous-step value; it underpins the delayed integral in the
 * composed form of the incremental join.
 */
class Z1Op(override val name: String = "z1") :
    UnaryOperator<TupleZSet, TupleZSet> {

    private var stored: TupleZSet = emptyTupleZSet()

    override fun eval(input: TupleZSet): TupleZSet {
        val previous = stored
        stored = input
        return previous
    }
}
