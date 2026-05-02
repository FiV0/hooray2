package org.hooray.incremental

/**
 * Source operator for the corrected incremental GenericJoin computation.
 *
 * This class implements the eval/commit pattern:
 * - eval(): Distributes deltas and computes the join result
 * - commit(): Advances the state of all relations
 */
class IncrementalJoinOperator(
    relations: List<IncrementalIndex>,
    levels: Int
) : SourceOperator {
    override val name: String = "IncrementalJoin"
    private val engine = IncrementalGenericJoinEngine(relations, levels)

    override fun eval(input: ZSetIndices): ResultZSet = engine.eval(input)

    override fun commit() = engine.commit()
}
