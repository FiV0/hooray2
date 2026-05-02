package org.hooray.incremental

/**
 * Wrapper that adapts IncrementalGenericJoin-style join computation to the SourceOperator interface.
 *
 * This class implements the eval/commit pattern:
 * - eval(): Distributes deltas and computes the join result
 * - commit(): Advances the state of all relations
 *
 * The computation logic mirrors IncrementalGenericJoin but separates
 * the state update from the computation.
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
