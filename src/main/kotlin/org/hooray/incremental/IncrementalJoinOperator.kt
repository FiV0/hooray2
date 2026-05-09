package org.hooray.incremental

/**
 * Source operator for the telescoping incremental WCOJ computation.
 *
 * This class implements the eval/commit pattern:
 * - eval(): Distributes deltas and computes the join result
 * - commit(): Advances the state of all relations
 */
class IncrementalJoinOperator(
    patterns: List<CompiledTriplePattern>,
    levels: Int
) : SourceOperator {
    override val name: String = "IncrementalJoin"
    private val engine = IncrementalWcojJoinEngine(patterns, levels)

    override fun eval(input: ZSetIndices): ResultZSet = engine.eval(input)

    override fun commit() = engine.commit()
}
