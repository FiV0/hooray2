package org.hooray.incremental.stream

/**
 * Node-kind taxonomy for the WCOJ expansion's structural DAG. These
 * map onto the operator vocabulary the spec mandates: a per-pattern
 * MapIndex into branch variable order, an Integrate for patterns
 * that participate in CURRENT state, a ZSetGenericJoin per branch,
 * and an optional MapIndex canonicalizing the result back.
 */
enum class WcojNodeKind {
    INPUT_REINDEX,       // MapIndex from pattern's natural order into branch variable order
    INTEGRATE_CURRENT,   // Accumulator for non-delta patterns (CURRENT state)
    JOIN,                // ZSetGenericJoin over the reindexed inputs
    CANONICALIZE         // MapIndex back to canonical variable order
}

/**
 * One branch in the WCOJ expansion. There is one branch per delta-term
 * pattern. `nodeIds` and `nodeKinds` line up positionally and describe
 * the structural DAG that the spec's stream model produces.
 */
data class WcojBranchExpansion(
    val deltaPatternIndex: Int,
    val variableOrder: List<Int>,
    val nodeIds: List<NodeId>,
    val nodeKinds: List<WcojNodeKind>
) {
    init {
        require(nodeIds.size == nodeKinds.size) {
            "nodeIds and nodeKinds must align (got ${nodeIds.size} vs ${nodeKinds.size})"
        }
    }
}

/** Full WCOJ expansion, one entry per delta-term branch. */
data class WcojExpansion(
    val branches: List<WcojBranchExpansion>
)

/**
 * Structural expansion of an `IncrementalWcojJoinSpec` — one branch
 * per pattern. The expansion is inspection-only in v1: the runtime
 * uses `IncrementalWcojJoinSource` directly, but analysis (and future
 * visualization) walks this DAG description.
 *
 * Per branch:
 * - For each pattern: an INPUT_REINDEX node (MapIndex into branch order).
 *   Non-delta patterns prepend an INTEGRATE_CURRENT node so they read
 *   accumulated state instead of the delta.
 * - One JOIN node consuming all reindexed inputs.
 * - One CANONICALIZE node if the branch's variable order differs from
 *   canonical; otherwise none.
 */
fun expand(spec: IncrementalWcojJoinSpec): WcojExpansion {
    var nextId = 1
    fun fresh(): NodeId = NodeId(nextId++)

    val identityOrder = (0 until spec.levels).toList()
    val branches = spec.patterns.indices.map { deltaIndex ->
        val variableOrder = variableOrderForDeltaTerm(spec.patterns[deltaIndex], spec.levels)
        val kinds = mutableListOf<WcojNodeKind>()
        val ids = mutableListOf<NodeId>()

        for (patternIndex in spec.patterns.indices) {
            if (patternIndex != deltaIndex) {
                kinds += WcojNodeKind.INTEGRATE_CURRENT
                ids += fresh()
            }
            kinds += WcojNodeKind.INPUT_REINDEX
            ids += fresh()
        }
        kinds += WcojNodeKind.JOIN
        ids += fresh()
        if (variableOrder != identityOrder) {
            kinds += WcojNodeKind.CANONICALIZE
            ids += fresh()
        }

        WcojBranchExpansion(deltaIndex, variableOrder, ids, kinds)
    }
    return WcojExpansion(branches)
}
