package org.hooray.engine

data class Proposal(
    val proposer: Int,
    val count: Int,
)

internal const val NO_PROPOSER = -1

interface ExecPattern {
    val idx: Int
    val variables: Set<Variable>

    /** Updates per-row proposals only when this pattern has a strictly cheaper positive count. */
    fun count(
        input: BindingSet,
        added: List<Variable>,
        proposals: List<Proposal>,
    ): List<Proposal> = proposals

    /** Extends the input when [added] is non-empty; otherwise filters it without changing its layout. */
    fun join(
        input: BindingSet,
        added: List<Variable>,
        targetVariables: List<Variable>,
    ): BindingSet
}

internal fun updateProposals(
    idx: Int,
    proposals: List<Proposal>,
    counts: List<Int>,
): List<Proposal> {
    require(counts.size == proposals.size) {
        "Pattern returned ${counts.size} counts, expected ${proposals.size}"
    }
    return proposals.mapIndexed { rowIndex, proposal ->
        val count = counts[rowIndex]
        if (count > 0 && count < proposal.count) Proposal(idx, count) else proposal
    }
}
