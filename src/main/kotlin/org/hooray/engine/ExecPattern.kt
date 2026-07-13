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
        introduces: List<Variable>,
        proposals: List<Proposal>,
    ): List<Proposal> = proposals

    /** Extends the input and returns exactly [targetVariables] in that order. */
    fun propose(
        input: BindingSet,
        introduces: List<Variable>,
        targetVariables: List<Variable>,
    ): BindingSet {
        throw UnsupportedOperationException("Pattern cannot propose for this stage")
    }

    /** Filters rows without changing the input layout. */
    fun validate(
        input: BindingSet,
        introduces: List<Variable>,
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
