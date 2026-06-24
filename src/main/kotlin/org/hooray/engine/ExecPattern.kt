package org.hooray.engine

data class Proposal(
    val idx: Int,
    val count: Int,
)

internal const val NO_PROPOSAL = -1

interface ExecPattern {
    val idx: Int
    val variables: Set<Any>

    fun count(
        input: BindingSet,
        introduces: List<Any>,
        proposals: List<Proposal>,
    ): List<Proposal> = proposals

    fun propose(
        input: BindingSet,
        introduces: List<Any>,
        targetVariables: List<Any>,
    ): BindingSet {
        throw UnsupportedOperationException("Pattern cannot propose for this stage")
    }

    fun validate(input: BindingSet): BindingSet
}

internal fun updateProposals(
    idx: Int,
    proposals: List<Proposal>,
    counts: List<Int>,
): List<Proposal> {
    require(proposals.size == counts.size) {
        "Pattern count returned ${counts.size} rows, expected ${proposals.size}"
    }
    return proposals.mapIndexed { rowIndex, proposal ->
        val count = counts[rowIndex]
        if (count > 0 && count < proposal.count) {
            Proposal(idx, count)
        } else {
            proposal
        }
    }
}
