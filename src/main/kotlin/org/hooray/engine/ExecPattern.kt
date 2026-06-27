package org.hooray.engine

import clojure.lang.Symbol

data class Proposal(
    val idx: Int,
    val count: Int,
)

internal const val NO_PROPOSAL = -1

typealias Variable = Symbol

interface ExecPattern {
    val idx: Int
    val variables: Set<Variable>

    fun count(
        input: BindingSet,
        introduces: List<Variable>,
        proposals: List<Proposal>,
    ): List<Proposal> = proposals

    fun propose(
        input: BindingSet,
        introduces: List<Variable>,
        targetVariables: List<Variable>,
    ): BindingSet {
        throw UnsupportedOperationException("Pattern cannot propose for this stage")
    }

    fun validate(
        input: BindingSet,
        introduces: List<Variable>,
        targetVariables: List<Variable>,
    ): BindingSet
}

internal fun updateProposals( idx: Int, proposals: List<Proposal>, counts: List<Int>): List<Proposal> =
    proposals.mapIndexed { rowIndex, proposal ->
        val count = counts[rowIndex]
        if (count > 0 && count < proposal.count) {
            Proposal(idx, count)
        } else {
            proposal
        }
    }