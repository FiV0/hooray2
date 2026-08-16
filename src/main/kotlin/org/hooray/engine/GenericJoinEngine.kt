package org.hooray.engine

// generic-join based on
// http://www.frankmcsherry.org/dataflow/relational/join/2015/04/11/genericjoin.html
// https://arxiv.org/abs/1310.3314
//
// The ExecPattern interface very much inspired by
// https://github.com/frankmcsherry/datatoad

class GenericJoinEngine {

    private fun validateAll(
        input: BindingSet,
        stage: IStage,
        validators: List<ExecPattern>,
    ): BindingSet {
        return validators.fold(input) { bindings, validator ->
            val validated = validator.join(
                input = bindings,
                added = emptyList(),
                targetVariables = stage.targetVariables,
            )
            check(validated.variables == bindings.variables) {
                "Pattern ${validator.idx} changed the layout during validation"
            }
            validated
        }
    }

    private fun executeProposingStage(stage: IStage, input: BindingSet): BindingSet {
        val proposers = stage.proposers

        // A sole proposer does not need count arbitration, even when other participants validate its output.
        if (proposers.size == 1) {
            val proposer = proposers.first()
            val proposed = proposer.join(
                input = input,
                added = stage.added,
                targetVariables = stage.targetVariables,
            )
            check(proposed.variables == stage.targetVariables) {
                "Pattern ${proposer.idx} proposed layout ${proposed.variables}, expected ${stage.targetVariables}"
            }
            return validateAll(
                proposed,
                stage,
                stage.participants.filterNot { it.idx == proposer.idx },
            )
        }

        val proposerIds = proposers.mapTo(hashSetOf()) { it.idx }
        val initial = List(input.rowCount) { Proposal(NO_PROPOSER, Int.MAX_VALUE) }

        // Count the number of proposals for each row from each designated proposer.
        val proposals = proposers.fold(initial) { current, proposer ->
            val updated = proposer.count(input, stage.added, current)
            check(updated.size == input.rowCount) {
                "Pattern ${proposer.idx} returned ${updated.size} proposals, expected ${input.rowCount}"
            }
            updated
        }

        proposals.forEach { proposal ->
            check(proposal.proposer == NO_PROPOSER || proposal.proposer in proposerIds) {
                "Unknown proposer index ${proposal.proposer}"
            }
        }

        val shards = linkedMapOf<Int, MutableList<Int>>()
        proposals.forEachIndexed { rowIndex, proposal ->
            if (proposal.proposer != NO_PROPOSER && proposal.count > 0) {
                shards.getOrPut(proposal.proposer, ::mutableListOf).add(rowIndex)
            }
        }

        val participantsById = stage.participants.associateBy { it.idx }
        var result = BindingSet(stage.targetVariables, emptyList())
        for ((proposerId, rowIndexes) in shards) {
            val proposer = participantsById.getValue(proposerId)
            val proposed = proposer.join(
                input = input.selectRows(rowIndexes),
                added = stage.added,
                targetVariables = stage.targetVariables,
            )
            check(proposed.variables == stage.targetVariables) {
                "Pattern $proposerId proposed layout ${proposed.variables}, expected ${stage.targetVariables}"
            }

            val validators = stage.participants.filterNot { it.idx == proposerId }
            val validated = validateAll(proposed, stage, validators)
            result = result.union(validated)
        }
        return result
    }

    private fun executeStage(stage: IStage, input: BindingSet): BindingSet {
        require(input.variables.intersect(stage.added.toSet()).isEmpty()) {
            "Stage added variables must not already be bound"
        }
        require(stage.targetVariables.toSet() == (input.variables + stage.added).toSet()) {
            "Stage target variables must equal input variables plus added variables"
        }

        return if (stage.added.isEmpty()) {
            validateAll(input, stage, stage.participants).reorder(stage.targetVariables)
        } else {
            executeProposingStage(stage, input)
        }
    }

    fun execute(stages: List<IStage>, input: BindingSet): BindingSet {
        return stages.fold(input) { bindings, stage -> executeStage(stage, bindings) }
    }
}
