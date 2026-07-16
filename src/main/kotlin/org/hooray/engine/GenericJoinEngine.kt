package org.hooray.engine

class GenericJoinEngine {

    private fun validateAll(
        input: BindingSet,
        stage: Stage,
        validators: List<ExecPattern>,
    ): BindingSet {
        return validators.fold(input) { bindings, validator ->
            val validated = validator.join(
                input = bindings,
                added = emptyList(),
                targetVariables = stage.targetVariables,
            )
            assert(validated.variables == bindings.variables) {
                "Pattern ${validator.idx} changed the layout during validation"
            }
            validated
        }
    }

    private fun executeProposingStage(stage: Stage, input: BindingSet): BindingSet {
        // If the stage has a single participant, we can skip the count, propose and validation.
        if (stage.participants.size == 1) {
            val proposer = stage.participants.first()
            val proposed = proposer.join(
                input = input,
                added = stage.added,
                targetVariables = stage.targetVariables,
            )
            assert(proposed.variables == stage.targetVariables) {
                "Pattern ${proposer.idx} proposed layout ${proposed.variables}, expected ${stage.targetVariables}"
            }
            return proposed
        }

        val participantIds = stage.participants.mapTo(hashSetOf()) { it.idx }
        val initial = List(input.rowCount) { Proposal(NO_PROPOSER, Int.MAX_VALUE) }

        // Count the number of proposals for each row from each participant
        val proposals = stage.participants.fold(initial) { current, participant ->
            val updated = participant.count(input, stage.added, current)
            assert(updated.size == input.rowCount) {
                "Pattern ${participant.idx} returned ${updated.size} proposals, expected ${input.rowCount}"
            }
            updated
        }

        proposals.forEach { proposal ->
            assert(proposal.proposer == NO_PROPOSER || proposal.proposer in participantIds) {
                "Unknown proposer index ${proposal.proposer}"
            }
        }

        val rowIndexesByProposer = linkedMapOf<Int, MutableList<Int>>()
        proposals.forEachIndexed { rowIndex, proposal ->
            if (proposal.proposer != NO_PROPOSER && proposal.count > 0) {
                rowIndexesByProposer.getOrPut(proposal.proposer, ::mutableListOf).add(rowIndex)
            }
        }

        val participantsById = stage.participants.associateBy { it.idx }
        val shards = rowIndexesByProposer.entries.sortedBy { (_, rowIndexes) -> rowIndexes.first() }
        var result = BindingSet(stage.targetVariables, emptyList())
        for ((proposerId, rowIndexes) in shards) {
            val proposer = participantsById.getValue(proposerId)
            val proposed = proposer.join(
                input = input.selectRows(rowIndexes),
                added = stage.added,
                targetVariables = stage.targetVariables,
            )
            assert(proposed.variables == stage.targetVariables) {
                "Pattern $proposerId proposed layout ${proposed.variables}, expected ${stage.targetVariables}"
            }

            val validators = stage.participants.filterNot { it.idx == proposerId }
            val validated = validateAll(proposed, stage, validators)
            result = result.union(validated)
        }
        return result
    }

    private fun executeStage(stage: Stage, input: BindingSet): BindingSet {
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

    fun execute(stages: List<Stage>, input: BindingSet): BindingSet {
        return stages.fold(input) { bindings, stage -> executeStage(stage, bindings) }
    }
}
