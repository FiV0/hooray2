package org.hooray.engine

class StageEngine {
    fun execute(stages: List<Stage>, input: BindingSet): BindingSet {
        return stages.fold(input) { bindings, stage -> executeStage(stage, bindings) }
    }

    private fun executeStage(stage: Stage, input: BindingSet): BindingSet {
        require(input.variables.intersect(stage.introduces.toSet()).isEmpty()) {
            "Stage introduced variables must not already be bound"
        }
        require(stage.targetVariables.toSet() == (input.variables + stage.introduces).toSet()) {
            "Stage target variables must equal input variables plus introduced variables"
        }

        return if (stage.introduces.isEmpty()) {
            validateAll(input, stage, stage.participants).reorder(stage.targetVariables)
        } else {
            executeProposingStage(stage, input)
        }
    }

    private fun executeProposingStage(stage: Stage, input: BindingSet): BindingSet {
        val participantIds = stage.participants.mapTo(hashSetOf()) { it.idx }
        val initial = List(input.rowCount) { Proposal(NO_PROPOSER, Int.MAX_VALUE) }
        val proposals = stage.participants.fold(initial) { current, participant ->
            val updated = participant.count(input, stage.introduces, current)
            require(updated.size == input.rowCount) {
                "Pattern ${participant.idx} returned ${updated.size} proposals, expected ${input.rowCount}"
            }
            updated
        }
        proposals.forEach { proposal ->
            require(proposal.proposer == NO_PROPOSER || proposal.proposer in participantIds) {
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
            val proposed = proposer.propose(
                input = input.selectRows(rowIndexes),
                introduces = stage.introduces,
                targetVariables = stage.targetVariables,
            )
            require(proposed.variables == stage.targetVariables) {
                "Pattern $proposerId proposed layout ${proposed.variables}, expected ${stage.targetVariables}"
            }

            val validators = stage.participants.filterNot { it.idx == proposerId }
            val validated = validateAll(proposed, stage, validators)
            result = result.unionDistinct(validated)
        }
        return result
    }

    private fun validateAll(
        input: BindingSet,
        stage: Stage,
        validators: List<ExecPattern>,
    ): BindingSet {
        return validators.fold(input) { bindings, validator ->
            val validated = validator.validate(
                input = bindings,
                introduces = stage.introduces,
                targetVariables = stage.targetVariables,
            )
            require(validated.variables == bindings.variables) {
                "Pattern ${validator.idx} changed the layout during validation"
            }
            validated
        }
    }
}
