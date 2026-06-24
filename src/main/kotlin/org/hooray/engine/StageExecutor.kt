package org.hooray.engine

class StageExecutor {
    fun execute(stage: Stage, input: BindingSet): BindingSet {
        return if (!stage.introducesVariables) {
            validateAll(input, stage.participants, stage.targetVariables)
        } else {
            executeProposingStage(stage, input)
        }
    }

    private fun executeProposingStage(stage: Stage, input: BindingSet): BindingSet {
        val participantsByIndex = stage.participants.associateBy { it.idx }
        val shards = partitionByBestProposal(input, stage.participants, stage.introduces)

        val outputRows = mutableListOf<BindingRow>()
        for (shard in shards) {
            val proposer = participantsByIndex.getValue(shard.proposerIndex)
            val proposed = proposer.propose(
                input = shard.input,
                introduces = stage.introduces,
                targetVariables = stage.targetVariables,
            )
            val validators = stage.participants.filterNot { it.idx == shard.proposerIndex }
            val validated = validateAll(proposed, validators, stage.targetVariables)
            outputRows.addAll(validated.rows)
        }

        return BindingSet(stage.targetVariables, outputRows).distinctRows()
    }

    private fun validateAll(
        input: BindingSet,
        validators: List<ExecPattern>,
        targetVariables: List<Any>,
    ): BindingSet {
        var current = input
        for (validator in validators) {
            current = validator.validate(current)
        }
        return current.reorder(targetVariables)
    }

    private fun partitionByBestProposal(
        input: BindingSet,
        participants: List<ExecPattern>,
        introduces: List<Any>,
    ): List<ProposerShard> {
        val initialProposals = List(input.rowCount) { Proposal(NO_PROPOSAL, Int.MAX_VALUE) }
        val proposals = participants.fold(initialProposals) { current, participant ->
            val updated = participant.count(input, introduces, current)
            require(updated.size == input.rowCount) {
                "Pattern proposal count returned ${updated.size} rows, expected ${input.rowCount}"
            }
            updated
        }

        val rowsByProposer = participants.associate { participant ->
            participant.idx to mutableListOf<IndexedBindingRow>()
        }
        input.rows.forEachIndexed { rowIndex, row ->
            val proposal = proposals[rowIndex]
            if (proposal.idx != NO_PROPOSAL && proposal.count > 0) {
                rowsByProposer.getValue(proposal.idx).add(IndexedBindingRow(rowIndex, row))
            }
        }

        return participants.mapNotNull { participant ->
            val indexedRows = rowsByProposer.getValue(participant.idx)
            val rows = indexedRows.map { it.row }
            if (rows.isEmpty()) {
                null
            } else {
                ProposerShard(
                    proposerIndex = participant.idx,
                    firstInputRowIndex = indexedRows.minOf { it.inputRowIndex },
                    input = BindingSet(input.variables, rows),
                )
            }
        }.sortedBy { it.firstInputRowIndex }
    }

    private data class IndexedBindingRow(
        val inputRowIndex: Int,
        val row: BindingRow,
    )

    private data class ProposerShard(
        val proposerIndex: Int,
        val firstInputRowIndex: Int,
        val input: BindingSet,
    )
}
