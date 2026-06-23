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
        val proposers = stage.proposerEligibleParticipants
        require(proposers.isNotEmpty()) {
            "Stage that introduces variables must have at least one proposer"
        }

        val shards = if (proposers.size == 1) {
            listOf(ProposerShard(proposers.first(), input))
        } else {
            partitionByCheapestProposer(input, proposers, stage.introduces)
        }

        val outputRows = mutableListOf<BindingRow>()
        for (shard in shards) {
            val proposed = shard.proposer.propose(
                input = shard.input,
                introduces = stage.introduces,
                targetVariables = stage.targetVariables,
            )
            val validators = stage.participants.filterNot { it === shard.proposer }
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

    private fun partitionByCheapestProposer(
        input: BindingSet,
        proposers: List<ExecPattern>,
        introduces: List<Any>,
    ): List<ProposerShard> {
        val countsByProposer = proposers.map { proposer ->
            val counts = proposer.count(input, introduces)
            require(counts.size == input.rowCount) {
                "Proposer count returned ${counts.size} rows, expected ${input.rowCount}"
            }
            counts
        }

        val rowsByProposer = proposers.indices.associateWith { mutableListOf<BindingRow>() }
        input.rows.forEachIndexed { rowIndex, row ->
            val chosen = proposers.indices
                .asSequence()
                .map { proposerIndex -> proposerIndex to countsByProposer[proposerIndex][rowIndex] }
                .filter { (_, count) -> count > 0 }
                .minWithOrNull(compareBy<Pair<Int, Int>> { it.second }.thenBy { it.first })

            if (chosen != null) {
                rowsByProposer.getValue(chosen.first).add(row)
            }
        }

        return proposers.indices.mapNotNull { proposerIndex ->
            val rows = rowsByProposer.getValue(proposerIndex)
            if (rows.isEmpty()) {
                null
            } else {
                ProposerShard(
                    proposer = proposers[proposerIndex],
                    input = BindingSet(input.variables, rows),
                )
            }
        }
    }

    private data class ProposerShard(
        val proposer: ExecPattern,
        val input: BindingSet,
    )
}
