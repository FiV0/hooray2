package org.hooray.engine

class OrPattern @JvmOverloads constructor(
    override val idx: Int,
    override val variables: Set<Any>,
    private val proposalBranches: List<List<Stage>>,
    private val validationBranches: List<List<Stage>>,
    private val canPropose: Boolean,
    private val executor: StageExecutor = StageExecutor(),
) : ExecPattern {
    init {
        require(!canPropose || proposalBranches.isNotEmpty()) { "Proposing OR pattern must have at least one branch" }
        require(validationBranches.isNotEmpty()) { "OR pattern must have at least one branch" }
    }

    override fun count(
        input: BindingSet,
        introduces: List<Any>,
        proposals: List<Proposal>,
    ): List<Proposal> {
        val coveredVariables = input.variables.toSet() + introduces
        if (!canPropose || !coveredVariables.containsAll(variables)) {
            return proposals
        }

        return proposals.map { proposal ->
            if (proposal.idx == NO_PROPOSAL) {
                Proposal(idx, Int.MAX_VALUE)
            } else {
                proposal
            }
        }
    }

    override fun propose(
        input: BindingSet,
        introduces: List<Any>,
        targetVariables: List<Any>,
    ): BindingSet {
        require(canPropose && (input.variables.toSet() + introduces).containsAll(variables)) {
            "OR pattern can only propose when input variables and introduced variables cover all OR variables"
        }

        val rows = mutableListOf<BindingRow>()
        for (branch in proposalBranches) {
            rows.addAll(runBranch(input, branch).reorder(targetVariables).rows)
        }
        return BindingSet(targetVariables, rows).distinctRows()
    }

    override fun validate(input: BindingSet): BindingSet {
        val rows = input.rows.filter { row ->
            val seededRow = BindingSet(input.variables, listOf(row))
            validationBranches.any { branch ->
                runBranch(seededRow, branch).rows.isNotEmpty()
            }
        }
        return BindingSet(input.variables, rows)
    }

    private fun runBranch(seed: BindingSet, branch: List<Stage>): BindingSet {
        return branch.fold(seed) { bindings, stage ->
            executor.execute(stage, bindings)
        }
    }
}
