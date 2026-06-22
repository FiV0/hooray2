package org.hooray.engine

class OrPattern @JvmOverloads constructor(
    override val variables: Set<Any>,
    private val branches: List<List<Stage>>,
    override val proposerEligible: Boolean,
    private val executor: StageExecutor = StageExecutor(),
) : ExecPattern {
    init {
        require(branches.isNotEmpty()) { "OR pattern must have at least one branch" }
    }

    override fun count(input: BindingSet, introduces: List<Any>): List<Int> {
        if (!proposerEligible) {
            return super.count(input, introduces)
        }

        return input.rows.map { row ->
            propose(
                input = BindingSet(input.variables, listOf(row)),
                introduces = introduces,
                targetVariables = input.variables + introduces,
            ).rowCount
        }
    }

    override fun propose(
        input: BindingSet,
        introduces: List<Any>,
        targetVariables: List<Any>,
    ): BindingSet {
        require(proposerEligible) { "OR pattern is not proposer-eligible in this stage" }

        val rows = mutableListOf<BindingRow>()
        for (branch in branches) {
            rows.addAll(runBranch(input, branch).reorder(targetVariables).rows)
        }
        return BindingSet(targetVariables, rows).distinctRows()
    }

    override fun validate(input: BindingSet, targetVariables: List<Any>): BindingSet {
        val rows = input.rows.filter { row ->
            val seededRow = BindingSet(input.variables, listOf(row))
            branches.any { branch ->
                runBranch(seededRow, branch).rows.isNotEmpty()
            }
        }
        return BindingSet(input.variables, rows).reorder(targetVariables)
    }

    private fun runBranch(seed: BindingSet, branch: List<Stage>): BindingSet {
        return branch.fold(seed) { bindings, stage ->
            executor.execute(stage, bindings)
        }
    }
}
