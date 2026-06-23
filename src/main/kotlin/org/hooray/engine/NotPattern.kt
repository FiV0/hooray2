package org.hooray.engine

class NotPattern @JvmOverloads constructor(
    override val variables: Set<Any>,
    private val branch: List<Stage>,
    private val executor: StageExecutor = StageExecutor(),
) : ExecPattern {
    override val proposerEligible: Boolean = false

    override fun validate(input: BindingSet): BindingSet {
        val rows = input.rows.filter { row ->
            val seededRow = BindingSet(input.variables, listOf(row))
            runBranch(seededRow).rows.isEmpty()
        }
        return BindingSet(input.variables, rows)
    }

    private fun runBranch(seed: BindingSet): BindingSet {
        return branch.fold(seed) { bindings, stage ->
            executor.execute(stage, bindings)
        }
    }
}
