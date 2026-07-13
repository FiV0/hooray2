package org.hooray.engine

class NotPattern(
    override val idx: Int,
    private val branch: PatternBranch,
    engine: StageEngine = StageEngine(),
) : PlanPattern, ExecPattern {
    private val branchExecutor = CachedBranchExecutor(engine)

    override val orderedVariables: List<Variable> = branchOrderedVariables(branch)
    override val variables: Set<Variable> = orderedVariables.toSet()

    override fun groundable(bound: Set<Variable>): List<Variable> = emptyList()

    override fun validate(
        input: BindingSet,
        introduces: List<Variable>,
        targetVariables: List<Variable>,
    ): BindingSet {
        val request = StageRequest(
            seedVariables = input.variables,
            introduces = introduces,
            targetVariables = input.variables,
            mode = StageRequestMode.VALIDATE,
        )
        val matches = branchExecutor.execute(0, branch, request, input)
            .project(input.variables)
            .distinctRows()
        return input.antijoin(matches)
    }
}
