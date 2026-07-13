package org.hooray.engine

class AndPattern(
    override val idx: Int,
    private val branch: PatternBranch,
    engine: StageEngine = StageEngine(),
) : PlanPattern, ExecPattern {
    private val branchExecutor = CachedBranchExecutor(engine)

    override val orderedVariables: List<Variable> = branchOrderedVariables(branch)
    override val variables: Set<Variable> = orderedVariables.toSet()

    override fun groundingGroups(bound: List<Variable>): List<GroundingGroup> {
        return groundingClosure(branch.patterns, bound).groups
    }

    override fun count(
        input: BindingSet,
        introduces: List<Variable>,
        proposals: List<Proposal>,
    ): List<Proposal> {
        if (introduces.isEmpty() || !variables.containsAll(introduces)) return proposals
        val targetVariables = input.variables + introduces
        val request = StageRequest(
            seedVariables = input.variables,
            introduces = introduces,
            targetVariables = targetVariables,
            mode = StageRequestMode.PROPOSE,
        )
        val counts = input.rows.map { row ->
            val completed = branchExecutor.execute(
                branchIndex = 0,
                branch = branch,
                request = request,
                input = BindingSet(input.variables, listOf(row)),
            )
            completed.project(introduces).distinctRows().rowCount
        }
        return updateProposals(idx, proposals, counts)
    }

    override fun propose(
        input: BindingSet,
        introduces: List<Variable>,
        targetVariables: List<Variable>,
    ): BindingSet {
        require(introduces.isNotEmpty() && variables.containsAll(introduces)) {
            "AND pattern cannot introduce variables it does not contain"
        }
        val request = StageRequest(
            seedVariables = input.variables,
            introduces = introduces,
            targetVariables = targetVariables,
            mode = StageRequestMode.PROPOSE,
        )
        return branchExecutor.execute(0, branch, request, input)
            .project(targetVariables)
            .distinctRows()
    }

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
        val completed = branchExecutor.execute(0, branch, request, input)
        return input.semijoin(completed.project(input.variables).distinctRows())
    }
}
