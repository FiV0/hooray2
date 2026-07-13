package org.hooray.engine

class OrPattern(
    override val idx: Int,
    private val branches: List<PatternBranch>,
    engine: StageEngine = StageEngine(),
) : PlanPattern, ExecPattern {
    private val branchExecutor = CachedBranchExecutor(engine)

    override val orderedVariables: List<Variable>
    override val variables: Set<Variable>

    init {
        require(branches.isNotEmpty()) { "OR patterns must have at least one branch" }
        val branchVariables = branches.map(::branchOrderedVariables)
        require(branchVariables.all { it == branchVariables.first() }) {
            "OR branches must have the same ordered variables"
        }
        orderedVariables = branchVariables.first()
        variables = orderedVariables.toSet()
    }

    override fun groundingGroups(bound: List<Variable>): List<GroundingGroup> {
        val missing = orderedVariables.filterNot { it in bound }
        if (missing.isEmpty()) return emptyList()

        val everyBranchCoversMissing = branches.all { branch ->
            groundingClosure(branch.patterns, bound).covered.containsAll(missing)
        }
        return if (everyBranchCoversMissing) listOf(GroundingGroup(missing)) else emptyList()
    }

    override fun count(
        input: BindingSet,
        introduces: List<Variable>,
        proposals: List<Proposal>,
    ): List<Proposal> {
        val missing = orderedVariables.filterNot { it in input.variables }
        if (introduces.toSet() != missing.toSet()) return proposals
        return proposals.map { proposal ->
            if (proposal.proposer == NO_PROPOSER) Proposal(idx, Int.MAX_VALUE) else proposal
        }
    }

    override fun propose(
        input: BindingSet,
        introduces: List<Variable>,
        targetVariables: List<Variable>,
    ): BindingSet {
        val missing = orderedVariables.filterNot { it in input.variables }
        require(introduces.toSet() == missing.toSet()) {
            "OR pattern can only propose all of its missing variables"
        }
        val request = StageRequest(
            seedVariables = input.variables,
            introduces = introduces,
            targetVariables = targetVariables,
            mode = StageRequestMode.PROPOSE,
        )
        var result = BindingSet(targetVariables, emptyList())
        branches.forEachIndexed { branchIndex, branch ->
            val branchResult = branchExecutor.execute(branchIndex, branch, request, input)
                .project(targetVariables)
                .distinctRows()
            result = result.unionDistinct(branchResult)
        }
        return result
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
        var supported = BindingSet(input.variables, emptyList())
        branches.forEachIndexed { branchIndex, branch ->
            val branchResult = branchExecutor.execute(branchIndex, branch, request, input)
                .project(input.variables)
                .distinctRows()
            supported = supported.unionDistinct(branchResult)
        }
        return input.semijoin(supported)
    }
}
