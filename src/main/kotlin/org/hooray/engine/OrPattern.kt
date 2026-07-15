package org.hooray.engine

class OrPattern(
    override val idx: Int,
    private val branches: List<PatternBranch>,
    private val engine: GenericJoinEngine = GenericJoinEngine(),
) : PlanPattern, ExecPattern {
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

    override fun groundable(bound: Set<Variable>): List<Variable> {
        val missing = orderedVariables.filterNot { it in bound }
        if (missing.isEmpty()) return emptyList()

        val everyBranchCoversMissing = branches.all { branch ->
            groundingClosure(branch.patterns, bound).covered.containsAll(missing)
        }
        return if (everyBranchCoversMissing) missing else emptyList()
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
        var result = BindingSet(targetVariables, emptyList())
        branches.forEach { branch ->
            val branchResult = engine.execute(branch.proposalStages, input)
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
        var supported = BindingSet(input.variables, emptyList())
        branches.forEach { branch ->
            val branchResult = engine.execute(branch.validationStages, input)
                .project(input.variables)
                .distinctRows()
            supported = supported.unionDistinct(branchResult)
        }
        return input.semijoin(supported)
    }
}
