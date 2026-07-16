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
        added: List<Variable>,
        proposals: List<Proposal>,
    ): List<Proposal> {
        val missing = orderedVariables.filterNot { it in input.variables }
        if (added.toSet() != missing.toSet()) return proposals
        return proposals.map { proposal ->
            if (proposal.proposer == NO_PROPOSER) Proposal(idx, Int.MAX_VALUE) else proposal
        }
    }

    override fun join(
        input: BindingSet,
        added: List<Variable>,
        targetVariables: List<Variable>,
    ): BindingSet = if (added.isEmpty()) {
        validate(input, added, targetVariables)
    } else {
        propose(input, added, targetVariables)
    }

    private fun propose(
        input: BindingSet,
        added: List<Variable>,
        targetVariables: List<Variable>,
    ): BindingSet {
        val missing = orderedVariables.filterNot { it in input.variables }
        require(added.toSet() == missing.toSet()) {
            "OR pattern can only propose all of its missing variables"
        }
        var result = BindingSet(targetVariables, emptyList())
        branches.forEach { branch ->
            val branchResult = engine.execute(branch.stages, input)
                .project(targetVariables)
                .distinctRows()
            result = result.unionDistinct(branchResult)
        }
        return result
    }

    private fun validate(
        input: BindingSet,
        added: List<Variable>,
        targetVariables: List<Variable>,
    ): BindingSet {
        require(added.isEmpty()) { "OR validation cannot add variables" }
        var supported = BindingSet(input.variables, emptyList())
        branches.forEach { branch ->
            val branchResult = engine.execute(branch.stages, input)
                .project(input.variables)
                .distinctRows()
            supported = supported.unionDistinct(branchResult)
        }
        return input.semijoin(supported)
    }
}
