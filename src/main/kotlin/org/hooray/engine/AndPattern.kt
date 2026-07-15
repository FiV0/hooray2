package org.hooray.engine

class AndPattern(
    override val idx: Int,
    private val branch: PatternBranch,
    private val engine: GenericJoinEngine = GenericJoinEngine(),
) : PlanPattern, ExecPattern {
    override val orderedVariables: List<Variable> = branchOrderedVariables(branch)
    override val variables: Set<Variable> = orderedVariables.toSet()

    override fun groundable(bound: Set<Variable>): List<Variable> {
        return groundingClosure(branch.patterns, bound).groundable
    }

    override fun count(
        input: BindingSet,
        introduces: List<Variable>,
        proposals: List<Proposal>,
    ): List<Proposal> {
        if (introduces.isEmpty() || !variables.containsAll(introduces)) return proposals
        val counts = input.rows.map { row ->
            val completed = engine.execute(
                branch.proposalStages,
                BindingSet(input.variables, listOf(row)),
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
        return engine.execute(branch.proposalStages, input)
            .project(targetVariables)
            .distinctRows()
    }

    override fun validate(
        input: BindingSet,
        introduces: List<Variable>,
        targetVariables: List<Variable>,
    ): BindingSet {
        val completed = engine.execute(branch.validationStages, input)
        return input.semijoin(completed.project(input.variables).distinctRows())
    }
}
