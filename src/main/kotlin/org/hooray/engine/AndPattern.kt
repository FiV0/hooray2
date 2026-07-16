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
        added: List<Variable>,
        proposals: List<Proposal>,
    ): List<Proposal> {
        if (added.isEmpty() || !variables.containsAll(added)) return proposals
        val counts = input.rows.map { row ->
            val completed = engine.execute(
                branch.stages,
                BindingSet(input.variables, listOf(row)),
            )
            completed.project(added).distinctRows().rowCount
        }
        return updateProposals(idx, proposals, counts)
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
        require(added.isNotEmpty() && variables.containsAll(added)) {
            "AND pattern cannot introduce variables it does not contain"
        }
        return engine.execute(branch.stages, input)
            .project(targetVariables)
            .distinctRows()
    }

    private fun validate(
        input: BindingSet,
        added: List<Variable>,
        targetVariables: List<Variable>,
    ): BindingSet {
        require(added.isEmpty()) { "AND validation cannot add variables" }
        val completed = engine.execute(branch.stages, input)
        return input.semijoin(completed.project(input.variables).distinctRows())
    }
}
