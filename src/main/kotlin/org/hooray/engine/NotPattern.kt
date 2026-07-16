package org.hooray.engine

class NotPattern(
    override val idx: Int,
    private val branch: PatternBranch,
    private val engine: GenericJoinEngine = GenericJoinEngine(),
) : PlanPattern, ExecPattern {
    override val orderedVariables: List<Variable> = branchOrderedVariables(branch)
    override val variables: Set<Variable> = orderedVariables.toSet()

    override fun groundable(bound: Set<Variable>): List<Variable> = emptyList()

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
        throw UnsupportedOperationException("Pattern cannot propose for this stage")
    }

    private fun validate(
        input: BindingSet,
        added: List<Variable>,
        targetVariables: List<Variable>,
    ): BindingSet {
        require(added.isEmpty()) { "NOT validation cannot add variables" }
        val matches = engine.execute(branch.stages, input)
            .project(input.variables)
            .distinctRows()
        return input.antijoin(matches)
    }
}
