package org.hooray.engine

class NotPattern(
    override val idx: Int,
    private val branch: PatternBranch,
    private val engine: GenericJoinEngine = GenericJoinEngine(),
) : PlanPattern, ExecPattern {
    override val orderedVariables: List<Variable> = branchOrderedVariables(branch)
    override val variables: Set<Variable> = orderedVariables.toSet()

    override fun groundable(bound: Set<Variable>): List<Variable> = emptyList()

    override fun validate(
        input: BindingSet,
        introduces: List<Variable>,
        targetVariables: List<Variable>,
    ): BindingSet {
        val matches = engine.execute(branch.validationStages, input)
            .project(input.variables)
            .distinctRows()
        return input.antijoin(matches)
    }
}
