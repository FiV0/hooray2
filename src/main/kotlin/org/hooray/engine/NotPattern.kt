package org.hooray.engine

class NotPattern(
    override val idx: Int,
    private val stages: List<Stage>,
) : PlanPattern, ExecPattern {
    private val engine: GenericJoinEngine = GenericJoinEngine()

    override val orderedVariables: List<Variable> = stages
        .flatMap { stage -> stage.added }

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
        throw UnsupportedOperationException("NotPattern cannot propose for this stage")
    }

    private fun validate(
        input: BindingSet,
        added: List<Variable>,
        targetVariables: List<Variable>,
    ): BindingSet {
        require(added.isEmpty()) { "NOT validation cannot add variables" }
        require (input.variables.toSet() == variables) { "Input variables must all be bound for a NotPattern" }
        val inputRelation = RelationPattern(idx, input)
        val unit = BindingSet(emptyList(), listOf(emptyList()))
        val matches = engine.execute(stages.map {
            Stage(it.added, it.participants + inputRelation, it.targetVariables)
        }, unit)
            .project(input.variables)
        return input.antijoin(matches)
    }
}
