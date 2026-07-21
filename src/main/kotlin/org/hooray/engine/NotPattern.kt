package org.hooray.engine

class NotPattern(
    override val idx: Int,
    private val stages: List<IStage>,
) : ExecPattern {
    private val engine: GenericJoinEngine = GenericJoinEngine()

    private val orderedVariables: List<Variable> = stages
        .flatMap { stage -> stage.added }

    override val variables: Set<Variable> = orderedVariables.toSet()

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
        require(input.variables.containsAll(variables)) {
            "Input must bind all NotPattern variables"
        }
        val inputRelation = RelationPattern(idx, input.project(orderedVariables))
        val unit = BindingSet(emptyList(), listOf(emptyList()))
        val matches = engine.execute(stages.map { stage ->
            Stage(
                added = stage.added,
                participants = stage.participants + inputRelation,
                targetVariables = stage.targetVariables,
            )
        }, unit)
            .project(orderedVariables)
        return input.antijoin(matches)
    }
}
