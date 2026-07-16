package org.hooray.engine

class OrPattern(
    override val idx: Int,
    private val branches: List<List<Stage>>,
) : Pattern {
    private val engine = GenericJoinEngine()

    override val orderedVariables: List<Variable>
    override val variables: Set<Variable>

    init {
        require(branches.isNotEmpty()) { "OR patterns must have at least one branch" }
        val branchVariables = branches.map { stages ->
            stages.flatMap { stage -> stage.added }
        }
        require(branchVariables.all { it.toSet() == branchVariables.first().toSet() }) {
            "OR branches must have the same variables"
        }
        orderedVariables = branchVariables.first()
        variables = orderedVariables.toSet()
    }

    private fun branchGroundable(
        stages: List<Stage>,
        bound: Set<Variable>,
    ): Set<Variable> {
        val patterns = stages
            .flatMap { stage -> stage.participants }
            .distinct()
        val covered = bound.toMutableSet()
        val groundable = linkedSetOf<Variable>()

        var changed: Boolean
        do {
            changed = false
            patterns.forEach { pattern ->
                pattern.groundable(covered).forEach { variable ->
                    require(variable in pattern.orderedVariables) {
                        "Pattern returned a groundable variable it does not contain"
                    }
                    if (covered.add(variable)) {
                        groundable += variable
                        changed = true
                    }
                }
            }
        } while (changed)

        return groundable
    }

    override fun groundable(bound: Set<Variable>): List<Variable> {
        val common = branches
            .map { stages -> branchGroundable(stages, bound) }
            .reduce { intersection, branch -> intersection.intersect(branch) }
        return orderedVariables.filter { variable -> variable in common }
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
        return input.join(executeBranches(input))
            .project(targetVariables)
            .distinctRows()
    }

    private fun validate(
        input: BindingSet,
        added: List<Variable>,
        targetVariables: List<Variable>,
    ): BindingSet {
        require(added.isEmpty()) { "OR validation cannot add variables" }
        return input.semijoin(executeBranches(input))
    }

    private fun executeBranches(input: BindingSet): BindingSet {
        val unit = BindingSet(emptyList(), listOf(emptyList()))
        var result = BindingSet(orderedVariables, emptyList())
        branches.forEach { stages ->
            val branchVariables = stages.flatMap { stage -> stage.added }
            val inputVariables = branchVariables.filter { variable -> variable in input.variables }
            val inputRelation = RelationPattern(idx, input.project(inputVariables))
            val branchResult = engine.execute(stages.map { stage ->
                if (stage.added.any { variable -> variable in inputRelation.variables }) {
                    stage.copy(participants = stage.participants + inputRelation)
                } else {
                    stage
                }
            }, unit).project(orderedVariables)
            result = result.unionDistinct(branchResult)
        }
        return result
    }
}
