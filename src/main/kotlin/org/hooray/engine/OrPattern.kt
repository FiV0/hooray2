package org.hooray.engine

/**
 * Executes a disjunction as a set of independently planned branches.
 *
 * @property idx The index of this pattern in the plan.
 * @property branches The branches to execute. Each branch is a list of stages.
 *
 * Every branch must introduce the same variables in the same order. The incoming [BindingSet] must bind every branch
 * variable that is not groundable by every branch. Before nested execution, the branch-relevant input columns are
 * projected into a [RelationPattern] that constrains each branch. The branch results are unioned distinctly and
 * correlated with the complete input so that unrelated outer columns are preserved. [GenericJoinEngine] calls an
 * OrPattern to propose only when it is the sole participant in a stage.
 */
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
        require(branchVariables.all { it == branchVariables.first() }) {
            "OR branches must introduce variables in the same order"
        }
        orderedVariables = branchVariables.first()
        variables = orderedVariables.toSet()
    }

    // TODO: If the children had a topological order, a simple fold might suffice.
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
        // GenericJoinEngine bypasses counting when this is the stage's sole participant.
        // TODO: propose for disjunctions. Upper bound is at least sumOf(count(branch))
        return proposals
    }

    // Both proposal and validation execute the nested branches. Datatoad handles the analogous seeded-plan case
    // specially: stage 0 semijoins the seed with fully covered atoms without introducing columns.
    // TODO: Move per-stage sequestration into GenericJoinEngine. As in Datatoad, temporarily remove input columns not
    // referenced by any participant in the current stage, execute the stage, and then reattach those columns.
    // If GenericJoinEngine handled sequestration itself, OrPattern would not need to project away unrelated input
    // columns before branch execution and then join or semijoin the branch results with the full input.
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
        val groundable = groundable(input.variables.toSet())
        require(groundable.containsAll(added)) {
            "Or pattern can only propose variables it can ground"
        }
        val missing = orderedVariables.filterNot { it in input.variables }
        require(added.toSet() == missing.toSet()) {
            "Or pattern can only propose all of its missing variables"
        }
        return input.join(executeBranches(input)).project(targetVariables)
    }

    private fun validate(
        input: BindingSet,
        added: List<Variable>,
        targetVariables: List<Variable>,
    ): BindingSet {
        require(added.isEmpty()) { "OR validation cannot add variables" }
        val missing = orderedVariables.filterNot { it in input.variables }
        val groundable = groundable(input.variables.toSet())
        require(groundable.containsAll(missing)) {
            "OR pattern can only validate if all its non-groundable variables are already bound in the input"
        }
        return input.semijoin(executeBranches(input))
    }

    private fun executeBranches(input: BindingSet): BindingSet {
        val unit = BindingSet(emptyList(), listOf(emptyList()))
        var result = BindingSet(orderedVariables, emptyList())
        val inputRelation = RelationPattern(
            idx,
            input.project(orderedVariables.filter { variable -> variable in input.variables }),
        )
        branches.forEach { stages ->
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
