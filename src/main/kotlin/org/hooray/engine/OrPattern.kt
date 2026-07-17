package org.hooray.engine

/**
 * A pattern that executes a set of branches, each of which is a list of stages. The result is the union of the results of each branch.
 *
 * @property idx The index of this pattern in the plan.
 * @property branches The branches to execute. Each branch is a list of stages.
 *
 * The contract with the outer caller is that the incoming input [BindingSet] must contain all variables that the
 * branches can not ground. Each branch contains the same variables. The incoming [BindingSet] is projected/reordered
 * to match the execution of the branches. Each branch is executed independently and the results are unioned together.
 * Finally, the unioned result is semijoined again against the full incoming [BindingSet].
 * We also guarantee that the input.variables restricted to variables appearing in the branches will be added in that
 * order in the inner stages execution. This is required so that the RelationPattern can work correctly when joining
 * against the inner patterns.
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
        // TODO: OrPatter count is currently a no-op. It should only ever propose if it's the only
        // proposer of a set of variables.
        return proposals
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

    // TODO: both propose and validate are kind of the same thing in this case
    // Datatoad does a special casing here, by doing the first stage as pure validation stage against the input.
    // TODO: Once we have a columnar trie implementation, the semi-join can likely go.
    // We could also check if the variables of executeBranches match the input variables, and if so,
    // we can skip the semi-join as well.
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
        return input.semijoin(executeBranches(input)).project(targetVariables)
    }

    private fun validate(
        input: BindingSet,
        added: List<Variable>,
        targetVariables: List<Variable>,
    ): BindingSet {
        require(added.isEmpty()) { "OR validation cannot add variables" }
        val groundable = groundable(input.variables.toSet())
        require(input.variables.toSet() + groundable.toSet() == orderedVariables.toSet()) {
            "OR pattern can only validate if all of it's non-groundable variables are already bound in the input"
        }
        return input.semijoin(executeBranches(input)).project(targetVariables)
    }

    private fun executeBranches(input: BindingSet): BindingSet {
        val unit = BindingSet(emptyList(), listOf(emptyList()))
        var result = BindingSet(orderedVariables, emptyList())
        val inputRelation = RelationPattern(0, input.project(orderedVariables.filter {variable -> variable in input.variables}))
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
