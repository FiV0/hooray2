package org.hooray.engine

import java.util.concurrent.ConcurrentHashMap

internal data class GroundingClosure(
    val groups: List<GroundingGroup>,
    val covered: Set<Variable>,
)

internal fun branchOrderedVariables(branch: PatternBranch): List<Variable> {
    val variables = linkedSetOf<Variable>()
    branch.patterns.forEach { pattern -> variables += pattern.orderedVariables }
    return variables.toList()
}

internal fun groundingClosure(
    patterns: List<PlanPattern>,
    initiallyBound: List<Variable>,
): GroundingClosure {
    require(initiallyBound.toSet().size == initiallyBound.size) { "Bound variables must be distinct" }
    val covered = initiallyBound.toMutableSet()
    val groups = mutableListOf<GroundingGroup>()

    var changed: Boolean
    do {
        changed = false
        for (pattern in patterns) {
            for (group in pattern.groundingGroups(covered.toList())) {
                require(pattern.orderedVariables.containsAll(group.variables)) {
                    "Pattern returned a grounding group containing variables it does not contain"
                }
                require(group.variables.none { it in covered }) {
                    "Pattern returned a grounding group containing an already bound variable"
                }
                groups += group
                covered += group.variables
                changed = true
            }
        }
    } while (changed)

    return GroundingClosure(groups, covered)
}

internal class CachedBranchExecutor(
    private val engine: StageEngine,
) {
    private val stagesByRequest = ConcurrentHashMap<BranchRequest, List<Stage>>()

    fun execute(
        branchIndex: Int,
        branch: PatternBranch,
        request: StageRequest,
        input: BindingSet,
    ): BindingSet {
        val key = BranchRequest(branchIndex, request)
        val stages = stagesByRequest.computeIfAbsent(key) {
            branch.stageFactory.stages(request).toList()
        }
        return engine.execute(stages, input)
    }

    private data class BranchRequest(
        val branchIndex: Int,
        val request: StageRequest,
    )
}
