package org.hooray.engine

import java.util.concurrent.ConcurrentHashMap

internal data class GroundingClosure(
    val groundable: List<Variable>,
    val covered: Set<Variable>,
)

internal fun branchOrderedVariables(branch: PatternBranch): List<Variable> {
    val variables = linkedSetOf<Variable>()
    branch.patterns.forEach { pattern -> variables += pattern.orderedVariables }
    return variables.toList()
}

internal fun groundingClosure(
    patterns: List<PlanPattern>,
    initiallyBound: Set<Variable>,
): GroundingClosure {
    val covered = initiallyBound.toMutableSet()
    val groundable = mutableListOf<Variable>()

    var changed: Boolean
    do {
        changed = false
        for (pattern in patterns) {
            for (variable in pattern.groundable(covered)) {
                require(variable in pattern.orderedVariables) {
                    "Pattern returned a groundable variable it does not contain"
                }
                require(variable !in covered) {
                    "Pattern returned an already bound variable as groundable"
                }
                groundable += variable
                covered += variable
                changed = true
            }
        }
    } while (changed)

    return GroundingClosure(groundable, covered)
}

internal class CachedBranchExecutor(
    private val engine: GenericJoinEngine,
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
