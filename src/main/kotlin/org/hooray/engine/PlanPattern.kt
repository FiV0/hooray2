package org.hooray.engine

interface PlanPattern {
    val orderedVariables: List<Variable>

    /** Returns variables this pattern can ground from the supplied bound variables. */
    fun groundable(bound: Set<Variable>): List<Variable>
}

enum class StageRequestMode {
    PROPOSE,
    VALIDATE,
}

data class StageRequest(
    val seedVariables: List<Variable>,
    val introduces: List<Variable>,
    val targetVariables: List<Variable>,
    val mode: StageRequestMode,
) {
    init {
        require(seedVariables.toSet().size == seedVariables.size) {
            "Stage request seed variables must be distinct"
        }
        require(introduces.toSet().size == introduces.size) {
            "Stage request introduced variables must be distinct"
        }
        require(targetVariables.toSet().size == targetVariables.size) {
            "Stage request target variables must be distinct"
        }
    }
}

fun interface StageFactory {
    /** Builds the nested stages for one exact seed shape and execution mode. */
    fun stages(request: StageRequest): List<Stage>
}

data class PatternBranch(
    val patterns: List<PlanPattern>,
    val stageFactory: StageFactory,
) {
    init {
        require(patterns.isNotEmpty()) { "Pattern branches must not be empty" }
    }
}
