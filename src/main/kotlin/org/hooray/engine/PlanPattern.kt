package org.hooray.engine

interface PlanPattern {
    val orderedVariables: List<Variable>

    /** Returns variables this pattern can ground from the supplied bound variables. */
    fun groundable(bound: Set<Variable>): List<Variable>
}

data class PatternBranch(
    val patterns: List<PlanPattern>,
    val proposalStages: List<Stage>,
    val validationStages: List<Stage>,
) {
    init {
        require(patterns.isNotEmpty()) { "Pattern branches must not be empty" }
    }
}
