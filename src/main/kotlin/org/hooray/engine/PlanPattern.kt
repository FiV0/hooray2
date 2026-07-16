package org.hooray.engine

interface PlanPattern {
    val orderedVariables: List<Variable>

    /** Returns variables this pattern can ground from the supplied bound variables. */
    fun groundable(bound: Set<Variable>): List<Variable>
}
