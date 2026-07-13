package org.hooray.engine

class PredicatePattern(
    override val idx: Int,
    private val arguments: List<PatternValue>,
    private val predicate: Any,
) : PlanPattern, ExecPattern {
    init {
        require(arguments.size in 1..2) { "Hooray only supports unary and binary predicates for now." }
    }

    override val orderedVariables: List<Variable> = arguments.orderedVariables()
    override val variables: Set<Variable> = orderedVariables.toSet()

    override fun groundingGroups(bound: List<Variable>): List<GroundingGroup> = emptyList()

    override fun validate(
        input: BindingSet,
        introduces: List<Variable>,
        targetVariables: List<Variable>,
    ): BindingSet {
        require(input.variables.containsAll(variables)) { "Predicate arguments must be bound before validation" }
        return BindingSet(input.variables, input.rows.filter { row -> evaluate(input.variables, row) })
    }

    @Suppress("UNCHECKED_CAST")
    private fun evaluate(layout: List<Variable>, row: BindingRow): Boolean {
        val values = arguments.map { it.resolve(layout, row) }
        return when (values.size) {
            1 -> (predicate as (Any) -> Boolean)(values[0])
            2 -> (predicate as (Any, Any) -> Boolean)(values[0], values[1])
            else -> error("Unreachable")
        }
    }
}
