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

    override fun groundable(bound: Set<Variable>): List<Variable> = emptyList()

    @Suppress("UNCHECKED_CAST")
    private fun evaluator(columnIndexes: Map<Variable, Int>): (BindingRow) -> Boolean {
        val readers = arguments.map { argument -> argument.rowReader(columnIndexes) }
        return when (readers.size) {
            1 -> {
                val f = predicate as (Any) -> Boolean
                val r0 = readers[0]
                val evaluate: (BindingRow) -> Boolean = { row -> f(r0(row)) }
                evaluate
            }
            2 -> {
                val f = predicate as (Any, Any) -> Boolean
                val r0 = readers[0]
                val r1 = readers[1]
                val evaluate: (BindingRow) -> Boolean = { row -> f(r0(row), r1(row)) }
                evaluate
            }
            else -> error("Unreachable")
        }
    }

    override fun validate(
        input: BindingSet,
        introduces: List<Variable>,
        targetVariables: List<Variable>,
    ): BindingSet {
        require(input.variables.containsAll(variables)) { "Predicate arguments must be bound before validation" }
        val evaluate = evaluator(input.columnIndexes)
        return BindingSet(input.variables, input.rows.filter(evaluate))
    }

}
