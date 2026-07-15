package org.hooray.engine

class FunctionPattern(
    override val idx: Int,
    private val arguments: List<PatternValue>,
    private val output: Variable,
    private val function: Any,
) : PlanPattern, ExecPattern {
    init {
        require(arguments.size in 1..2) { "Hooray only supports unary and binary functions for now." }
        require(output !in arguments.orderedVariables()) { "Function output must differ from its arguments" }
    }

    private val argumentVariables = arguments.orderedVariables()
    override val orderedVariables: List<Variable> = argumentVariables + output
    override val variables: Set<Variable> = orderedVariables.toSet()

    override fun groundable(bound: Set<Variable>): List<Variable> {
        return if (bound.containsAll(argumentVariables)) {
            listOf(output)
        } else {
            emptyList()
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun evaluate(layout: List<Variable>, row: BindingRow): Any {
        val values = arguments.map { it.resolve(layout, row) }
        return when (values.size) {
            1 -> (function as (Any) -> Any)(values[0])
            2 -> (function as (Any, Any) -> Any)(values[0], values[1])
            else -> error("Unreachable")
        }
    }

    override fun count(
        input: BindingSet,
        introduces: List<Variable>,
        proposals: List<Proposal>,
    ): List<Proposal> {
        if (introduces != listOf(output) || !input.variables.containsAll(argumentVariables)) return proposals
        return updateProposals(idx, proposals, List(input.rowCount) { 1 })
    }

    override fun propose(
        input: BindingSet,
        introduces: List<Variable>,
        targetVariables: List<Variable>,
    ): BindingSet {
        require(introduces == listOf(output) && input.variables.containsAll(argumentVariables)) {
            "Function can only introduce its output after its arguments are bound"
        }
        val extensions = input.rows.mapIndexed { rowIndex, row ->
            RowExtension(rowIndex, listOf(evaluate(input.variables, row)))
        }
        return input.extend(introduces, extensions).reorder(targetVariables).distinctRows()
    }

    override fun validate(
        input: BindingSet,
        introduces: List<Variable>,
        targetVariables: List<Variable>,
    ): BindingSet {
        require(input.variables.containsAll(variables)) { "Function arguments and output must be bound before validation" }
        val outputIndex = input.columnIndex(output)
        return BindingSet(
            input.variables,
            input.rows.filter { row -> row[outputIndex] == evaluate(input.variables, row) },
        )
    }
}
