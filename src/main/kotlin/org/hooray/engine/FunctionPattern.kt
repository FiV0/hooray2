package org.hooray.engine

class FunctionPattern(
    override val idx: Int,
    private val arguments: List<PatternValue>,
    private val output: Variable,
    private val function: Any,
) : Pattern {
    init {
        require(arguments.size in 1..2) { "Hooray only supports unary and binary functions for now." }
        require(output !in arguments.orderedVariables()) { "Function output must differ from its arguments" }
    }

    private val argumentVariables = arguments.orderedVariables()
    override val orderedVariables: List<Variable> = argumentVariables + output
    override val variables: Set<Variable> = orderedVariables.toSet()

    override fun groundable(bound: Set<Variable>): List<Variable> {
        return if (bound.containsAll(argumentVariables) && output !in bound) {
            listOf(output)
        } else {
            emptyList()
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun evaluator(columnIndexes: Map<Variable, Int>): (BindingRow) -> Any {
        val readers = arguments.map { argument -> argument.rowReader(columnIndexes) }
        return when (readers.size) {
            1 -> {
                val f = function as (Any) -> Any
                val r0 = readers[0]
                val evaluate: (BindingRow) -> Any = { row -> f(r0(row)) }
                evaluate
            }
            2 -> {
                val f = function as (Any, Any) -> Any
                val r0 = readers[0]
                val r1 = readers[1]
                val evaluate: (BindingRow) -> Any = { row -> f(r0(row), r1(row)) }
                evaluate
            }
            else -> error("Unreachable")
        }
    }

    override fun count(
        input: BindingSet,
        added: List<Variable>,
        proposals: List<Proposal>,
    ): List<Proposal> {
        if (added != listOf(output) || !input.variables.containsAll(argumentVariables)) return proposals
        return updateProposals(idx, proposals, List(input.rowCount) { 1 })
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

    private fun propose(
        input: BindingSet,
        added: List<Variable>,
        targetVariables: List<Variable>,
    ): BindingSet {
        require(added == listOf(output) && input.variables.containsAll(argumentVariables)) {
            "Function can only introduce its output after its arguments are bound"
        }
        val evaluate = evaluator(input.columnIndexes)
        val extensions = input.rows.mapIndexed { rowIndex, row ->
            RowExtension(rowIndex, listOf(evaluate(row)))
        }
        return input.extend(added, extensions).reorder(targetVariables)
    }

    private fun validate(
        input: BindingSet,
        added: List<Variable>,
        targetVariables: List<Variable>,
    ): BindingSet {
        require(added.isEmpty()) { "Function validation cannot add variables" }
        require(input.variables.containsAll(variables)) { "Function arguments and output must be bound before validation" }
        val outputIndex = input.columnIndex(output)
        val evaluate = evaluator(input.columnIndexes)
        return BindingSet(
            input.variables,
            input.rows.filter { row -> row[outputIndex] == evaluate(row) },
        )
    }
}
