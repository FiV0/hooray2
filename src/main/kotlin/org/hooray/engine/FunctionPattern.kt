package org.hooray.engine

import java.util.function.Function

class FunctionPattern(
    override val idx: Int,
    private val arguments: List<PatternValue>,
    private val returnVariable: Any,
    private val function: Function<List<Any>, Any>,
) : ExecPattern {
    private val argumentVariables = arguments.variables()

    override val variables: Set<Any> = argumentVariables + returnVariable

    override fun count(
        input: BindingSet,
        introduces: List<Any>,
        proposals: List<Proposal>,
    ): List<Proposal> {
        if (introduces != listOf(returnVariable)) {
            return proposals
        }
        if (!input.variables.containsAll(argumentVariables)) {
            return proposals
        }
        return updateProposals(idx, proposals, List(input.rowCount) { 1 })
    }

    override fun propose(
        input: BindingSet,
        introduces: List<Any>,
        targetVariables: List<Any>,
    ): BindingSet {
        require(introduces == listOf(returnVariable)) {
            "Function pattern can only introduce its return variable"
        }
        requireVariablesBound(input.variables, argumentVariables, "Function input variables must be bound before proposal")

        val extensions = input.rows.mapIndexed { rowIndex, row ->
            RowExtension(
                inputRowIndex = rowIndex,
                values = listOf(function.apply(arguments.map { argument -> argument.value(input.variables, row) })),
            )
        }
        return input.extend(introduces, extensions).reorder(targetVariables)
    }

    override fun validate(input: BindingSet): BindingSet {
        requireVariablesBound(input.variables, variables, "Function variables must be bound before validation")

        val returnIndex = input.columnIndex(returnVariable)
        val rows = input.rows.filter { row ->
            function.apply(arguments.map { argument -> argument.value(input.variables, row) }) == row[returnIndex]
        }
        return BindingSet(input.variables, rows)
    }
}
