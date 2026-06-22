package org.hooray.engine

class InputPattern private constructor(
    private val orderedVariables: List<Any>,
    private val bindingRows: List<BindingRow>,
) : ExecPattern {
    init {
        require(orderedVariables.toSet().size == orderedVariables.size) {
            "Input variables must be distinct"
        }
        bindingRows.forEachIndexed { index, row ->
            require(row.size == orderedVariables.size) {
                "Input row $index has arity ${row.size}, expected ${orderedVariables.size}"
            }
        }
    }

    override val variables: Set<Any> = orderedVariables.toSet()
    override val proposerEligible: Boolean = true

    override fun count(input: BindingSet, introduces: List<Any>): List<Int> {
        require(variables.containsAll(introduces)) {
            "Input pattern cannot introduce variables it does not contain"
        }

        return input.rows.map { row ->
            matchingRows(input.variables, row)
                .map { bindingRow -> introducedValues(bindingRow, introduces) }
                .toSet()
                .size
        }
    }

    override fun propose(
        input: BindingSet,
        introduces: List<Any>,
        targetVariables: List<Any>,
    ): BindingSet {
        require(variables.containsAll(introduces)) {
            "Input pattern cannot introduce variables it does not contain"
        }

        val extensions = mutableListOf<RowExtension>()
        input.rows.forEachIndexed { rowIndex, row ->
            val seenIntroductions = mutableSetOf<BindingRow>()
            for (bindingRow in matchingRows(input.variables, row)) {
                val introducedValues = introducedValues(bindingRow, introduces)
                if (seenIntroductions.add(introducedValues)) {
                    extensions += RowExtension(rowIndex, introducedValues)
                }
            }
        }

        return input.extend(introduces, extensions).reorder(targetVariables)
    }

    override fun validate(input: BindingSet, targetVariables: List<Any>): BindingSet {
        val rows = input.rows.filter { row ->
            matchingRows(input.variables, row).isNotEmpty()
        }
        return BindingSet(input.variables, rows).reorder(targetVariables)
    }

    private fun matchingRows(layout: List<Any>, row: BindingRow): List<BindingRow> {
        return bindingRows.filter { bindingRow ->
            orderedVariables.indices.all { index ->
                val variable = orderedVariables[index]
                val boundIndex = layout.indexOf(variable)
                boundIndex < 0 || row[boundIndex] == bindingRow[index]
            }
        }
    }

    private fun introducedValues(bindingRow: BindingRow, introduces: List<Any>): BindingRow {
        return introduces.map { variable ->
            val index = orderedVariables.indexOf(variable)
            require(index >= 0) { "Unknown input variable $variable" }
            bindingRow[index]
        }
    }

    companion object {
        @JvmStatic
        fun scalar(variable: Any, values: List<Any>): InputPattern {
            return relation(
                variables = listOf(variable),
                rows = values.map { value -> listOf(value) },
            )
        }

        @JvmStatic
        fun relation(
            variables: List<Any>,
            rows: List<BindingRow>,
        ): InputPattern = InputPattern(variables, rows)
    }
}
