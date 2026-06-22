package org.hooray.engine

typealias BindingRow = List<Any>

data class RowExtension(
    val inputRowIndex: Int,
    val values: BindingRow,
)

data class BindingSet(
    val variables: List<Any>,
    val rows: List<BindingRow>,
) {
    init {
        require(variables.toSet().size == variables.size) {
            "BindingSet variables must be distinct"
        }
        rows.forEachIndexed { index, row ->
            require(row.size == variables.size) {
                "Row $index has arity ${row.size}, expected ${variables.size}"
            }
        }
    }

    val rowCount: Int
        get() = rows.size

    fun valueAt(rowIndex: Int, variable: Any): Any {
        return rows[rowIndex][columnIndex(variable)]
    }

    fun columnIndex(variable: Any): Int {
        val index = variables.indexOf(variable)
        require(index >= 0) { "Unknown variable $variable" }
        return index
    }

    fun extend(
        introducedVariables: List<Any>,
        extensions: List<RowExtension>,
    ): BindingSet {
        require(introducedVariables.toSet().size == introducedVariables.size) {
            "Introduced variables must be distinct"
        }
        require(variables.intersect(introducedVariables.toSet()).isEmpty()) {
            "Introduced variables must not already be bound"
        }

        val expectedArity = introducedVariables.size
        val extendedRows = extensions.map { extension ->
            require(extension.inputRowIndex in rows.indices) {
                "Input row index ${extension.inputRowIndex} is out of bounds"
            }
            require(extension.values.size == expectedArity) {
                "Extension for input row ${extension.inputRowIndex} has arity ${extension.values.size}, expected $expectedArity"
            }
            rows[extension.inputRowIndex] + extension.values
        }

        return BindingSet(variables + introducedVariables, extendedRows)
    }

    fun distinctRows(): BindingSet {
        return BindingSet(variables, rows.distinct())
    }

    fun reorder(targetVariables: List<Any>): BindingSet {
        require(targetVariables.toSet().size == targetVariables.size) {
            "Target layout variables must be distinct"
        }
        require(targetVariables.toSet() == variables.toSet()) {
            "Target layout must contain the same variables"
        }

        val order = targetVariables.map(::columnIndex)
        return BindingSet(
            variables = targetVariables,
            rows = rows.map { row -> order.map { index -> row[index] } },
        )
    }
}
