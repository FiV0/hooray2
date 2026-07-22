package org.hooray.engine

import clojure.lang.Symbol

typealias Variable = Symbol

typealias BindingRow = List<Any>

data class RowExtension(
    val inputRowIndex: Int,
    val values: BindingRow,
)

data class BindingIndex(
    val rowIndexesByKey: Map<BindingRow, List<Int>>,
)

data class BindingSet(
    val variables: List<Variable>,
    val rows: List<BindingRow>,
) {
    val columnIndexes: Map<Variable, Int>

    init {
        require(variables.toSet().size == variables.size) {
            "BindingSet variables must be distinct"
        }
        rows.forEachIndexed { index, row ->
            require(row.size == variables.size) {
                "Row $index has arity ${row.size}, expected ${variables.size}"
            }
        }
        columnIndexes = variables.mapIndexed { index, variable -> variable to index }.toMap()
    }

    val rowCount: Int
        get() = rows.size

    fun valueAt(rowIndex: Int, variable: Variable): Any {
        return rows[rowIndex][columnIndex(variable)]
    }

    fun columnIndex(variable: Variable): Int {
        return columnIndexes[variable]
            ?: throw IllegalArgumentException("Unknown variable $variable")
    }

    fun extend(
        introducedVariables: List<Variable>,
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

    fun indexBy(indexVariables: List<Variable>): BindingIndex {
        require(indexVariables.toSet().size == indexVariables.size) {
            "Index variables must be distinct"
        }

        val keyIndexes = indexVariables.map(::columnIndex)
        val mutableIndex = linkedMapOf<BindingRow, MutableList<Int>>()
        rows.forEachIndexed { rowIndex, row ->
            val key = keyIndexes.map { index -> row[index] }
            mutableIndex.getOrPut(key, ::mutableListOf).add(rowIndex)
        }

        return BindingIndex(
            rowIndexesByKey = mutableIndex.mapValues { (_, rowIndexes) -> rowIndexes.toList() },
        )
    }

    fun selectRows(rowIndexes: List<Int>): BindingSet {
        val selectedRows = rowIndexes.map { rowIndex ->
            require(rowIndex in rows.indices) {
                "Row index $rowIndex is out of bounds"
            }
            rows[rowIndex]
        }
        return BindingSet(variables, selectedRows)
    }

    fun project(targetVariables: List<Variable>): BindingSet {
        if (targetVariables == variables) {
            return this
        }
        require(targetVariables.toSet().size == targetVariables.size) {
            "Projection variables must be distinct"
        }
        require(variables.containsAll(targetVariables)) {
            "Projection variables must be a subset of the current variables"
        }

        val projection = targetVariables.map(::columnIndex)
        return BindingSet(
            variables = targetVariables,
            rows = rows.map { row -> projection.map { index -> row[index] } },
        )
    }

    /**
     * Natural join on the shared variables.
     *
     * The result column order is not stable: the sides are swapped so that the smaller
     * input is indexed, so whether this layout or [other]'s comes first depends on the
     * row counts. Callers must [project] or [reorder] the result into the layout they need.
     */
    fun join(other: BindingSet): BindingSet {
        if (this.rowCount < other.rowCount) {
            return other.join(this)
        }
        val sharedVariables = variables.filter { variable -> variable in other.columnIndexes }
        val leftKeyIndexes = sharedVariables.map(::columnIndex)
        val rightOnlyVariables = other.variables.filter { variable -> variable !in columnIndexes }
        val rightOnlyIndexes = rightOnlyVariables.map(other::columnIndex)
        val rightIndex = other.indexBy(sharedVariables)

        val joinedRows = buildList {
            rows.forEach { leftRow ->
                val key = leftKeyIndexes.map { index -> leftRow[index] }
                rightIndex.rowIndexesByKey[key].orEmpty().forEach { rightRowIndex ->
                    val rightRow = other.rows[rightRowIndex]
                    add(leftRow + rightOnlyIndexes.map { index -> rightRow[index] })
                }
            }
        }

        return BindingSet(variables + rightOnlyVariables, joinedRows)
    }

    private fun filterByExistence(
        other: BindingSet,
        keepMatches: Boolean,
    ): BindingSet {
        val sharedVariables = variables.filter { variable -> variable in other.columnIndexes }
        val leftKeyIndexes = sharedVariables.map(::columnIndex)
        val rightKeyIndexes = sharedVariables.map(other::columnIndex)
        val rightKeys = other.rows.mapTo(hashSetOf()) { row ->
            rightKeyIndexes.map { index -> row[index] }
        }
        val filteredRows = rows.filter { row ->
            val key = leftKeyIndexes.map { index -> row[index] }
            (key in rightKeys) == keepMatches
        }
        return BindingSet(variables, filteredRows)
    }

    fun semijoin(other: BindingSet): BindingSet {
        return filterByExistence(other, keepMatches = true)
    }

    fun antijoin(other: BindingSet): BindingSet {
        return filterByExistence(other, keepMatches = false)
    }

    fun union(other: BindingSet): BindingSet {
        require(variables.toSet() == other.variables.toSet()) {
            "Union requires the same variables"
        }

        val alignedOther = if (variables == other.variables) other else other.reorder(variables)
        return BindingSet(variables, (rows + alignedOther.rows))
    }

    fun unionDistinct(other: BindingSet): BindingSet {
        require(variables.toSet() == other.variables.toSet()) {
            "Union requires the same variables"
        }

        val alignedOther = if (variables == other.variables) other else other.reorder(variables)
        return BindingSet(variables, (rows + alignedOther.rows).distinct())
    }

    fun reorder(targetVariables: List<Variable>): BindingSet {
        require(targetVariables.toSet().size == targetVariables.size) {
            "Target layout variables must be distinct"
        }
        require(targetVariables.toSet() == variables.toSet()) {
            "Target layout must contain the same variables"
        }

        return project(targetVariables)
    }

}
