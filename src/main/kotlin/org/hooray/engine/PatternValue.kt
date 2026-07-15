package org.hooray.engine

sealed interface PatternValue {
    data class Variable(val name: clojure.lang.Symbol) : PatternValue
    data class Constant(val value: Any) : PatternValue
}

internal fun Iterable<PatternValue>.orderedVariables(): List<Variable> {
    val result = linkedSetOf<Variable>()
    for (value in this) {
        if (value is PatternValue.Variable) result += value.name
    }
    return result.toList()
}

internal fun PatternValue.resolve(layout: List<Variable>, row: BindingRow): Any {
    return when (this) {
        is PatternValue.Constant -> value
        is PatternValue.Variable -> {
            val column = layout.indexOf(name)
            require(column >= 0) { "Variable $name is not bound" }
            row[column]
        }
    }
}

internal fun PatternValue.rowReader(columnIndexes: Map<Variable, Int>): (BindingRow) -> Any {
    return when (this) {
        is PatternValue.Constant -> {
            val constant = value
            val reader: (BindingRow) -> Any = { constant }
            reader
        }
        is PatternValue.Variable -> {
            val column = columnIndexes[name]
                ?: throw IllegalArgumentException("Variable $name is not bound")
            val reader: (BindingRow) -> Any = { row -> row[column] }
            reader
        }
    }
}
