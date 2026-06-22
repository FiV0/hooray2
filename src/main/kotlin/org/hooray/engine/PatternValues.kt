package org.hooray.engine

fun List<PatternValue>.variables(): Set<Any> {
    return mapNotNull { argument ->
        (argument as? PatternValue.Variable)?.name
    }.toSet()
}

fun PatternValue.value(layout: List<Any>, row: BindingRow): Any {
    return when (this) {
        is PatternValue.Constant -> value
        is PatternValue.Variable -> {
            val index = layout.indexOf(name)
            require(index >= 0) { "Variable $name is not bound" }
            row[index]
        }
    }
}

fun requireVariablesBound(layout: List<Any>, variables: Set<Any>, message: String) {
    require(layout.containsAll(variables)) { message }
}
