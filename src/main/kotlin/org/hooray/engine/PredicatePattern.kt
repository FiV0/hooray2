package org.hooray.engine

import java.util.function.Function

class PredicatePattern(
    override val idx: Int,
    private val arguments: List<PatternValue>,
    private val predicate: Function<List<Any>, Boolean>,
) : ExecPattern {
    override val variables: Set<Any> = arguments.variables()

    override fun validate(input: BindingSet): BindingSet {
        requireVariablesBound(input.variables, variables, "Predicate variables must be bound before validation")

        val rows = input.rows.filter { row ->
            predicate.apply(arguments.map { argument -> argument.value(input.variables, row) })
        }
        return BindingSet(input.variables, rows)
    }
}
