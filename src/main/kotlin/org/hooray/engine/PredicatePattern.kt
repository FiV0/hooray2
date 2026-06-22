package org.hooray.engine

import java.util.function.Function

class PredicatePattern(
    private val arguments: List<PatternValue>,
    private val predicate: Function<List<Any>, Boolean>,
) : ExecPattern {
    override val variables: Set<Any> = arguments.variables()
    override val proposerEligible: Boolean = false

    override fun validate(input: BindingSet, targetVariables: List<Any>): BindingSet {
        requireVariablesBound(input.variables, variables, "Predicate variables must be bound before validation")

        val rows = input.rows.filter { row ->
            predicate.apply(arguments.map { argument -> argument.value(input.variables, row) })
        }
        return BindingSet(input.variables, rows).reorder(targetVariables)
    }
}
