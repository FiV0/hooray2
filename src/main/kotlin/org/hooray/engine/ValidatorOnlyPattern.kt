package org.hooray.engine

class ValidatorOnlyPattern(
    private val delegate: ExecPattern,
) : ExecPattern {
    override val variables: Set<Any> = delegate.variables
    override val proposerEligible: Boolean = false

    override fun validate(input: BindingSet, targetVariables: List<Any>): BindingSet {
        return delegate.validate(input, targetVariables)
    }
}
