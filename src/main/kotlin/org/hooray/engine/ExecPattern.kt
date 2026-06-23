package org.hooray.engine

interface ExecPattern {
    val variables: Set<Any>
    val proposerEligible: Boolean

    fun count(input: BindingSet, introduces: List<Any>): List<Int> {
        throw UnsupportedOperationException("Pattern is not proposer-eligible")
    }

    fun propose(
        input: BindingSet,
        introduces: List<Any>,
        targetVariables: List<Any>,
    ): BindingSet {
        throw UnsupportedOperationException("Pattern is not proposer-eligible")
    }

    fun validate(input: BindingSet): BindingSet
}
