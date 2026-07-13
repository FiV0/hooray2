package org.hooray.engine

class TriplePattern(
    override val idx: Int,
    private val aev: Map<Any, Map<Any, Set<Any>>>,
    private val ave: Map<Any, Map<Any, Set<Any>>>,
    private val entity: PatternValue,
    private val attribute: Any,
    private val value: PatternValue,
) : PlanPattern, ExecPattern {
    override val orderedVariables: List<Variable> = listOf(entity, value).orderedVariables()
    override val variables: Set<Variable> = orderedVariables.toSet()

    init {
        val entityVariable = (entity as? PatternValue.Variable)?.name
        val valueVariable = (value as? PatternValue.Variable)?.name
        require(entityVariable == null || entityVariable != valueVariable) {
            "Triple pattern entity and value variables must be different"
        }
    }

    override fun groundingGroups(bound: List<Variable>): List<GroundingGroup> {
        return orderedVariables
            .filterNot { it in bound }
            .map { GroundingGroup(listOf(it)) }
    }

    override fun count(
        input: BindingSet,
        introduces: List<Variable>,
        proposals: List<Proposal>,
    ): List<Proposal> {
        if (introduces.isEmpty() || !variables.containsAll(introduces)) return proposals
        return updateProposals(
            idx,
            proposals,
            input.rows.map { row -> matchingIntroductions(input.variables, row, introduces).size },
        )
    }

    override fun propose(
        input: BindingSet,
        introduces: List<Variable>,
        targetVariables: List<Variable>,
    ): BindingSet {
        require(introduces.isNotEmpty() && variables.containsAll(introduces)) {
            "Triple pattern cannot introduce variables it does not contain"
        }
        val extensions = buildList {
            input.rows.forEachIndexed { rowIndex, row ->
                matchingIntroductions(input.variables, row, introduces).forEach { values ->
                    add(RowExtension(rowIndex, values))
                }
            }
        }
        return input.extend(introduces, extensions).reorder(targetVariables).distinctRows()
    }

    override fun validate(
        input: BindingSet,
        introduces: List<Variable>,
        targetVariables: List<Variable>,
    ): BindingSet {
        return BindingSet(
            input.variables,
            input.rows.filter { row -> matchingIntroductions(input.variables, row, emptyList()).isNotEmpty() },
        )
    }

    private fun matchingIntroductions(
        layout: List<Variable>,
        row: BindingRow,
        introduces: List<Variable>,
    ): List<BindingRow> {
        val resolvedEntity = resolve(entity, layout, row)
        val resolvedValue = resolve(value, layout, row)
        val seen = linkedSetOf<BindingRow>()
        for ((candidateEntity, candidateValue) in candidates(resolvedEntity, resolvedValue)) {
            val valuesByVariable = mutableMapOf<Variable, Any>()
            if (!matches(entity, candidateEntity, layout, row, valuesByVariable)) continue
            if (!matches(value, candidateValue, layout, row, valuesByVariable)) continue
            seen += introduces.map(valuesByVariable::getValue)
        }
        return seen.toList()
    }

    private fun candidates(entity: Resolved, value: Resolved): List<Pair<Any, Any>> {
        return when {
            entity is Resolved.Bound && value is Resolved.Bound -> {
                if (aev[attribute]?.get(entity.value)?.contains(value.value) == true) {
                    listOf(entity.value to value.value)
                } else {
                    emptyList()
                }
            }
            entity is Resolved.Bound ->
                aev[attribute]?.get(entity.value).orEmpty().map { entity.value to it }
            value is Resolved.Bound ->
                ave[attribute]?.get(value.value).orEmpty().map { it to value.value }
            else ->
                aev[attribute].orEmpty().flatMap { (candidateEntity, values) ->
                    values.map { candidateValue -> candidateEntity to candidateValue }
                }
        }
    }

    private fun resolve(slot: PatternValue, layout: List<Variable>, row: BindingRow): Resolved {
        return when (slot) {
            is PatternValue.Constant -> Resolved.Bound(slot.value)
            is PatternValue.Variable -> {
                val index = layout.indexOf(slot.name)
                if (index >= 0) Resolved.Bound(row[index]) else Resolved.Unbound
            }
        }
    }

    private fun matches(
        slot: PatternValue,
        candidate: Any,
        layout: List<Variable>,
        row: BindingRow,
        valuesByVariable: MutableMap<Variable, Any>,
    ): Boolean {
        return when (slot) {
            is PatternValue.Constant -> slot.value == candidate
            is PatternValue.Variable -> {
                val index = layout.indexOf(slot.name)
                if (index >= 0) row[index] == candidate
                else valuesByVariable.put(slot.name, candidate) == null
            }
        }
    }

    private sealed interface Resolved {
        data class Bound(val value: Any) : Resolved
        data object Unbound : Resolved
    }
}
