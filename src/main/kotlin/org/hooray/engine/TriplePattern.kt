package org.hooray.engine

class TriplePattern(
    override val idx: Int,
    aev: Map<Any, Map<Any, Set<Any>>>,
    ave: Map<Any, Map<Any, Set<Any>>>,
    private val entity: PatternValue,
    attribute: Any,
    private val value: PatternValue,
) : PlanPattern, ExecPattern {
    override val orderedVariables: List<Variable> = listOf(entity, value).orderedVariables()
    override val variables: Set<Variable> = orderedVariables.toSet()
    private val ev = aev[attribute] ?: emptyMap()
    private val ve = ave[attribute] ?: emptyMap()

    private val entityVariable = (entity as? PatternValue.Variable)?.name
    private val valueVariable = (value as? PatternValue.Variable)?.name
    private val entityConstant = (entity as? PatternValue.Constant)?.value
    private val valueConstant = (value as? PatternValue.Constant)?.value

    init {
        require(entityVariable == null || entityVariable != valueVariable) {
            "Triple pattern entity and value variables must be different"
        }
    }

    override fun groundable(bound: Set<Variable>): List<Variable> =
        orderedVariables.filterNot { it in bound }

    private sealed interface Resolved {
        data class Bound(val value: Any) : Resolved
        data object Unbound : Resolved
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

    private fun candidates(entity: Resolved, value: Resolved): List<Pair<Any, Any>> {
        return when {
            entity is Resolved.Bound && value is Resolved.Bound -> {
                if (ev[entity.value]?.contains(value.value) == true) {
                    listOf(entity.value to value.value)
                } else {
                    emptyList()
                }
            }
            entity is Resolved.Bound ->
                ev[entity.value].orEmpty().map { entity.value to it }
            value is Resolved.Bound ->
                ve[value.value].orEmpty().map { it to value.value }
            else ->
                ev.flatMap { (candidateEntity, values) ->
                    values.map { candidateValue -> candidateEntity to candidateValue }
                }
        }
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

    override fun count(
        input: BindingSet,
        introduces: List<Variable>,
        proposals: List<Proposal>,
    ): List<Proposal> {
        require(introduces.isNotEmpty() && variables.intersect(introduces.toSet()).isNotEmpty()) {
            "Triple pattern cannot introduce variables it does not contain"
        }
        return updateProposals(
            idx,
            proposals,
            input.rows.map { row -> matchingIntroductions(input.variables, row, introduces).size },
        )
    }

    // TODO the intermediate row extension lists seem wasted work
    override fun propose(
        input: BindingSet,
        introduces: List<Variable>,
        targetVariables: List<Variable>,
    ): BindingSet {
        require(introduces.isNotEmpty() && variables.containsAll(introduces)) {
            "Triple pattern cannot introduce variables it does not contain"
        }
        when  {
            variables.size == 1 && introduces.size == 1 -> {
                require (introduces[0] == entityVariable || introduces[0] == valueVariable) {
                    "Triple pattern with one variable must introduce that variable"
                }
                when  {
                    entityVariable != null -> {
                        val value = valueConstant!!
                        val es = ve[value]
                        val extensions = buildList {
                            input.rows.forEachIndexed { rowIndex, _ ->
                                es?.forEach { entityValue ->
                                    add(RowExtension(rowIndex, listOf(entityValue)))
                                }
                            }
                        }
                        return input.extend(introduces, extensions).reorder(targetVariables)
                    }
                    valueVariable != null -> {
                        val entityValue = entityConstant!!
                        val vs = ev[entityValue]
                        val extensions = buildList {
                            input.rows.forEachIndexed { rowIndex, row ->
                                vs?.forEach { value ->
                                    add(RowExtension(rowIndex, listOf(value)))
                                }
                            }
                        }
                        return input.extend(introduces, extensions).reorder(targetVariables)
                    }
                    else -> throw IllegalStateException("Triple pattern with one variable must have that variable in the entity or value position")
                }

            }
            variables.size == 2 && introduces.size == 1 -> {
                val introduced = introduces.single()
                when (introduced) {
                    entityVariable -> {
                        val valueIndex = input.columnIndexes[valueVariable]
                        val extensions = if (valueIndex != null) {
                            buildList {
                                ve.forEach { (value, entityValues) ->
                                    entityValues.forEach { entityValue ->
                                        input.rows.forEachIndexed { rowIndex, row ->
                                            if (row[valueIndex] == value) {
                                                add(RowExtension(rowIndex, listOf(entityValue)))
                                            }
                                        }
                                    }
                                }
                            }
                        } else {
                            buildList {
                                ev.keys.forEach { entityValue ->
                                    input.rows.forEachIndexed { rowIndex, _ ->
                                        add(RowExtension(rowIndex, listOf(entityValue)))
                                    }
                                }
                            }
                        }
                        return input.extend(introduces, extensions).reorder(targetVariables)
                    }
                    valueVariable -> {
                        val entityIndex = input.columnIndexes[entityVariable]
                        val extensions = if (entityIndex != null) {
                            buildList {
                                ev.forEach { (entityValue, values) ->
                                    values.forEach { value ->
                                        input.rows.forEachIndexed { rowIndex, row ->
                                            if (row[entityIndex] == entityValue) {
                                                add(RowExtension(rowIndex, listOf(value)))
                                            }
                                        }
                                    }
                                }
                            }
                        } else {
                            buildList {
                                ve.keys.forEach { value ->
                                    input.rows.forEachIndexed { rowIndex, _ ->
                                        add(RowExtension(rowIndex, listOf(value)))
                                    }
                                }
                            }
                        }
                        return input.extend(introduces, extensions).reorder(targetVariables)
                    }
                    else -> throw IllegalStateException("Triple pattern with two variables must introduce one of those variables")
                }
            }
            variables.size == 2 && introduces.size == 2 -> {
                require (input.variables.intersect(introduces.toSet()).isEmpty()) {
                    "Triple pattern with two variables cannot introduce a variable that is already bound"
                }
                val extensions = buildList {
                    input.rows.forEachIndexed { rowIndex, _ ->
                        ev.forEach { (entityValue, values) ->
                            values.forEach { value ->
                                val introducedValues = introduces.map { variable ->
                                    when (variable) {
                                        entityVariable -> entityValue
                                        valueVariable -> value
                                        else -> throw IllegalStateException("Triple pattern can only introduce its entity and value variables")
                                    }
                                }
                                add(RowExtension(rowIndex, introducedValues))
                            }
                        }
                    }
                }
                return input.extend(introduces, extensions).reorder(targetVariables)
            }
            else -> throw IllegalStateException("Triple pattern must have 1 or 2 variables, found ${variables.size}")
        }
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
}
