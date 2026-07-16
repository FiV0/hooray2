package org.hooray.engine

class TriplePattern(
    override val idx: Int,
    aev: Map<Any, Map<Any, Set<Any>>>,
    ave: Map<Any, Map<Any, Set<Any>>>,
    private val entity: PatternValue,
    attribute: Any,
    private val value: PatternValue,
) : Pattern {
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

    override fun count(
        input: BindingSet,
        added: List<Variable>,
        proposals: List<Proposal>,
    ): List<Proposal> {
        require(added.isNotEmpty() && variables.containsAll(added)) {
            "Triple pattern cannot introduce variables it does not contain"
        }
        when  {
            variables.size == 1 && added.size == 1 -> {
                require (added[0] == entityVariable || added[0] == valueVariable) {
                    "Triple pattern with one variable must introduce that variable"
                }
                when  {
                    entityVariable != null -> {
                        val value = valueConstant!!
                        val es = ve[value]
                        val counts = input.rows.map { es?.size ?: 0 }
                        return updateProposals(idx, proposals, counts)
                    }
                    valueVariable != null -> {
                        val entityValue = entityConstant!!
                        val vs = ev[entityValue]
                        val counts = input.rows.map { vs?.size ?: 0 }
                        return updateProposals(idx, proposals, counts)
                    }
                    else -> throw IllegalStateException("Triple pattern with one variable must have that variable in the entity or value position")
                }
            }
            variables.size == 2 && added.size == 1 -> {
                val introduced = added.single()
                when (introduced) {
                    entityVariable -> {
                        val valueIndex = input.columnIndexes[valueVariable]
                        val counts = if (valueIndex != null) {
                            input.rows.map { row -> ve[row[valueIndex]].orEmpty().size }
                        } else {
                            input.rows.map { ev.keys.size }
                        }
                        return updateProposals(idx, proposals, counts)
                    }
                    valueVariable -> {
                        val entityIndex = input.columnIndexes[entityVariable]
                        val counts = if (entityIndex != null) {
                            input.rows.map { row -> ev[row[entityIndex]].orEmpty().size }
                        } else {
                            input.rows.map { ve.keys.size }
                        }
                        return updateProposals(idx, proposals, counts)
                    }
                    else -> throw IllegalStateException("Triple pattern with two variables must introduce one of those variables")
                }
            }
            variables.size == 2 && added.size == 2 -> {
                require (input.variables.intersect(added.toSet()).isEmpty()) {
                    "Triple pattern with two variables cannot introduce a variable that is already bound"
                }
                val count = ev.values.sumOf { values -> values.size }
                val counts = input.rows.map { count }
                return updateProposals(idx, proposals, counts)
            }
            else -> throw IllegalStateException("Triple pattern must have 1 or 2 variables, found ${variables.size}")
        }
    }

    override fun join(
        input: BindingSet,
        added: List<Variable>,
        targetVariables: List<Variable>,
    ): BindingSet = if (added.isEmpty()) {
        validate(input, added, targetVariables)
    } else {
        propose(input, added, targetVariables)
    }

    // TODO the intermediate row extension lists seem wasted work
    private fun propose(
        input: BindingSet,
        added: List<Variable>,
        targetVariables: List<Variable>,
    ): BindingSet {
        require(added.isNotEmpty() && variables.containsAll(added)) {
            "Triple pattern cannot introduce variables it does not contain"
        }
        when  {
            variables.size == 1 && added.size == 1 -> {
                require (added[0] == entityVariable || added[0] == valueVariable) {
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
                        return input.extend(added, extensions).reorder(targetVariables)
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
                        return input.extend(added, extensions).reorder(targetVariables)
                    }
                    else -> throw IllegalStateException("Triple pattern with one variable must have that variable in the entity or value position")
                }

            }
            variables.size == 2 && added.size == 1 -> {
                val introduced = added.single()
                when (introduced) {
                    entityVariable -> {
                        val valueIndex = input.columnIndexes[valueVariable]
                        val extensions = if (valueIndex != null) {
                            buildList {
                                input.rows.forEachIndexed { rowIndex, row ->
                                    ve[row[valueIndex]].orEmpty().forEach { entityValue ->
                                        add(RowExtension(rowIndex, listOf(entityValue)))
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
                        return input.extend(added, extensions).reorder(targetVariables)
                    }
                    valueVariable -> {
                        val entityIndex = input.columnIndexes[entityVariable]
                        val extensions = if (entityIndex != null) {
                            buildList {
                                input.rows.forEachIndexed { rowIndex, row ->
                                    ev[row[entityIndex]].orEmpty().forEach { value ->
                                        add(RowExtension(rowIndex, listOf(value)))
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
                        return input.extend(added, extensions).reorder(targetVariables)
                    }
                    else -> throw IllegalStateException("Triple pattern with two variables must introduce one of those variables")
                }
            }
            variables.size == 2 && added.size == 2 -> {
                require (input.variables.intersect(added.toSet()).isEmpty()) {
                    "Triple pattern with two variables cannot introduce a variable that is already bound"
                }
                val extensions = buildList {
                    ev.forEach { (entityValue, values) ->
                        values.forEach { value ->
                            val introducedValues = added.map { variable ->
                                when (variable) {
                                    entityVariable -> entityValue
                                    valueVariable -> value
                                    else -> throw IllegalStateException("Triple pattern can only introduce its entity and value variables")
                                }
                            }
                            input.rows.forEachIndexed { rowIndex, _ ->
                                add(RowExtension(rowIndex, introducedValues))
                            }
                        }
                    }
                }
                return input.extend(added, extensions).reorder(targetVariables)
            }
            else -> throw IllegalStateException("Triple pattern must have 1 or 2 variables, found ${variables.size}")
        }
    }

    private fun validate(
        input: BindingSet,
        added: List<Variable>,
        targetVariables: List<Variable>,
    ): BindingSet {
        require(input.variables.containsAll(added)) {
            "BindingSet must contain all introduced variables for validation"
        }
        when {
            variables.isEmpty() -> {
                val matches = ev[entityConstant]?.contains(valueConstant) == true
                val rows = if (matches) input.rows else emptyList()
                return BindingSet(input.variables, rows)
            }
            variables.size == 1 -> {
                when {
                    entityVariable != null -> {
                        val value = valueConstant!!
                        val es = ve[value]
                        val entityIndex = input.columnIndexes[entityVariable]
                        val rows = if (entityIndex != null) {
                            input.rows.filter { row -> es?.contains(row[entityIndex]) == true }
                        } else if (es.isNullOrEmpty()) {
                            emptyList()
                        } else {
                            input.rows
                        }
                        return BindingSet(input.variables, rows)
                    }
                    valueVariable != null -> {
                        val entityValue = entityConstant!!
                        val vs = ev[entityValue]
                        val valueIndex = input.columnIndexes[valueVariable]
                        val rows = if (valueIndex != null) {
                            input.rows.filter { row -> vs?.contains(row[valueIndex]) == true }
                        } else if (vs.isNullOrEmpty()) {
                            emptyList()
                        } else {
                            input.rows
                        }
                        return BindingSet(input.variables, rows)
                    }
                    else -> throw IllegalStateException("Triple pattern with one variable must have that variable in the entity or value position")
                }
            }
            variables.size == 2 -> {
                val entityIndex = input.columnIndexes[entityVariable]
                val valueIndex = input.columnIndexes[valueVariable]
                val rows = when {
                    entityIndex != null && valueIndex != null -> {
                        input.rows.filter { row -> ev[row[entityIndex]]?.contains(row[valueIndex]) == true }
                    }
                    entityIndex != null -> {
                        input.rows.filter { row -> ev[row[entityIndex]].orEmpty().isNotEmpty() }
                    }
                    valueIndex != null -> {
                        input.rows.filter { row -> ve[row[valueIndex]].orEmpty().isNotEmpty() }
                    }
                    ev.values.any { values -> values.isNotEmpty() } -> input.rows
                    else -> emptyList()
                }
                return BindingSet(input.variables, rows)
            }
            else -> throw IllegalStateException("Triple pattern must have 0, 1 or 2 variables, found ${variables.size}")
        }
    }
}
