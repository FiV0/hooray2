package org.hooray.engine

data class Triple(
    val entity: Any,
    val attribute: Any,
    val value: Any,
)

class TripleIndex private constructor(
    private val triples: List<Triple>,
) {
    fun matching(
        layout: List<Any>,
        row: BindingRow,
        entity: PatternValue,
        attribute: PatternValue,
        value: PatternValue,
    ): List<Triple> {
        return triples.filter { triple ->
            val seenVariables = mutableMapOf<Any, Any>()
            matchesSlot(entity, triple.entity, layout, row, seenVariables) &&
                matchesSlot(attribute, triple.attribute, layout, row, seenVariables) &&
                matchesSlot(value, triple.value, layout, row, seenVariables)
        }
    }

    private fun matchesSlot(
        slot: PatternValue,
        tripleValue: Any,
        layout: List<Any>,
        row: BindingRow,
        seenVariables: MutableMap<Any, Any>,
    ): Boolean {
        return when (slot) {
            is PatternValue.Constant -> slot.value == tripleValue
            is PatternValue.Variable -> {
                val boundIndex = layout.indexOf(slot.name)
                if (boundIndex >= 0) {
                    row[boundIndex] == tripleValue
                } else {
                    val seen = seenVariables.putIfAbsent(slot.name, tripleValue)
                    seen == null || seen == tripleValue
                }
            }
        }
    }

    companion object {
        @JvmStatic
        fun of(vararg triples: Triple): TripleIndex = TripleIndex(triples.toList())

        @JvmStatic
        fun fromEav(eav: Map<*, *>): TripleIndex {
            val triples = mutableListOf<Triple>()
            for ((entity, attributesAny) in eav) {
                val attributes = attributesAny as? Map<*, *> ?: continue
                for ((attribute, valuesAny) in attributes) {
                    val values = valuesAny as? Iterable<*> ?: continue
                    for (value in values) {
                        if (entity != null && attribute != null && value != null) {
                            triples += Triple(entity, attribute, value)
                        }
                    }
                }
            }
            return TripleIndex(triples)
        }
    }
}

sealed interface PatternValue {
    data class Variable(val name: Any) : PatternValue
    data class Constant(val value: Any) : PatternValue
}

class TriplePattern(
    private val index: TripleIndex,
    private val entity: PatternValue,
    private val attribute: PatternValue,
    private val value: PatternValue,
) : ExecPattern {
    override val variables: Set<Any> = listOf(entity, attribute, value)
        .mapNotNull { slot -> (slot as? PatternValue.Variable)?.name }
        .toSet()

    override val proposerEligible: Boolean = true

    override fun count(input: BindingSet, introduces: List<Any>): List<Int> {
        require(variables.containsAll(introduces)) {
            "Triple pattern cannot introduce variables it does not contain"
        }

        return input.rows.map { row ->
            index.matching(input.variables, row, entity, attribute, value)
                .map { triple -> introducedValues(triple, introduces) }
                .toSet()
                .size
        }
    }

    override fun propose(
        input: BindingSet,
        introduces: List<Any>,
        targetVariables: List<Any>,
    ): BindingSet {
        require(variables.containsAll(introduces)) {
            "Triple pattern cannot introduce variables it does not contain"
        }

        val extensions = mutableListOf<RowExtension>()
        input.rows.forEachIndexed { rowIndex, row ->
            val seenIntroductions = mutableSetOf<BindingRow>()
            for (triple in index.matching(input.variables, row, entity, attribute, value)) {
                val introducedValues = introducedValues(triple, introduces)
                if (seenIntroductions.add(introducedValues)) {
                    extensions += RowExtension(rowIndex, introducedValues)
                }
            }
        }

        return input.extend(introduces, extensions).reorder(targetVariables)
    }

    override fun validate(input: BindingSet, targetVariables: List<Any>): BindingSet {
        val rows = input.rows.filter { row ->
            index.matching(input.variables, row, entity, attribute, value).isNotEmpty()
        }
        return BindingSet(input.variables, rows).reorder(targetVariables)
    }

    private fun introducedValues(triple: Triple, introduces: List<Any>): BindingRow {
        val valuesByVariable = mutableMapOf<Any, Any>()
        recordVariableValue(entity, triple.entity, valuesByVariable)
        recordVariableValue(attribute, triple.attribute, valuesByVariable)
        recordVariableValue(value, triple.value, valuesByVariable)
        return introduces.map { variable ->
            valuesByVariable.getValue(variable)
        }
    }

    private fun recordVariableValue(
        slot: PatternValue,
        tripleValue: Any,
        valuesByVariable: MutableMap<Any, Any>,
    ) {
        if (slot is PatternValue.Variable) {
            valuesByVariable[slot.name] = tripleValue
        }
    }
}
