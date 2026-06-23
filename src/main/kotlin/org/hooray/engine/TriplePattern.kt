package org.hooray.engine

sealed interface PatternValue {
    data class Variable(val name: Any) : PatternValue
    data class Constant(val value: Any) : PatternValue
}

class TriplePattern(
    private val eav: Map<*, *>,
    private val aev: Map<*, *>,
    private val ave: Map<*, *>,
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
            matchingIntroductions(input.variables, row, introduces).size
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
            for (introducedValues in matchingIntroductions(input.variables, row, introduces)) {
                extensions += RowExtension(rowIndex, introducedValues)
            }
        }

        return input.extend(introduces, extensions).reorder(targetVariables)
    }

    override fun validate(input: BindingSet): BindingSet {
        val rows = input.rows.filter { row ->
            matchingIntroductions(input.variables, row, emptyList()).isNotEmpty()
        }
        return BindingSet(input.variables, rows)
    }

    private fun matchingIntroductions(
        layout: List<Any>,
        row: BindingRow,
        introduces: List<Any>,
    ): List<BindingRow> {
        val resolvedEntity = resolve(entity, layout, row)
        val resolvedAttribute = resolve(attribute, layout, row)
        val resolvedValue = resolve(value, layout, row)
        val attributeValue = when (resolvedAttribute) {
            is ResolvedSlot.Bound -> resolvedAttribute.value
            is ResolvedSlot.Unbound -> throw IllegalArgumentException(
                "Triple pattern cannot use an unbound attribute variable",
            )
        }

        val result = mutableListOf<BindingRow>()
        val seenIntroductions = mutableSetOf<BindingRow>()
        for (candidate in candidatePairs(resolvedEntity, attributeValue, resolvedValue)) {
            val valuesByVariable = mutableMapOf<Any, Any>()
            if (!recordMatchingValue(entity, candidate.entity, layout, row, valuesByVariable)) continue
            if (!recordMatchingValue(attribute, attributeValue, layout, row, valuesByVariable)) continue
            if (!recordMatchingValue(value, candidate.value, layout, row, valuesByVariable)) continue

            val introducedValues = introduces.map { variable ->
                valuesByVariable.getValue(variable)
            }
            if (seenIntroductions.add(introducedValues)) {
                result += introducedValues
            }
        }
        return result
    }

    private fun candidatePairs(
        entity: ResolvedSlot,
        attribute: Any,
        value: ResolvedSlot,
    ): List<CandidatePair> {
        return when {
            entity is ResolvedSlot.Bound && value is ResolvedSlot.Bound ->
                if (containsEntityAttributeValue(entity.value, attribute, value.value)) {
                    listOf(CandidatePair(entity.value, value.value))
                } else {
                    emptyList()
                }

            entity is ResolvedSlot.Bound ->
                valuesForEntityAttribute(entity.value, attribute)
                    .map { candidateValue -> CandidatePair(entity.value, candidateValue) }

            value is ResolvedSlot.Bound ->
                entitiesForAttributeValue(attribute, value.value)
                    .map { candidateEntity -> CandidatePair(candidateEntity, value.value) }

            else -> pairsForAttribute(attribute)
        }
    }

    private fun containsEntityAttributeValue(entity: Any, attribute: Any, value: Any): Boolean {
        return valuesForEntityAttribute(entity, attribute).any { it == value }
    }

    private fun valuesForEntityAttribute(entity: Any, attribute: Any): List<Any> {
        val attributes = eav[entity] as? Map<*, *> ?: return emptyList()
        val values = attributes[attribute] as? Iterable<*> ?: return emptyList()
        return values.filterNotNull()
    }

    private fun entitiesForAttributeValue(attribute: Any, value: Any): List<Any> {
        val values = ave[attribute] as? Map<*, *> ?: return emptyList()
        val entities = values[value] as? Iterable<*> ?: return emptyList()
        return entities.filterNotNull()
    }

    private fun pairsForAttribute(attribute: Any): List<CandidatePair> {
        val entities = aev[attribute] as? Map<*, *> ?: return emptyList()
        val pairs = mutableListOf<CandidatePair>()
        for ((entity, valuesAny) in entities) {
            if (entity == null) continue
            val values = valuesAny as? Iterable<*> ?: continue
            for (value in values) {
                if (value != null) {
                    pairs += CandidatePair(entity, value)
                }
            }
        }
        return pairs
    }

    private fun resolve(slot: PatternValue, layout: List<Any>, row: BindingRow): ResolvedSlot {
        return when (slot) {
            is PatternValue.Constant -> ResolvedSlot.Bound(slot.value)
            is PatternValue.Variable -> {
                val boundIndex = layout.indexOf(slot.name)
                if (boundIndex >= 0) {
                    ResolvedSlot.Bound(row[boundIndex])
                } else {
                    ResolvedSlot.Unbound(slot.name)
                }
            }
        }
    }

    private fun recordMatchingValue(
        slot: PatternValue,
        tripleValue: Any,
        layout: List<Any>,
        row: BindingRow,
        valuesByVariable: MutableMap<Any, Any>,
    ): Boolean {
        return when (slot) {
            is PatternValue.Constant -> slot.value == tripleValue
            is PatternValue.Variable -> {
                val boundIndex = layout.indexOf(slot.name)
                if (boundIndex >= 0 && row[boundIndex] != tripleValue) {
                    return false
                }
                val seen = valuesByVariable.putIfAbsent(slot.name, tripleValue)
                seen == null || seen == tripleValue
            }
        }
    }

    private data class CandidatePair(
        val entity: Any,
        val value: Any,
    )

    private sealed interface ResolvedSlot {
        data class Bound(val value: Any) : ResolvedSlot
        data class Unbound(val variable: Any) : ResolvedSlot
    }
}
