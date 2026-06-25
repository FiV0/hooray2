package org.hooray.engine

import java.lang.IllegalStateException
import org.hooray.algo.Prefix
import org.hooray.iterator.SealedIndex

sealed interface PatternValue {
    data class Variable(val name: Any) : PatternValue
    data class Constant(val value: Any) : PatternValue
}

@Suppress("UNCHECKED_CAST")
class TriplePattern(
    override val idx: Int,
    override val variables: Set<Any>,
    private val index: SealedIndex,
) : ExecPattern {

    private fun relevantIntroduces(introduces: List<Any>): List<Any> {
        return introduces.filter { it in variables }
    }

    private fun prefixIndexes(input: BindingSet) : List<Int> {
        val res = mutableListOf<Int>()
        for ((index, variable) in input.variables.withIndex()) {
            if (variable in variables) {
                res.add(index)
            }
        }
        return res
    }

    private fun indexFromPrefix(prefix: Prefix, prefixIndexes: List<Int>): SealedIndex? {
        var currentIndex : SealedIndex? = index

        for (idx in prefixIndexes) {
            val key = prefix[idx]
            currentIndex = when (currentIndex) {
                is SealedIndex.MapIndex ->
                    when(val newIndex = currentIndex.map[key]) {
                        is Map<*, *> -> SealedIndex.MapIndex(newIndex as Map<Any, Any>)
                        is Set<*> -> SealedIndex.SetIndex(newIndex as Set<Any> )
                        null -> null
                        else -> throw IllegalArgumentException("Unsupported value type in HashMap for key: $key")
                    }
                else -> throw IllegalArgumentException("Cannot index into a HashSet with a key")
            }
        }
        return currentIndex
    }

    private fun checkIntroduces(introduces: List<Any>) {
        require(introduces.size == 1 || introduces.size == 2) {
            "Triple pattern can only introduce one or two variables at a time"
        }

        require(variables.containsAll(introduces)) {
            "Triple pattern cannot introduce variables it does not contain"
        }
    }

    override fun count(
        input: BindingSet,
        introduces: List<Any>,
        proposals: List<Proposal>,
    ): List<Proposal> {
        val relevantIntroduces = relevantIntroduces(introduces)
        checkIntroduces(relevantIntroduces)

        val prefixIndexes = prefixIndexes(input)

        val counts = input.rows.map { prefix ->
            when (val index = indexFromPrefix(prefix, prefixIndexes)) {
                null -> 0
                is SealedIndex.MapIndex ->  when (relevantIntroduces.size) {
                    1 -> index.map.size
                    2 -> index.map.values.sumOf { value ->
                        when (value) {
                            is Map<*, *> -> value.size
                            is Set<*> -> value.size
                            else -> throw IllegalArgumentException("Unsupported value type in HashMap for key: $value")
                        }
                    }
                    else -> throw IllegalArgumentException("Triple pattern can only introduce one or two variables at a time")
                }
                is SealedIndex.SetIndex -> when (relevantIntroduces.size) {
                    1 -> index.set.size
                    2 -> throw IllegalStateException("Triple pattern reached set where map was expected")
                    else -> throw IllegalArgumentException("Triple pattern can only introduce one or two variables at a time")
                }
            }
        }
        return updateProposals(idx, proposals, counts)
    }

    override fun propose(
        input: BindingSet,
        introduces: List<Any>,
        targetVariables: List<Any>,
    ): BindingSet {
        val relevantIntroduces = relevantIntroduces(introduces)
        checkIntroduces(relevantIntroduces)

        val prefixIndexes = prefixIndexes(input)

        val extensions = mutableListOf<RowExtension>()
        input.rows.forEachIndexed { rowIndex, prefix ->
            when (val index = indexFromPrefix(prefix, prefixIndexes)) {
                null -> {}
                is SealedIndex.MapIndex ->  when (relevantIntroduces.size) {
                    1 -> index.map.forEach { (key, value) ->
                        val introducedValues = listOf(key)
                        extensions += RowExtension(rowIndex, introducedValues)
                    }
                    2 -> index.map.values.sumOf { value ->
                        when (value) {
                            is Map<*, *> -> value.size
                            is Set<*> -> value.size
                            else -> throw IllegalArgumentException("Unsupported value type in HashMap for key: $value")
                        }
                    }
                    else -> throw IllegalArgumentException("Triple pattern can only introduce one or two variables at a time")
                }
                is SealedIndex.SetIndex -> when (relevantIntroduces.size) {
                    1 -> index.set.size
                    2 -> throw IllegalStateException("Triple pattern reached set where map was expected")
                    else -> throw IllegalArgumentException("Triple pattern can only introduce one or two variables at a time")
                }
            }


            val index = indexFromPrefix(row, prefixIndexes)
            when (index) {
                null -> {}
                is SealedIndex.SetIndex -> index.set.forEach { value ->
                    val introducedValues = listOf(value)
                    extensions += RowExtension(rowIndex, introducedValues)
                }
            }
        }
        // TODO reorder necessary?
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
