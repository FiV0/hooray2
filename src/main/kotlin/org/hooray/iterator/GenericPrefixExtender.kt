package org.hooray.iterator

import clojure.lang.Var
import org.hooray.algo.Extension
import org.hooray.algo.Prefix
import org.hooray.algo.PrefixExtender
import org.hooray.engine.BindingRow
import org.hooray.engine.BindingSet
import org.hooray.engine.ExecPattern
import org.hooray.engine.Proposal
import org.hooray.engine.RowExtension
import org.hooray.engine.Variable
import org.hooray.engine.updateProposals
import java.lang.IllegalStateException

sealed interface SealedIndex {
    data class MapIndex(val map: Map<Any, Any>) : SealedIndex
    data class SetIndex(val set: Set<Any>) : SealedIndex
}

@Suppress("UNCHECKED_CAST")
open class GenericPrefixExtender(val index: SealedIndex, val participatesInLevel: List<Int>) : PrefixExtender, LevelParticipation, ExecPattern {
    override val idx: Int
        get() = TODO("Not yet implemented")

    override val variables: Set<Variable>
        get() = TODO("Not yet implemented")


    private fun relevantIntroduces(introduces: List<Variable>): List<Variable> {
        return introduces.filter { it in variables }
    }

    private fun prefixIndexes(inputVariables: List<Variable>) : List<Int> {
        val res = mutableListOf<Int>()
        for ((index, variable) in inputVariables.withIndex()) {
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

    private fun rowPresent(row: BindingRow, indices: List<Int>): Boolean {
        return when (val index = indexFromPrefix(row.dropLast(1), indices.dropLast(1))) {
            null -> false
            is SealedIndex.MapIndex -> index.map.containsKey(row[indices.last()])
            is SealedIndex.SetIndex -> index.set.contains(row[indices.last()])
        }
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
        introduces: List<Variable>,
        proposals: List<Proposal>,
    ): List<Proposal> {
        val relevantIntroduces = relevantIntroduces(introduces)
        checkIntroduces(relevantIntroduces)

        val prefixIndexes = prefixIndexes(input.variables)

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
        introduces: List<Variable>,
        targetVariables: List<Variable>,
    ): BindingSet {

        val relevantIntroduces = relevantIntroduces(introduces)
        checkIntroduces(relevantIntroduces)

        val prefixIndexes = prefixIndexes(input.variables)

        val extensions = mutableListOf<RowExtension>()

        input.rows.mapIndexed { index,  prefix ->
            when (val trie = indexFromPrefix(prefix, prefixIndexes)) {
                null -> {}
                is SealedIndex.MapIndex ->  when (relevantIntroduces.size) {
                    1 -> trie.map.entries.forEach { (key, _) ->
                        extensions.add(RowExtension(index, listOf(key)))
                    }
                    2 -> trie.map.entries.forEach { (key, value) ->
                        when (value) {
                            is Map<*, *> -> value.keys.forEach { subKey ->
                                extensions.add(RowExtension(index, listOf(key, subKey) as BindingRow))
                            }
                            is Set<*> -> value.forEach { subKey ->
                                extensions.add(RowExtension(index, listOf(key, subKey) as BindingRow))
                            }
                            else -> throw IllegalArgumentException("Unsupported value type in HashMap for key: $value")
                        }
                    }


                    else -> throw IllegalArgumentException("Triple pattern can only introduce one or two variables at a time")
                }
                is SealedIndex.SetIndex -> when (relevantIntroduces.size) {
                    1 -> trie.set.forEach { key ->
                        extensions.add(RowExtension(index, listOf(key)))
                    }
                    2 -> throw IllegalStateException("Triple pattern reached set where map was expected")
                    else -> throw IllegalArgumentException("Triple pattern can only introduce one or two variables at a time")
                }
            }
        }

        return input.extend(relevantIntroduces, extensions).reorder(targetVariables)
    }

    override fun validate(input: BindingSet, introduces: List<Variable>, targetVariables: List<Variable>): BindingSet {
        val relevantIntroduces = relevantIntroduces(introduces)
        checkIntroduces(relevantIntroduces)

        val prefixIndexes = prefixIndexes(input.variables + introduces)

        val filteredRows = input.rows.filter { row -> rowPresent(row, prefixIndexes) }

        return BindingSet(input.variables, filteredRows).reorder(targetVariables)
    }

    protected fun internalPrefix(prefix: Prefix): Prefix {
        val newPrefix = mutableListOf<Any>()
        for (level in participatesInLevel) {
            if (level < prefix.size) {
                newPrefix.add(prefix[level])
            }
        }
        return newPrefix
    }

    private fun indexFromPrefix(prefix: Prefix): SealedIndex? {
        var currentIndex : SealedIndex? = index
        val internalPrefix = internalPrefix(prefix)

        for (key in internalPrefix) {
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

    override fun count(prefix: Prefix) =
        when (val index= indexFromPrefix(prefix)) {
            null -> 0
            is SealedIndex.MapIndex -> index.map.size
            is SealedIndex.SetIndex -> index.set.size
        }

    override fun propose(prefix: Prefix)  =
        when (val index= indexFromPrefix(prefix)) {
            null -> emptyList()
            // we don't just use keys, as that does not preserve order in case of sorted collections
            is SealedIndex.MapIndex -> index.map.iterator().asSequence().map { it.key }.toList()
            is SealedIndex.SetIndex -> index.set.toList()
        }

    // This is WCO as we start with the smallest extension list so
    // extensions.size <= map.size or set.size
    override fun intersect(prefix: Prefix, extensions: List<Extension>) =
        when (val index = indexFromPrefix(prefix)) {
            null -> emptyList()
            is SealedIndex.MapIndex -> extensions.filter { ext -> index.map.containsKey(ext) }
            is SealedIndex.SetIndex -> extensions.filter { ext -> index.set.contains(ext) }
        }

    override fun participatesInLevel(level: Int) = participatesInLevel.contains(level)
}
