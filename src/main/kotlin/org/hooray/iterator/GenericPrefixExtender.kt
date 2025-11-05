package org.hooray.iterator

import org.hooray.algo.Extension
import org.hooray.algo.Prefix
import org.hooray.algo.PrefixExtender

sealed interface SealedIndex {
    data class MapIndex(val map: Map<Any, Any>) : SealedIndex
    data class SetIndex(val set: Set<Any>) : SealedIndex
}

@Suppress("UNCHECKED_CAST")
open class GenericPrefixExtender(val index: SealedIndex, val participatesInLevel: List<Int>) : PrefixExtender, LevelParticipation {

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

    override open fun count(prefix: Prefix) =
        when (val index= indexFromPrefix(prefix)) {
            null -> 0
            is SealedIndex.MapIndex -> index.map.size
            is SealedIndex.SetIndex -> index.set.size
        }

    override open fun propose(prefix: Prefix)  =
        when (val index= indexFromPrefix(prefix)) {
            null -> emptyList()
            // we don't just use .keys as that does not preserve order in case of sorted collections
            is SealedIndex.MapIndex -> index.map.iterator().asSequence().map { it.key }.toList()
            is SealedIndex.SetIndex -> index.set.toList()
        }

    // This is WCO as we start with the smallest extension list so
    // extensions.size <= map.size or set.size
    override open fun extend(prefix: Prefix, extensions: List<Extension>) =
        when (val index = indexFromPrefix(prefix)) {
            null -> emptyList()
            is SealedIndex.MapIndex -> extensions.filter { ext -> index.map.containsKey(ext) }
            is SealedIndex.SetIndex -> extensions.filter { ext -> index.set.contains(ext) }
        }

    override fun participatesInLevel(level: Int) = participatesInLevel.contains(level)
}
