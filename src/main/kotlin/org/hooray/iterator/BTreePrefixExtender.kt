package org.hooray.iterator

import me.tonsky.persistent_sorted_set.APersistentSortedSet
import org.hooray.algo.Extension
import org.hooray.algo.Prefix
import org.hooray.algo.PrefixExtender
import org.hooray.util.IPersistentSortedMap

sealed interface SealedIndex {
    data class BTreeMap(val map: IPersistentSortedMap) : SealedIndex
    data class BTreeSet(val set: APersistentSortedSet<Any, Any>) : SealedIndex
}

@Suppress("UNCHECKED_CAST")
class BTreePrefixExtender(val index: SealedIndex, val participatesInLevel: List<Int>) : PrefixExtender {

    private fun internalPrefix(prefix: Prefix): Prefix {
        val newPrefix = mutableListOf<Any>()
        for (level in participatesInLevel) {
            if (level < prefix.size) {
                newPrefix.add(prefix[level])
            }
        }
        return newPrefix
    }

    private fun indexFromPrefix(prefix: Prefix): SealedIndex {
        var currentIndex = index
        val internalPrefix = internalPrefix(prefix)

        for (key in internalPrefix) {
            currentIndex = when (currentIndex) {
                is SealedIndex.BTreeMap ->
                    when(val newIndex = currentIndex.map.valAt(key)) {
                        is IPersistentSortedMap -> SealedIndex.BTreeMap(newIndex)
                        is APersistentSortedSet<*, *> -> SealedIndex.BTreeSet(newIndex as APersistentSortedSet<Any, Any>)
                        else -> throw IllegalArgumentException("Unsupported value type in BTreeMap for key: $key")
                    }
                else -> throw IllegalArgumentException("Cannot index into a BTreeSet with a key")
            }
        }
        return currentIndex
    }

    override fun count(prefix: Prefix) =
        when (val index= indexFromPrefix(prefix)) {
            is SealedIndex.BTreeMap -> index.map.count()
            is SealedIndex.BTreeSet -> index.set.size
        }

    override fun propose(prefix: Prefix)  =
        when (val index= indexFromPrefix(prefix)) {
            is SealedIndex.BTreeMap -> index.map.iterator().asSequence().toList() as List<Extension>
            is SealedIndex.BTreeSet -> index.set.iterator().asSequence().toList() as List<Extension>
        }

    // This is WCO (up to O(log(n))) as we start with the smallest extension list so
    // extensions.size <= bTreeMap.size or bTreeSet.size
    // Potentially we could optimize further by seeking on the BTree/Set instead of doing contains
    override fun extend(prefix: Prefix, extensions: List<Extension>) =
        when (val index = indexFromPrefix(prefix)) {
            is SealedIndex.BTreeMap -> extensions.filter { ext -> index.map.containsKey(ext) }
            is SealedIndex.BTreeSet -> extensions.filter { ext -> index.set.contains(ext) }
        }
}