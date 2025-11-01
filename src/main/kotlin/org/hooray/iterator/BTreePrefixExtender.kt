package org.hooray.iterator

import me.tonsky.persistent_sorted_set.APersistentSortedSet
import me.tonsky.persistent_sorted_set.Seq
import org.hooray.algo.Extension
import org.hooray.algo.Prefix
import org.hooray.algo.PrefixExtender
import org.hooray.util.IPersistentSortedMap

sealed interface SealedBTreeIndex {
    data class BTreeMap(val map: IPersistentSortedMap) : SealedBTreeIndex
    data class BTreeSet(val set: APersistentSortedSet<Any, Any>) : SealedBTreeIndex
}

@Suppress("UNCHECKED_CAST")
class BTreePrefixExtender(val bTreeIndex: SealedBTreeIndex, participatesInLevel: List<Int>) :
    GenericPrefixExtender(when(bTreeIndex) {
        is SealedBTreeIndex.BTreeMap -> SealedIndex.MapIndex(bTreeIndex.map)
        is SealedBTreeIndex.BTreeSet -> SealedIndex.SetIndex(bTreeIndex.set as Set<Any>)
    }, participatesInLevel) {

    private fun indexFromPrefix(prefix: Prefix): SealedBTreeIndex {
        var currentIndex = bTreeIndex
        val internalPrefix = internalPrefix(prefix)

        for (key in internalPrefix) {
            currentIndex = when (currentIndex) {
                is SealedBTreeIndex.BTreeMap ->
                    when(val newIndex = currentIndex.map[key]) {
                        is IPersistentSortedMap -> SealedBTreeIndex.BTreeMap(newIndex)
                        is APersistentSortedSet<*, *> -> SealedBTreeIndex.BTreeSet(newIndex as APersistentSortedSet<Any, Any>)
                        else -> throw IllegalArgumentException("Unsupported value type in BTreePrefixExtender for key: $key")
                    }
                else -> throw IllegalArgumentException("Cannot index into a BTreeSet with a key")
            }
        }
        return currentIndex
    }

    override fun extend(prefix: Prefix, extensions: List<Extension>) : List<Extension> {
        var seq = when (val index = indexFromPrefix(prefix)) {
            is SealedBTreeIndex.BTreeMap -> index.map.seq() as Seq
            is SealedBTreeIndex.BTreeSet -> index.set.seq() as Seq
        }
        val result = mutableListOf<Extension>()
        for (ext in extensions) {
            seq = seq.seek(ext)
            if(seq.isEmpty()) break
            if (seq.first() == ext) {
                result.add(ext)
            }
        }
        return result
    }
}