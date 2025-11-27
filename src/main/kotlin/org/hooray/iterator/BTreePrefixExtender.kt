package org.hooray.iterator

import me.tonsky.persistent_sorted_set.APersistentSortedSet
import me.tonsky.persistent_sorted_set.Seq
import org.hooray.algo.Extension
import org.hooray.algo.Prefix
import org.hooray.util.IPersistentSortedMap

sealed interface BTreeIndex {
    data class BTreeMap(val map: IPersistentSortedMap) : BTreeIndex
    data class BTreeSet(val set: APersistentSortedSet<Any, Any>) : BTreeIndex
}

@Suppress("UNCHECKED_CAST")
class BTreePrefixExtender(val bTreeIndex: BTreeIndex, participatesInLevel: List<Int>) :
    GenericPrefixExtender(when(bTreeIndex) {
        is BTreeIndex.BTreeMap -> SealedIndex.MapIndex(bTreeIndex.map)
        is BTreeIndex.BTreeSet -> SealedIndex.SetIndex(bTreeIndex.set as Set<Any>)
    }, participatesInLevel) {

    private fun indexFromPrefix(prefix: Prefix): BTreeIndex {
        var currentIndex = bTreeIndex
        val internalPrefix = internalPrefix(prefix)

        for (key in internalPrefix) {
            currentIndex = when (currentIndex) {
                is BTreeIndex.BTreeMap ->
                    when(val newIndex = currentIndex.map[key]) {
                        is IPersistentSortedMap -> BTreeIndex.BTreeMap(newIndex)
                        is APersistentSortedSet<*, *> -> BTreeIndex.BTreeSet(newIndex as APersistentSortedSet<Any, Any>)
                        else -> throw IllegalArgumentException("Unsupported value type in BTreePrefixExtender for key: $key")
                    }
                else -> throw IllegalArgumentException("Cannot index into a BTreeSet with a key")
            }
        }
        return currentIndex
    }

    private fun mapKey(s: Seq) = (s.first() as clojure.lang.MapEntry).`val`()
    private fun setKey(s: Seq) = s.first()

    override fun intersect(prefix: Prefix, extensions: List<Extension>) : List<Extension> {
        var (seq, keyFn) = when (val index = indexFromPrefix(prefix)) {
            is BTreeIndex.BTreeMap -> Pair(index.map.seq() as Seq, ::mapKey)
            is BTreeIndex.BTreeSet -> Pair(index.set.seq() as Seq, ::setKey)
        }
        val result = mutableListOf<Extension>()
        for (ext in extensions) {
            seq = seq.seek(ext)
            if(seq.isEmpty()) break
            if (keyFn(seq) == ext) {
                result.add(ext)
            }
        }
        return result
    }
}