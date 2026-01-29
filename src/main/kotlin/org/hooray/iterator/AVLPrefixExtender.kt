package org.hooray.iterator

import org.hooray.algo.Extension
import org.hooray.algo.Prefix
import clojure.data.avl.AVLMap
import clojure.data.avl.AVLSet
import clojure.data.avl.IAVLSeq
import org.hooray.UniversalComparator

sealed interface AVLIndex {
    data class AVLMapIndex(val map: AVLMap): AVLIndex
    data class AVLSetIndex(val set: AVLSet): AVLIndex
}

@Suppress("UNCHECKED_CAST")
class AVLPrefixExtender(val avlIndex: AVLIndex, participatesInLevel: List<Int>) : GenericPrefixExtender(
    when(avlIndex) {
        is AVLIndex.AVLMapIndex -> SealedIndex.MapIndex(avlIndex.map as Map<Any, Any>)
        is AVLIndex.AVLSetIndex -> SealedIndex.SetIndex(avlIndex.set as Set<Any>)
    }, participatesInLevel) {

    private fun indexFromPrefix(prefix: Prefix): AVLIndex? {
        var currentIndex: AVLIndex? = avlIndex
        val internalPrefix = internalPrefix(prefix)

        for (key in internalPrefix) {
            currentIndex = when (currentIndex) {
                is AVLIndex.AVLMapIndex  ->
                    when(val newIndex = currentIndex.map[key]) {
                        is AVLMap -> AVLIndex.AVLMapIndex(newIndex)
                        is AVLSet -> AVLIndex.AVLSetIndex(newIndex)
                        null -> null
                        else -> throw IllegalArgumentException("Unsupported value type in AVLMap for key: $key")
                    }
                else -> throw IllegalArgumentException("Cannot index into an AVLSet with a key")
            }
        }
        return currentIndex
    }

    private fun mapKey(s: IAVLSeq) = (s.first() as clojure.lang.MapEntry).`val`()
    private fun setKey(s: IAVLSeq) = s.first()

    override fun intersect(prefix: Prefix, extensions: List<Extension>): List<Extension> {
        var (seq, keyFn) = when (val index = indexFromPrefix(prefix)) {
            is AVLIndex.AVLMapIndex -> Pair(index.map.seq() as IAVLSeq, ::mapKey)
            is AVLIndex.AVLSetIndex -> Pair(index.set.seq() as IAVLSeq, ::setKey)
            null -> return emptyList()
        }
        // TODO should we use mutableSetOf here, as we are doing Datalog?
        val result = mutableListOf<Extension>()
        for (ext in extensions) {
            seq = seq.seek(ext)
            if(seq.isEmpty()) break
            if (UniversalComparator.compare(keyFn(seq), ext) == 0) {
                result.add(ext)
            }
        }
        return result
    }
}