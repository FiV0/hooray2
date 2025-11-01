package org.hooray.iterator

import clojure.lang.ISeq
import org.hooray.algo.Extension
import org.hooray.algo.Prefix

interface AVLMap : ISeq, Map<Any, Any>
interface AVLSet : ISeq,  Set<Any>

interface AVLMapSeq: ISeq
interface AVLSetSeq: ISeq

sealed interface AVLIndex {
    data class AVLMapIndex(val map: AVLMap): AVLIndex
    data class AVLSetIndex(val set: AVLSet): AVLIndex
}

class AVLLeapfrogIndex(val avlIndex: AVLIndex, participatesInLevel: List<Int>) : GenericPrefixExtender(
    when(avlIndex) {
        is AVLIndex.AVLMapIndex -> SealedIndex.MapIndex(avlIndex.map)
        is AVLIndex.AVLSetIndex -> SealedIndex.SetIndex(avlIndex.set)
    }, participatesInLevel) {

    private fun indexFromPrefix(prefix: Prefix): AVLIndex {
        var currentIndex = avlIndex
        val internalPrefix = internalPrefix(prefix)

        for (key in internalPrefix) {
            currentIndex = when (currentIndex) {
                is AVLIndex.AVLMapIndex  ->
                    when(val newIndex = currentIndex.map[key]) {
                        is AVLMap -> AVLIndex.AVLMapIndex(newIndex)
                        is AVLSet -> AVLIndex.AVLSetIndex(newIndex)
                        else -> throw IllegalArgumentException("Unsupported value type in AVLMap for key: $key")
                    }
                else -> throw IllegalArgumentException("Cannot index into a BTreeSet with a key")
            }
        }
        return currentIndex
    }


    // This is WCO as we start with the smallest extension list so
    // extensions.size <= map.size or set.size
    override fun extend(prefix: Prefix, extensions: List<Extension>): List<Extension> {
        var seq = when (val index = indexFromPrefix(prefix)) {
            is AVLIndex.AVLMapIndex -> index.map.seq() as AVLMapSeq
            is AVLIndex.AVLSetIndex -> index.set.seq() as AVLSetSeq
        }
        val result = mutableListOf<Extension>()
        for (ext in extensions) {
            TODO()
//            seq = seq.seek(ext)
//            if(seq.isEmpty()) break
//            if (seq.first() == ext) {
//                result.add(ext)
//            }
        }
        return result
    }
}