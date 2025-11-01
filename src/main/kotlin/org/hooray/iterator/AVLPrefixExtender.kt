package org.hooray.iterator

import org.hooray.algo.Extension
import org.hooray.algo.Prefix
import org.hooray.algo.PrefixExtender

interface AVLMap : Map<Any, Any>
interface AVLSet : Set<Any>

sealed interface AVLIndex {
    data class AVLMapIndex(val map: AVLMap): AVLIndex
    data class AVLSetIndex(val set: AVLSet): AVLIndex
}

class AVLLeapfrogIndex(val index: AVLIndex, val participatesInLevel: List<Int>) : PrefixExtender {

    private fun internalPrefix(prefix: Prefix): Prefix {
        val newPrefix = mutableListOf<Any>()
        for (level in participatesInLevel) {
            if (level < prefix.size) {
                newPrefix.add(prefix[level])
            }
        }
        return newPrefix
    }

    private fun indexFromPrefix(prefix: Prefix): AVLIndex {
        var currentIndex = index
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

    override fun count(prefix: Prefix) =
        when (val index= indexFromPrefix(prefix)) {
            is AVLIndex.AVLMapIndex -> index.map.size
            is AVLIndex.AVLSetIndex -> index.set.size
        }

    override fun propose(prefix: Prefix)  =
        when (val index= indexFromPrefix(prefix)) {
            is AVLIndex.AVLMapIndex -> index.map.keys.toList()
            is AVLIndex.AVLSetIndex -> index.set.toList()
        }

    // This is WCO as we start with the smallest extension list so
    // extensions.size <= map.size or set.size
    override fun extend(prefix: Prefix, extensions: List<Extension>) =
        when (val index = indexFromPrefix(prefix)) {
            is AVLIndex.AVLMapIndex -> extensions.filter { ext -> index.map.containsKey(ext) }
            is AVLIndex.AVLSetIndex -> extensions.filter { ext -> index.set.contains(ext) }
        }
}