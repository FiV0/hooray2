package org.hooray.iterator

import org.hooray.algo.Extension
import org.hooray.algo.Prefix
import org.hooray.algo.PrefixExtender

sealed interface SealedHashIndex {
    data class HashMapIndex(val map: HashMap<Any, Any>) : SealedHashIndex
    data class HashSetIndex(val set: HashSet<Any>) : SealedHashIndex
}

@Suppress("UNCHECKED_CAST")
class HashPrefixExtender(val index: SealedHashIndex, val participatesInLevel: List<Int>) : PrefixExtender {

    private fun internalPrefix(prefix: Prefix): Prefix {
        val newPrefix = mutableListOf<Any>()
        for (level in participatesInLevel) {
            if (level < prefix.size) {
                newPrefix.add(prefix[level])
            }
        }
        return newPrefix
    }

    private fun indexFromPrefix(prefix: Prefix): SealedHashIndex {
        var currentIndex = index
        val internalPrefix = internalPrefix(prefix)

        for (key in internalPrefix) {
            currentIndex = when (currentIndex) {
                is SealedHashIndex.HashMapIndex ->
                    when(val newIndex = currentIndex.map[key]) {
                        is HashMap<*, *> -> SealedHashIndex.HashMapIndex(newIndex as HashMap<Any, Any>)
                        is HashSet<*> -> SealedHashIndex.HashSetIndex(newIndex as HashSet<Any> )
                        else -> throw IllegalArgumentException("Unsupported value type in HashMap for key: $key")
                    }
                else -> throw IllegalArgumentException("Cannot index into a HashSet with a key")
            }
        }
        return currentIndex
    }

    override fun count(prefix: Prefix) =
        when (val index= indexFromPrefix(prefix)) {
            is SealedHashIndex.HashMapIndex -> index.map.size
            is SealedHashIndex.HashSetIndex -> index.set.size
        }

    override fun propose(prefix: Prefix)  =
        when (val index= indexFromPrefix(prefix)) {
            is SealedHashIndex.HashMapIndex -> index.map.keys.toList()
            is SealedHashIndex.HashSetIndex -> index.set.toList()
        }

    // This is WCO as we start with the smallest extension list so
    // extensions.size <= map.size or set.size
    override fun extend(prefix: Prefix, extensions: List<Extension>) =
        when (val index = indexFromPrefix(prefix)) {
            is SealedHashIndex.HashMapIndex -> extensions.filter { ext -> index.map.containsKey(ext) }
            is SealedHashIndex.HashSetIndex -> extensions.filter { ext -> index.set.contains(ext) }
        }
}
