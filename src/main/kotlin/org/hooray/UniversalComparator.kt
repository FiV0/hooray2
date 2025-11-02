package org.hooray

import clojure.lang.Keyword
import clojure.lang.PersistentVector
import clojure.lang.Symbol
import clojure.lang.Util

object UniversalComparator : Comparator<Any> {
    override fun compare(a: Any?, b: Any?): Int {
        val aType = getTypeDiscriminator(a)
        val bType = getTypeDiscriminator(b)
        val typeCmp = aType.compareTo(bType)
        if (typeCmp != 0) return typeCmp

        return when (a) {
            null -> 0
            is Number, is Symbol, is Keyword, is PersistentVector -> Util.compare(a, b)
            is String-> a .compareTo(b as String)
            is List<*> -> compareListsStructurally(a, b as List<*>)
            is Map<*, *> -> compareMapsStructurally(a, b as Map<*, *>)
            else -> throw IllegalArgumentException("Unsupported type: ${a::class}")
        }
    }

    private fun getTypeDiscriminator(obj: Any?): Int = when (obj) {
        null -> 0
        is Number -> 1
        is String -> 2
        is Symbol -> 3
        is Keyword -> 4
        is PersistentVector -> 5
        is List<*> -> 6
        is Map<*, *> -> 7
        else -> throw IllegalArgumentException("Unsupported type: ${obj::class}")
    }

    private fun compareListsStructurally(a: List<*>, b: List<*>): Int {
        if (a.size != b.size) return a.size.compareTo(b.size)
        for (i in 0 until a.size) {
            val cmp = compare(a[i]!!, b[i]!!)
            if (cmp != 0) return cmp
        }
        return 0
    }

    private val entryComparator = Comparator<Map.Entry<*, *>> { e1, e2 ->
        val keyCmp = compare(e1.key, e2.key)
        if (keyCmp != 0) keyCmp else compare(e1.value, e2.value)
    }

    private fun compareMapsStructurally(a: Map<*, *>, b: Map<*, *>): Int {
        val sizeCmp = a.size.compareTo(b.size)
        if (sizeCmp != 0) return sizeCmp

        val aEntries = a.entries.sortedWith(entryComparator)
        val bEntries = b.entries.sortedWith(entryComparator)

        for (i in aEntries.indices) {
            val keyCmp = compare(aEntries[i].key, bEntries[i].key)
            if (keyCmp != 0) return keyCmp

            val valueCmp = compare(aEntries[i].value, bEntries[i].value)
            if (valueCmp != 0) return valueCmp
        }
        return 0
    }
}