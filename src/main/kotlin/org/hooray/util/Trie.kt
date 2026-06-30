package org.hooray.util

class Trie<T> {
    class Node<T> internal constructor() {
        private val mutableChildren: MutableMap<T, Node<T>> = hashMapOf()

        val children: Map<T, Node<T>>
            get() = mutableChildren

        internal fun childFor(value: T): Node<T>? =
            mutableChildren[value]

        internal fun childForInsert(value: T): Node<T> =
            mutableChildren.getOrPut(value) { Node() }
    }

    private val root = Node<T>()

    fun insert(prefix: List<T>) {
        var node = root
        for (value in prefix) {
            node = node.childForInsert(value)
        }
    }

    fun trieNodeFor(prefix: List<T>): Node<T>? {
        var node = root
        for (value in prefix) {
            node = node.childFor(value) ?: return null
        }
        return node
    }
}
