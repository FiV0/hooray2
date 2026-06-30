package org.hooray.util

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test

class TrieTest {

    @Test
    fun `finds inserted full and partial prefixes`() {
        val trie = Trie<String>()

        trie.insert(listOf("a", "x"))
        trie.insert(listOf("a", "y"))
        trie.insert(listOf("b", "z"))

        val root = trie.trieNodeFor(emptyList())
        val a = trie.trieNodeFor(listOf("a"))
        val ax = trie.trieNodeFor(listOf("a", "x"))

        assertNotNull(root)
        assertEquals(listOf("a", "b"), root?.children?.keys?.toList())
        assertEquals(listOf("x", "y"), a?.children?.keys?.toList())
        assertEquals(emptyList<String>(), ax?.children?.keys?.toList())
    }

    @Test
    fun `returns null for missing prefixes`() {
        val trie = Trie<String>()

        trie.insert(listOf("a", "x"))

        assertNull(trie.trieNodeFor(listOf("b")))
        assertNull(trie.trieNodeFor(listOf("a", "z")))
    }

    @Test
    fun `empty prefix insert leaves root available`() {
        val trie = Trie<String>()

        trie.insert(emptyList())

        val root = trie.trieNodeFor(emptyList())
        assertNotNull(root)
        assertEquals(emptyList<String>(), root?.children?.keys?.toList())
    }

    @Test
    fun `duplicate inserts do not duplicate child entries`() {
        val trie = Trie<String>()

        trie.insert(listOf("a", "x"))
        trie.insert(listOf("a", "x"))
        trie.insert(listOf("a", "y"))

        assertEquals(listOf("a"), trie.trieNodeFor(emptyList())?.children?.keys?.toList())
        assertEquals(listOf("x", "y"), trie.trieNodeFor(listOf("a"))?.children?.keys?.toList())
    }
}
