package org.hooray.incremental.stream

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class RelationTest {

    @Test
    fun `BaseRelation reports AEV and AVE as available`() {
        val rel = BaseRelation(NodeId(1), "people")
        assertEquals(setOf(IndexLayout.AEV, IndexLayout.AVE), rel.availableIndexes())
        assertTrue(rel.canProvide(IndexLayout.AEV))
        assertTrue(rel.canProvide(IndexLayout.AVE))
        assertFalse(rel.canProvide(IndexLayout.EAV))
        assertFalse(rel.canProvide(IndexLayout.VAE))
    }

    @Test
    fun `BaseRelation index streams are rooted at the relation itself`() {
        val rel = BaseRelation(NodeId(2), "edges")
        val aev = rel.index(IndexLayout.AEV)
        val ave = rel.index(IndexLayout.AVE)
        assertSame(rel, aev.node)
        assertSame(rel, ave.node)
    }

    @Test
    fun `BaseRelation canonicalStream is rooted at the relation`() {
        val rel = BaseRelation(NodeId(3), "triples")
        assertSame(rel, rel.canonicalStream.node)
    }

    @Test
    fun `BaseRelation index throws on unavailable layouts`() {
        val rel = BaseRelation(NodeId(4), "t4")
        assertThrows<IllegalArgumentException> { rel.index(IndexLayout.EAV) }
        assertThrows<IllegalArgumentException> { rel.index(IndexLayout.VAE) }
    }

    @Test
    fun `BaseRelation id matches its NodeId`() {
        val rel = BaseRelation(NodeId(7), "r7")
        assertEquals(NodeId(7), rel.id)
        assertEquals("r7", rel.label)
    }
}
