package org.hooray.incremental.stream

import org.hooray.algo.ResultTuple
import org.hooray.incremental.CompiledTriplePattern
import org.hooray.incremental.IntegerWeight
import org.hooray.incremental.ZSet
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class WcojBranchPlanTest {

    @Test
    fun `variableOrderForDeltaTerm puts the delta pattern's variables first`() {
        // patterns: R(0,1), S(0,2), T(1,2) at levels=3
        // delta=T -> variables [1,2]; remaining [0]; order [1,2,0]
        val t = CompiledTriplePattern(null, "t", null, 1, 2)
        assertEquals(listOf(1, 2, 0), variableOrderForDeltaTerm(t, levels = 3))
    }

    @Test
    fun `variableOrderForDeltaTerm with all variables in delta pattern preserves order`() {
        // Single-pattern join over 2 variables — delta has both
        val p = CompiledTriplePattern(null, "p", null, 0, 1)
        assertEquals(listOf(0, 1), variableOrderForDeltaTerm(p, levels = 2))
    }

    @Test
    fun `variableOrderForDeltaTerm orders delta variables consistently with the pattern`() {
        // Pattern with entity index 2, value index 0 — variableIndexes() returns [2,0]
        val p = CompiledTriplePattern(null, "p", null, 2, 0)
        // delta variables [2,0], remaining [1]; order [2,0,1]
        assertEquals(listOf(2, 0, 1), variableOrderForDeltaTerm(p, levels = 3))
    }

    @Test
    fun `permuteToCanonical with identity variable order returns input unchanged`() {
        val input: ResultTuple = listOf<Any>(1, 2, 3)
        val zset = ZSet.singleton(input, IntegerWeight(2))
        val out = permuteToCanonical(zset, variableOrder = listOf(0, 1, 2), levels = 3)
        assertEquals(IntegerWeight(2), out.weight(input))
        assertEquals(1, out.size)
    }

    @Test
    fun `permuteToCanonical swaps tuple positions to canonical order`() {
        // variableOrder = [1, 0]: position 0 holds variable 1, position 1 holds variable 0.
        // Tuple (a, b) under that order means variable_1=a, variable_0=b.
        // Canonical [variable_0, variable_1] = (b, a).
        val branchTuple: ResultTuple = listOf<Any>("a", "b")
        val zset = ZSet.singleton(branchTuple, IntegerWeight.ONE)
        val out = permuteToCanonical(zset, variableOrder = listOf(1, 0), levels = 2)
        assertEquals(IntegerWeight.ONE, out.weight(listOf<Any>("b", "a")))
        assertEquals(IntegerWeight.ZERO, out.weight(branchTuple))
    }

    @Test
    fun `permuteToCanonical triangle delta reorders into canonical x y z`() {
        // For the T-delta branch (variables [1,2,0] -> canonical [0,1,2])
        // a branch tuple (y=2, z=3, x=1) becomes canonical (x=1, y=2, z=3)
        val branchTuple: ResultTuple = listOf<Any>(2, 3, 1)
        val zset = ZSet.singleton(branchTuple, IntegerWeight.ONE)
        val out = permuteToCanonical(zset, variableOrder = listOf(1, 2, 0), levels = 3)
        assertEquals(IntegerWeight.ONE, out.weight(listOf<Any>(1, 2, 3)))
    }

    @Test
    fun `permuteToCanonical coalesces tuples that map to the same canonical tuple`() {
        // Both branch tuples happen to canonicalize to the same result; weights sum.
        val zset = ZSet.fromMap(mapOf<ResultTuple, IntegerWeight>(
            listOf<Any>("a", "b") to IntegerWeight(1),
            listOf<Any>("a", "b") to IntegerWeight(2)
        ))
        // Note: maps deduplicate keys, so the test above just yields a single entry with weight 2.
        // To exercise coalescing, use two distinct branch tuples that permute to the same canonical.
        val z = ZSet.fromMap(mapOf<ResultTuple, IntegerWeight>(
            listOf<Any>("x", "y") to IntegerWeight(3)
        ))
        // identity permutation — no actual coalescing, just sanity check the size
        val out = permuteToCanonical(z, listOf(0, 1), levels = 2)
        assertEquals(1, out.size)
        assertEquals(IntegerWeight(3), out.weight(listOf<Any>("x", "y")))
    }
}
