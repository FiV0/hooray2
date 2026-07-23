package org.hooray.dbsp

import org.hooray.incremental.IntegerWeight
import org.hooray.incremental.ZSet
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

class LinearOpsTest {

    /** Builds a TupleZSet; repeated tuples have their weights summed. */
    private fun zset(vararg pairs: Pair<Tuple, Int>): TupleZSet {
        val m = HashMap<Tuple, IntegerWeight>()
        for ((t, w) in pairs) {
            m.merge(t, IntegerWeight(w)) { a, b -> a.add(b) }
        }
        return ZSet.fromMap(m)
    }

    // --- FilterOp ---

    @Test
    fun `filter keeps matching tuples and drops the rest`() {
        val op = FilterOp("keep-a") { it[0] == "a" }
        val out = op.eval(zset(Tuple.of("a", 1) to 1, Tuple.of("b", 2) to 1))
        assertEquals(zset(Tuple.of("a", 1) to 1), out)
    }

    @Test
    fun `filter preserves weights including negative`() {
        val op = FilterOp { it[0] == "a" }
        val out = op.eval(zset(Tuple.of("a", 1) to -3, Tuple.of("a", 2) to 5))
        assertEquals(zset(Tuple.of("a", 1) to -3, Tuple.of("a", 2) to 5), out)
    }

    @Test
    fun `filter on empty input yields empty`() {
        assertTrue(FilterOp { true }.eval(emptyTupleZSet()).isEmpty())
    }

    @Test
    fun `filter from predicate keeps matching tuples and preserves weights`() {
        val op = FilterOp.fromPredicate("adult") { (it[1] as Int) >= 18 }
        val out = op.eval(
            zset(
                Tuple.of("Ada", 36) to 2,
                Tuple.of("Bob", 17) to 3,
                Tuple.of("Carla", 42) to -1,
            )
        )
        assertEquals(zset(Tuple.of("Ada", 36) to 2, Tuple.of("Carla", 42) to -1), out)
    }

    // --- MapOp ---

    @Test
    fun `map project reorders columns`() {
        val op = MapOp.project(intArrayOf(1, 0))
        val out = op.eval(zset(Tuple.of("e", "v") to 1))
        assertEquals(zset(Tuple.of("v", "e") to 1), out)
    }

    @Test
    fun `map projection sums weights of colliding tuples`() {
        val op = MapOp.project(intArrayOf(0))   // project to column 0
        val out = op.eval(zset(Tuple.of("a", 1) to 1, Tuple.of("a", 2) to 2))
        assertEquals(zset(Tuple.of("a") to 3), out)
    }

    @Test
    fun `map projection drops tuples whose weights cancel to zero`() {
        val op = MapOp.project(intArrayOf(0))
        val out = op.eval(zset(Tuple.of("a", 1) to 1, Tuple.of("a", 2) to -1))
        assertTrue(out.isEmpty())
    }

    @Test
    fun `map applies an arbitrary transform`() {
        val op = MapOp("tag") { Tuple.of(it[0], "tagged") }
        val out = op.eval(zset(Tuple.of("x") to 2))
        assertEquals(zset(Tuple.of("x", "tagged") to 2), out)
    }

    @Test
    fun `map from function appends a computed column and preserves weights`() {
        val op = MapOp.fromFunction("half") { t -> t.append((t[1] as Int) / 2) }
        val out = op.eval(zset(Tuple.of("Ada", 36) to 2, Tuple.of("Bob", 17) to -1))
        assertEquals(zset(Tuple.of("Ada", 36, 18) to 2, Tuple.of("Bob", 17, 8) to -1), out)
    }

    @Test
    fun `map on empty input yields empty`() {
        assertTrue(MapOp.project(intArrayOf(0)).eval(emptyTupleZSet()).isEmpty())
    }
}
