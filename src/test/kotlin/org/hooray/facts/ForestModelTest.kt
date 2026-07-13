package org.hooray.facts

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

/**
 * Model-based tests for [Forest] operations, ported from datatoad's
 * `tests/forest_model.rs`: each operation is checked against a transparent model of
 * facts as a set of tuples. Data comes in two regimes — uniformly typed terms (small
 * Longs) and mixed-type terms, which exercise the comparator's type discriminators.
 */
class ForestModelTest {

    private fun regimes(count: Int, arity: Int, seed: Long): List<Pair<String, List<TestFact>>> =
        listOf(
            Pair("longs", longTuples(count, arity, seed)),
            Pair("mixed", mixedTuples(count, arity, seed)),
        )

    @Test
    fun `build and merge match the model`() {
        for ((name, xs) in regimes(500, 3, 1)) {
            val ys = if (name == "longs") longTuples(500, 3, 2) else mixedTuples(500, 3, 2)
            val fx = forestOf(xs)
            val fy = forestOf(ys)
            assertEquals(xs.toSet(), extract(fx), "$name: build")
            assertEquals(xs.toSet() union ys.toSet(), extract(fx.merge(fy)), "$name: merge")
        }
    }

    @Test
    fun `semijoin and antijoin match the model`() {
        for ((name, xs) in regimes(500, 2, 7)) {
            val keys = if (name == "longs") longTuples(50, 1, 8) else mixedTuples(50, 1, 8)
            val fx = forestOf(xs)
            val fk = forestOf(keys)
            val mx = xs.toSet()
            val mk = keys.toSet()

            val semi = mx.filter { mk.contains(listOf(it[0])) }.toSet()
            val anti = mx.filter { !mk.contains(listOf(it[0])) }.toSet()
            assertEquals(semi, extractLsm(fx.semijoin(listOf(fk))), "$name: semijoin")
            assertEquals(anti, extractLsm(fx.antijoin(listOf(fk))), "$name: antijoin")
        }
    }

    @Test
    fun `semijoin against several forests uses their union`() {
        for ((name, xs) in regimes(300, 2, 9)) {
            val (ys, zs) = if (name == "longs") {
                Pair(longTuples(30, 1, 10), longTuples(30, 1, 11))
            } else {
                Pair(mixedTuples(30, 1, 10), mixedTuples(30, 1, 11))
            }
            val fx = forestOf(xs)
            val mk = ys.toSet() union zs.toSet()
            val expected = xs.toSet().filter { mk.contains(listOf(it[0])) }.toSet()
            assertEquals(expected, extractLsm(fx.semijoin(listOf(forestOf(ys), forestOf(zs)))), "$name: semijoin two")
        }
    }

    @Test
    fun `filter matches the model`() {
        for ((name, xs) in regimes(500, 3, 3)) {
            val fx = forestOf(xs)
            val mx = xs.toSet()
            val lit = xs[0][1]
            val absent = -1L // outside both term domains

            // Filter by a literal that has matches.
            val litExpected = mx.filter { it[1] == lit }.toSet()
            val litActual = fx.filter(listOf(Pair(1, lit)), emptyList())
            assertEquals(litExpected, litActual?.let { extract(it) } ?: emptySet<TestFact>(), "$name: lit")

            // Filter by a literal that cannot match.
            assertNull(fx.filter(listOf(Pair(0, absent)), emptyList()), "$name: absent lit")

            // Filter by inter-column equality.
            val varExpected = mx.filter { it[0] == it[2] }.toSet()
            val varActual = fx.filter(emptyList(), listOf(listOf(0, 2)))
            assertEquals(varExpected, varActual?.let { extract(it) } ?: emptySet<TestFact>(), "$name: var")

            // Filter by both a literal and an equality.
            val bothExpected = mx.filter { it[1] == lit && it[0] == it[2] }.toSet()
            val bothActual = fx.filter(listOf(Pair(1, lit)), listOf(listOf(0, 2)))
            assertEquals(bothExpected, bothActual?.let { extract(it) } ?: emptySet<TestFact>(), "$name: lit+var")
        }
    }

    @Test
    fun `fact set advance accumulates distinct facts`() {
        val regimes = listOf(
            Pair("longs", List(4) { longTuples(200, 2, 20L + it) }),
            Pair("mixed", List(4) { mixedTuples(200, 2, 30L + it) }),
        )
        for ((name, batches) in regimes) {
            val factSet = FactSet()
            val model = mutableSetOf<TestFact>()
            for (batch in batches) {
                factSet.extend(listOf(forestOf(batch)))
                model.addAll(batch)
                factSet.advance()

                val stable = factSet.stable.contents().flatMap { extract(it) }.toSet()
                val recent = factSet.recent?.let { extract(it) } ?: emptySet()
                assertEquals(model, stable union recent, "$name: contents")
                assertTrue(stable.intersect(recent).isEmpty(), "$name: recent must be distinct from stable")
            }
            factSet.advance()
            assertFalse(factSet.active(), "$name: quiescent after the last batch")
            assertEquals(model, factSet.stable.contents().flatMap { extract(it) }.toSet(), "$name: stable holds everything")
        }
    }

    @Test
    fun `zero arity forests merge to the single empty fact`() {
        val empty = Forest.of(emptyList())
        assertEquals(0, empty.arity)
        assertEquals(1, empty.len())
        assertEquals(setOf(emptyList<Any?>()), extract(empty))
        assertEquals(1, empty.merge(Forest.of(emptyList())).len())
    }
}
