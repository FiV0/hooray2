package org.hooray.dbsp

import org.hooray.incremental.IntegerWeight
import org.hooray.incremental.ZSet
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import kotlin.random.Random

class IncrementalJoinTest {

    private fun zset(vararg pairs: Pair<Tuple, Int>): TupleZSet {
        val m = HashMap<Tuple, IntegerWeight>()
        for ((t, w) in pairs) m.merge(t, IntegerWeight(w)) { a, b -> a.add(b) }
        return ZSet.fromMap(m)
    }

    @Test
    fun `hand-traced insert, right-only insert, left-only retract`() {
        val join = IncrementalJoinOp(keyArity = 1)

        // step 1: insert [k,x] left and [k,y] right -> they join
        assertEquals(
            zset(Tuple.of("k", "x", "y") to 1),
            join.eval(zset(Tuple.of("k", "x") to 1), zset(Tuple.of("k", "y") to 1)),
        )
        // step 2: insert [k,y2] on the right only -> joins the existing left tuple
        assertEquals(
            zset(Tuple.of("k", "x", "y2") to 1),
            join.eval(emptyTupleZSet(), zset(Tuple.of("k", "y2") to 1)),
        )
        // step 3: retract [k,x] on the left only -> removes both joined results
        assertEquals(
            zset(Tuple.of("k", "x", "y") to -1, Tuple.of("k", "x", "y2") to -1),
            join.eval(zset(Tuple.of("k", "x") to -1), emptyTupleZSet()),
        )
    }

    @Test
    fun `cross term is included when both inputs change in one step`() {
        val join = IncrementalJoinOp(keyArity = 1)
        // prime the integrals: A = {[k,a]}, B = {[k,p]}
        join.eval(zset(Tuple.of("k", "a") to 1), zset(Tuple.of("k", "p") to 1))

        // add [k,b] left and [k,q] right simultaneously.
        // ΔO = b⋈(p+q) + a⋈q = [k,b,p] + [k,b,q] + [k,a,q]   (the b⋈q cross term)
        val out = join.eval(zset(Tuple.of("k", "b") to 1), zset(Tuple.of("k", "q") to 1))
        assertEquals(
            zset(
                Tuple.of("k", "b", "p") to 1,
                Tuple.of("k", "b", "q") to 1,
                Tuple.of("k", "a", "q") to 1,
            ),
            out,
        )
    }

    @Test
    fun `incremental cartesian join`() {
        val join = IncrementalJoinOp(keyArity = 0)
        assertEquals(
            zset(Tuple.of("x", "p") to 1),
            join.eval(zset(Tuple.of("x") to 1), zset(Tuple.of("p") to 1)),
        )
        // add y on the left only -> joins the existing right tuple p
        assertEquals(
            zset(Tuple.of("y", "p") to 1),
            join.eval(zset(Tuple.of("y") to 1), emptyTupleZSet()),
        )
    }

    private fun randomDelta(rng: Random): TupleZSet {
        val keys = listOf("k1", "k2")
        val payloads = listOf("x", "y", "z")
        val weights = listOf(-2, -1, 1, 2)
        val m = HashMap<Tuple, IntegerWeight>()
        repeat(rng.nextInt(0, 4)) {
            val tuple = Tuple.of(keys.random(rng), payloads.random(rng))
            m.merge(tuple, IntegerWeight(weights.random(rng))) { a, b -> a.add(b) }
        }
        return ZSet.fromMap(m)
    }

    @Test
    fun `fused join matches the composed oracle on random delta streams`() {
        for (seed in 0 until 50) {
            val rng = Random(seed)
            val fused = IncrementalJoinOp(keyArity = 1)
            val composed = ComposedJoin(keyArity = 1)
            repeat(20) { step ->
                val da = randomDelta(rng)
                val db = randomDelta(rng)
                assertEquals(
                    composed.eval(da, db),
                    fused.eval(da, db),
                    "fused vs composed mismatch at seed=$seed step=$step",
                )
            }
        }
    }

    @Test
    fun `integral of join deltas equals the full join of input integrals`() {
        for (seed in 100 until 130) {
            val rng = Random(seed)
            val fused = IncrementalJoinOp(keyArity = 1)
            val fullJoin = StreamJoinOp(keyArity = 1)
            var integralA = emptyTupleZSet()
            var integralB = emptyTupleZSet()
            var integralOut = emptyTupleZSet()
            repeat(20) { step ->
                val da = randomDelta(rng)
                val db = randomDelta(rng)
                integralOut = integralOut.add(fused.eval(da, db))
                integralA = integralA.add(da)
                integralB = integralB.add(db)
                assertEquals(
                    fullJoin.eval(integralA, integralB),
                    integralOut,
                    "incremental result diverged from full join at seed=$seed step=$step",
                )
            }
        }
    }
}
