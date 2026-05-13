package org.hooray.incremental.stream

import org.hooray.algo.ResultTuple
import org.hooray.incremental.IndexedZSet
import org.hooray.incremental.IntegerWeight
import org.hooray.incremental.ZSet
import org.hooray.incremental.ZSetPrefixExtender
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import kotlin.random.Random

class ZSetGenericJoinTest {

    /**
     * Reference implementation: enumerate all combinations of one row
     * per input relation, check consistency on shared levels, and emit
     * the canonical tuple with the product of weights.
     *
     * `inputs[i]` is a list of (extensionTuple, weight) pairs where
     * `extensionTuple` lines up with `participatingLevels[i]`.
     */
    private fun bruteForceJoin(
        inputs: List<List<Pair<List<Any>, Int>>>,
        participatingLevels: List<List<Int>>,
        levels: Int
    ): Map<ResultTuple, Int> {
        val results = HashMap<ResultTuple, Int>()
        fun recurse(idx: Int, binding: MutableMap<Int, Any>, weight: Int) {
            if (idx == inputs.size) {
                val canonical: ResultTuple = (0 until levels).map { binding[it]!! }
                val merged = (results[canonical] ?: 0) + weight
                if (merged == 0) results.remove(canonical) else results[canonical] = merged
                return
            }
            val lvls = participatingLevels[idx]
            for ((tuple, w) in inputs[idx]) {
                val saved = HashMap<Int, Any?>()
                var ok = true
                for (j in tuple.indices) {
                    val lvl = lvls[j]
                    if (lvl in binding && binding[lvl] != tuple[j]) {
                        ok = false
                        break
                    }
                    saved[lvl] = binding[lvl]
                    binding[lvl] = tuple[j]
                }
                if (ok) recurse(idx + 1, binding, weight * w)
                for ((lvl, prior) in saved) {
                    if (prior == null) binding.remove(lvl) else binding[lvl] = prior
                }
            }
        }
        recurse(0, HashMap(), 1)
        return results
    }

    private fun nestedIndexedZSet(
        pairs: List<Pair<List<Any>, IntegerWeight>>
    ): IndexedZSet<Any, IntegerWeight> {
        // Build a depth-2 IndexedZSet (key -> inner ZSet of values).
        val groups = pairs.groupBy { it.first[0] }
        var root: IndexedZSet<Any, IntegerWeight> = IndexedZSet.empty()
        for ((firstKey, group) in groups) {
            val innerMap = HashMap<Any, IntegerWeight>()
            for ((tuple, w) in group) {
                innerMap.merge(tuple[1], w) { a, b -> a.add(b).takeIf { !it.isZero() } }
            }
            if (innerMap.isEmpty()) continue
            val inner = ZSet.fromMap(innerMap)
            val singleton = IndexedZSet.singleton(
                firstKey, inner, IntegerWeight.ZERO, IntegerWeight.ONE
            )
            root = if (root.isEmpty()) singleton else root.add(singleton)
        }
        return root
    }

    @Test
    fun `intersection of two unary relations multiplies weights`() {
        val r1 = ZSet.fromMap(mapOf<Any, IntegerWeight>(
            "a" to IntegerWeight(2),
            "b" to IntegerWeight(1)
        ))
        val r2 = ZSet.fromMap(mapOf<Any, IntegerWeight>(
            "a" to IntegerWeight(3),
            "b" to IntegerWeight(-1),
            "c" to IntegerWeight(5)
        ))
        val extenders = listOf(
            ZSetPrefixExtender.fromIndexedZSet(r1, listOf(0)),
            ZSetPrefixExtender.fromIndexedZSet(r2, listOf(0))
        )
        val result = computeZSetGenericJoin(extenders, levels = 1)
        assertEquals(IntegerWeight(6), result.weight(listOf<Any>("a")))   // 2 * 3
        assertEquals(IntegerWeight(-1), result.weight(listOf<Any>("b")))  // 1 * -1
        assertEquals(IntegerWeight.ZERO, result.weight(listOf<Any>("c"))) // c not in r1
        assertEquals(2, result.size)
    }

    @Test
    fun `triangle join produces the hand-computed triangles`() {
        // Canonical order [x, y, z], levels=3.
        // R(x,y) at levels [0,1]: {(1,2), (1,3), (2,3)}
        // S(x,z) at levels [0,2]: {(1,3), (2,3)}
        // T(y,z) at levels [1,2]: {(2,3), (3,3)}
        // Triangles:
        //   x=1, y=2, z=3: R(1,2) S(1,3) T(2,3) -> +1
        //   x=1, y=3, z=3: R(1,3) S(1,3) T(3,3) -> +1
        //   x=2, y=3, z=3: R(2,3) S(2,3) T(3,3) -> +1
        val r = nestedIndexedZSet(listOf(
            listOf<Any>(1, 2) to IntegerWeight.ONE,
            listOf<Any>(1, 3) to IntegerWeight.ONE,
            listOf<Any>(2, 3) to IntegerWeight.ONE
        ))
        val s = nestedIndexedZSet(listOf(
            listOf<Any>(1, 3) to IntegerWeight.ONE,
            listOf<Any>(2, 3) to IntegerWeight.ONE
        ))
        val t = nestedIndexedZSet(listOf(
            listOf<Any>(2, 3) to IntegerWeight.ONE,
            listOf<Any>(3, 3) to IntegerWeight.ONE
        ))
        val extenders = listOf(
            ZSetPrefixExtender.fromIndexedZSet(r, listOf(0, 1)),
            ZSetPrefixExtender.fromIndexedZSet(s, listOf(0, 2)),
            ZSetPrefixExtender.fromIndexedZSet(t, listOf(1, 2))
        )
        val result = computeZSetGenericJoin(extenders, levels = 3)
        assertEquals(IntegerWeight.ONE, result.weight(listOf<Any>(1, 2, 3)))
        assertEquals(IntegerWeight.ONE, result.weight(listOf<Any>(1, 3, 3)))
        assertEquals(IntegerWeight.ONE, result.weight(listOf<Any>(2, 3, 3)))
        assertEquals(3, result.size)
    }

    @Test
    fun `randomized binary join matches brute force reference across seeds`() {
        for (seed in 0 until 10) {
            val rng = Random(seed)
            // Two unary relations over level 0, with values drawn from {0..4}, weights from {-2..3}.
            val r1Raw = (0..4).mapNotNull { v ->
                val w = rng.nextInt(-2, 4)
                if (w == 0) null else listOf<Any>(v) to w
            }
            val r2Raw = (0..4).mapNotNull { v ->
                val w = rng.nextInt(-2, 4)
                if (w == 0) null else listOf<Any>(v) to w
            }
            val r1 = ZSet.fromMap(r1Raw.associate { it.first[0] to IntegerWeight(it.second) })
            val r2 = ZSet.fromMap(r2Raw.associate { it.first[0] to IntegerWeight(it.second) })
            val extenders = listOf(
                ZSetPrefixExtender.fromIndexedZSet(r1, listOf(0)),
                ZSetPrefixExtender.fromIndexedZSet(r2, listOf(0))
            )
            val actual = computeZSetGenericJoin(extenders, levels = 1)
            val expected = bruteForceJoin(
                inputs = listOf(r1Raw, r2Raw),
                participatingLevels = listOf(listOf(0), listOf(0)),
                levels = 1
            )

            for ((tuple, weight) in expected) {
                assertEquals(IntegerWeight(weight), actual.weight(tuple), "seed $seed tuple $tuple")
            }
            assertEquals(expected.size, actual.size, "seed $seed size mismatch")
        }
    }

    @Test
    fun `randomized 3-relation triangle join matches brute force across seeds`() {
        for (seed in 0 until 5) {
            val rng = Random(seed)
            fun randomBinary(): List<Pair<List<Any>, Int>> {
                val pairs = mutableListOf<Pair<List<Any>, Int>>()
                for (x in 0..2) for (y in 0..2) {
                    val w = rng.nextInt(-1, 3)
                    if (w != 0) pairs += listOf<Any>(x, y) to w
                }
                return pairs
            }
            val rRaw = randomBinary()
            val sRaw = randomBinary()
            val tRaw = randomBinary()
            val r = nestedIndexedZSet(rRaw.map { it.first to IntegerWeight(it.second) })
            val s = nestedIndexedZSet(sRaw.map { it.first to IntegerWeight(it.second) })
            val t = nestedIndexedZSet(tRaw.map { it.first to IntegerWeight(it.second) })
            val extenders = listOf(
                ZSetPrefixExtender.fromIndexedZSet(r, listOf(0, 1)),
                ZSetPrefixExtender.fromIndexedZSet(s, listOf(0, 2)),
                ZSetPrefixExtender.fromIndexedZSet(t, listOf(1, 2))
            )
            val actual = computeZSetGenericJoin(extenders, levels = 3)
            val expected = bruteForceJoin(
                inputs = listOf(rRaw, sRaw, tRaw),
                participatingLevels = listOf(listOf(0, 1), listOf(0, 2), listOf(1, 2)),
                levels = 3
            )
            for ((tuple, weight) in expected) {
                assertEquals(IntegerWeight(weight), actual.weight(tuple), "seed $seed tuple $tuple")
            }
            assertEquals(expected.size, actual.size, "seed $seed size mismatch")
        }
    }

    @Test
    fun `zSetGenericJoin builder produces a node rooted output`() {
        val input1: IndexedZSetStream<Any> = SimpleStream(SimpleNode(NodeId(0), "i1"))
        val input2: IndexedZSetStream<Any> = SimpleStream(SimpleNode(NodeId(1), "i2"))
        val out = zSetGenericJoin(
            NodeId(99), "join",
            listOf(input1, input2),
            listOf(listOf(0), listOf(0)),
            levels = 1
        )
        assertTrue(out.node is ZSetGenericJoinNode)
        val jn = out.node as ZSetGenericJoinNode
        assertEquals(NodeId(99), jn.id)
        assertEquals(1, jn.levels)
        assertSame(input1, jn.inputs[0])
    }
}
