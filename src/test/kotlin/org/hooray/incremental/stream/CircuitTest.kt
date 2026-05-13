package org.hooray.incremental.stream

import org.hooray.algo.ResultTuple
import org.hooray.incremental.IndexedZSet
import org.hooray.incremental.IntegerWeight
import org.hooray.incremental.ResultZSet
import org.hooray.incremental.ZSet
import org.hooray.incremental.ZSetIndices
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class CircuitTest {

    private fun emptyIndices(): ZSetIndices = ZSetIndices(
        aev = IndexedZSet.empty(IntegerWeight.ZERO, IntegerWeight.ONE),
        ave = IndexedZSet.empty(IntegerWeight.ZERO, IntegerWeight.ONE)
    )

    private fun nonEmptyIndices(tag: String): ZSetIndices = ZSetIndices(
        aev = IndexedZSet.singleton<Any, Any, IntegerWeight, ZSet<Any, IntegerWeight>>(
            "attr",
            ZSet.singleton(tag, IntegerWeight.ONE),
            IntegerWeight.ZERO,
            IntegerWeight.ONE
        ),
        ave = IndexedZSet.empty(IntegerWeight.ZERO, IntegerWeight.ONE)
    )

    @Test
    fun `no-op circuit returns an empty result`() {
        val source = CircuitSource { _ -> ZSet.empty<ResultTuple>() }
        val circuit = Circuit(CircuitSpec(source))
        val result = circuit.step()
        assertTrue(result.isEmpty())
    }

    @Test
    fun `step reads the buffered input exactly once then resets to empty`() {
        val received = mutableListOf<ZSetIndices>()
        val source = CircuitSource { input ->
            received += input
            ZSet.empty<ResultTuple>()
        }
        val circuit = Circuit(CircuitSpec(source))

        circuit.input.set(nonEmptyIndices("d1"))
        circuit.step()
        // Second step without setting input — source should see empty
        circuit.step()

        assertEquals(2, received.size)
        assertEquals(nonEmptyIndices("d1"), received[0])
        assertEquals(emptyIndices(), received[1])
    }

    @Test
    fun `step(input) wrapper is equivalent to input set then step`() {
        val viaWrapper = mutableListOf<ZSetIndices>()
        val viaExplicit = mutableListOf<ZSetIndices>()

        val sourceA = CircuitSource { input ->
            viaWrapper += input
            ZSet.empty<ResultTuple>()
        }
        val sourceB = CircuitSource { input ->
            viaExplicit += input
            ZSet.empty<ResultTuple>()
        }
        val ca = Circuit(CircuitSpec(sourceA))
        val cb = Circuit(CircuitSpec(sourceB))

        val d = nonEmptyIndices("delta")
        ca.step(d)

        cb.input.set(d)
        cb.step()

        assertEquals(viaExplicit, viaWrapper)
    }

    @Test
    fun `transforms run after the source in the same tick`() {
        // Source emits a fixed singleton; the transform doubles the weight by adding to itself.
        val tuple: ResultTuple = listOf<Any>("x")
        val source = CircuitSource { _ -> ZSet.singleton(tuple, IntegerWeight(1)) }
        val doubler = object : CircuitTransform {
            override fun eval(input: ResultZSet): ResultZSet {
                return input.add(input)
            }
        }
        val circuit = Circuit(CircuitSpec(source, transforms = listOf(doubler)))
        val result = circuit.step()
        assertEquals(IntegerWeight(2), result.weight(tuple))
    }

    @Test
    fun `commit on source and transforms is called after eval each tick`() {
        val log = mutableListOf<String>()
        val source = object : CircuitSource {
            override fun eval(input: ZSetIndices): ResultZSet {
                log += "source.eval"
                return ZSet.empty()
            }
            override fun commit() { log += "source.commit" }
        }
        val transform = object : CircuitTransform {
            override fun eval(input: ResultZSet): ResultZSet {
                log += "transform.eval"
                return input
            }
            override fun commit() { log += "transform.commit" }
        }
        val circuit = Circuit(CircuitSpec(source, transforms = listOf(transform)))
        circuit.step()
        circuit.step()
        // Each tick: source.eval, transform.eval, source.commit, transform.commit
        assertEquals(
            listOf(
                "source.eval", "transform.eval", "source.commit", "transform.commit",
                "source.eval", "transform.eval", "source.commit", "transform.commit"
            ),
            log
        )
    }

    @Test
    fun `circuit input handle is the same object across calls`() {
        val source = CircuitSource { _ -> ZSet.empty<ResultTuple>() }
        val circuit = Circuit(CircuitSpec(source))
        assertSame(circuit.input, circuit.input)
    }
}
