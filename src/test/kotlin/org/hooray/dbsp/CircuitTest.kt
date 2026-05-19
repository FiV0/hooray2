package org.hooray.dbsp

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

class CircuitTest {

    /** Doubles its input. */
    private class Doubler : UnaryOperator<Int, Int> {
        override val name = "double"
        override fun eval(input: Int) = input * 2
    }

    /** Adds its two inputs. */
    private class Adder : BinaryOperator<Int, Int, Int> {
        override val name = "add"
        override fun eval(left: Int, right: Int) = left + right
    }

    /** Records the order in which operators are evaluated, into [log]. */
    private class Tracer(override val name: String, val log: MutableList<String>) :
        UnaryOperator<Int, Int> {
        override fun eval(input: Int): Int {
            log.add(name)
            return input
        }
    }

    /** Sums all inputs seen so far — a stateful operator. */
    private class RunningSum : UnaryOperator<Int, Int> {
        override val name = "running-sum"
        private var total = 0
        override fun eval(input: Int): Int {
            total += input
            return total
        }
    }

    @Test
    fun `linear chain source - map - output`() {
        val circuit = Circuit()
        val (input, handle) = circuit.addInput<Int>()
        val doubled = circuit.addUnary(Doubler(), input)
        val out = circuit.output(doubled)

        handle.push(3)
        circuit.step()
        assertEquals(6, out.get())

        handle.push(4)
        circuit.step()
        assertEquals(8, out.get())
    }

    @Test
    fun `diamond DAG fans out and back in`() {
        val circuit = Circuit()
        val (input, handle) = circuit.addInput<Int>()
        val left = circuit.addUnary(Doubler(), input)        // x*2
        val right = circuit.addUnary(Doubler(), input)       // x*2
        val sum = circuit.addBinary(Adder(), left, right)    // 4x
        val out = circuit.output(sum)

        handle.push(5)
        circuit.step()
        assertEquals(20, out.get())
    }

    @Test
    fun `evaluates each node once per step in dependency order`() {
        val log = mutableListOf<String>()
        val circuit = Circuit()
        val (input, handle) = circuit.addInput<Int>()
        val a = circuit.addUnary(Tracer("a", log), input)
        val b = circuit.addUnary(Tracer("b", log), a)
        circuit.output(b)

        handle.push(1)
        circuit.step()
        assertEquals(listOf("a", "b"), log)

        handle.push(2)
        circuit.step()
        assertEquals(listOf("a", "b", "a", "b"), log)
    }

    @Test
    fun `stateful operator retains state across steps`() {
        val circuit = Circuit()
        val (input, handle) = circuit.addInput<Int>()
        val running = circuit.addUnary(RunningSum(), input)
        val out = circuit.output(running)

        handle.push(10); circuit.step()
        assertEquals(10, out.get())
        handle.push(5); circuit.step()
        assertEquals(15, out.get())
        handle.push(100); circuit.step()
        assertEquals(115, out.get())
    }

    @Test
    fun `circuit supports multiple outputs`() {
        val circuit = Circuit()
        val (input, handle) = circuit.addInput<Int>()
        val doubled = circuit.addUnary(Doubler(), input)
        val outRaw = circuit.output(input)
        val outDoubled = circuit.output(doubled)

        handle.push(7)
        circuit.step()
        assertEquals(7, outRaw.get())
        assertEquals(14, outDoubled.get())
    }

    @Test
    fun `circuit freezes after first step`() {
        val circuit = Circuit()
        val (input, handle) = circuit.addInput<Int>()
        circuit.output(input)
        handle.push(1)
        circuit.step()

        assertThrows(IllegalStateException::class.java) {
            circuit.addUnary(Doubler(), input)
        }
    }

    @Test
    fun `unpushed input fails the step`() {
        val circuit = Circuit()
        val (input, _) = circuit.addInput<Int>()
        circuit.output(input)
        assertThrows(IllegalStateException::class.java) { circuit.step() }
    }

    @Test
    fun `reading output before stepping fails`() {
        val circuit = Circuit()
        val (input, _) = circuit.addInput<Int>()
        val out = circuit.output(input)
        assertThrows(IllegalStateException::class.java) { out.get() }
    }

    @Test
    fun `operatorNames reflects build order`() {
        val circuit = Circuit()
        val (input, _) = circuit.addInput<Int>()
        circuit.addUnary(Doubler(), input)
        assertEquals(listOf("input", "double"), circuit.operatorNames())
        assertEquals(2, circuit.nodeCount)
    }
}
