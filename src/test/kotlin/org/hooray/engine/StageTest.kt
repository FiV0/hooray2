package org.hooray.engine

import clojure.lang.Symbol
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

class StageTest {
    private val x = Symbol.intern("?x")
    private val first = TestPattern(1)
    private val second = TestPattern(2)

    @Test
    fun `requires proposer positions for proposing stages`() {
        val error = assertThrows(IllegalArgumentException::class.java) {
            Stage(listOf(x), listOf(first), emptyList(), listOf(x))
        }

        assertEquals(
            "Stage proposer positions must be empty exactly when added variables are empty",
            error.message,
        )
    }

    @Test
    fun `forbids proposer positions for validation stages`() {
        val error = assertThrows(IllegalArgumentException::class.java) {
            Stage(emptyList(), listOf(first), listOf(0), emptyList())
        }

        assertEquals(
            "Stage proposer positions must be empty exactly when added variables are empty",
            error.message,
        )
    }

    @Test
    fun `requires proposer positions to be ordered and unique`() {
        listOf(listOf(0, 0), listOf(1, 0)).forEach { positions ->
            val error = assertThrows(IllegalArgumentException::class.java) {
                Stage(listOf(x), listOf(first, second), positions, listOf(x))
            }

            assertEquals("Stage proposer positions must be ordered and unique", error.message)
        }
    }

    @Test
    fun `requires proposer positions to be in bounds`() {
        listOf(listOf(-1), listOf(1)).forEach { positions ->
            val error = assertThrows(IllegalArgumentException::class.java) {
                Stage(listOf(x), listOf(first), positions, listOf(x))
            }

            assertEquals("Stage proposer positions must be in bounds", error.message)
        }
    }

    @Test
    fun `derives proposers in participant order`() {
        val stage = Stage(listOf(x), listOf(first, second), listOf(0, 1), listOf(x))

        assertEquals(listOf(first, second), stage.proposers)
    }

    private class TestPattern(override val idx: Int) : ExecPattern {
        override val variables: Set<Variable> = emptySet()

        override fun join(
            input: BindingSet,
            added: List<Variable>,
            targetVariables: List<Variable>,
        ): BindingSet = input
    }
}
