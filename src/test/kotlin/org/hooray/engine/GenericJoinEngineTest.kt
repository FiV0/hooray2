package org.hooray.engine

import clojure.lang.Symbol
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

class GenericJoinEngineTest {
    private val e = Symbol.intern("?e")
    private val x = Symbol.intern("?x")
    private val y = Symbol.intern("?y")

    @Test
    fun `executes stages sequentially`() {
        val first = MapPattern(
            idx = 0,
            patternVariables = setOf(e, x),
            values = mapOf(listOf("a") to listOf(listOf(1))),
        )
        val second = MapPattern(
            idx = 1,
            patternVariables = setOf(x, y),
            values = mapOf(listOf("a", 1) to listOf(listOf(2))),
        )

        val result = GenericJoinEngine().execute(
            stages = listOf(
                Stage(listOf(x), listOf(first), listOf(e, x)),
                Stage(listOf(y), listOf(second), listOf(e, x, y)),
            ),
            input = BindingSet(listOf(e), listOf(listOf("a"))),
        )

        assertEquals(BindingSet(listOf(e, x, y), listOf(listOf("a", 1, 2))), result)
    }

    @Test
    fun `chooses the cheapest proposer per row and uses participant order for ties`() {
        val input = BindingSet(
            listOf(e),
            listOf(listOf("a"), listOf("b"), listOf("c")),
        )
        val first = MapPattern(
            idx = 1,
            patternVariables = setOf(e, x),
            counts = mapOf(listOf("a") to 2, listOf("b") to 1, listOf("c") to 1),
            values = mapOf(listOf("b") to listOf(listOf(20)), listOf("c") to listOf(listOf(30))),
        )
        val second = MapPattern(
            idx = 2,
            patternVariables = setOf(e, x),
            counts = mapOf(listOf("a") to 1, listOf("b") to 2, listOf("c") to 1),
            values = mapOf(listOf("a") to listOf(listOf(10))),
        )

        val result = GenericJoinEngine().execute(
            listOf(Stage(listOf(x), listOf(first, second), listOf(e, x))),
            input,
        )

        assertEquals(listOf(listOf("a")), second.proposedInputs)
        assertEquals(listOf(listOf("b"), listOf("c")), first.proposedInputs)
        assertEquals(listOf(listOf("a", 10), listOf("b", 20), listOf("c", 30)), result.rows)
    }

    @Test
    fun `validators receive no added variables after another pattern proposes`() {
        val proposer = MapPattern(
            idx = 0,
            patternVariables = setOf(e, x),
            values = mapOf(listOf("a") to listOf(listOf(1))),
        )
        var validationAdded: List<Variable>? = null
        val validator = object : ExecPattern {
            override val idx = 1
            override val variables = setOf(e, x)

            override fun join(
                input: BindingSet,
                added: List<Variable>,
                targetVariables: List<Variable>,
            ): BindingSet {
                validationAdded = added
                return input
            }
        }

        GenericJoinEngine().execute(
            listOf(Stage(listOf(x), listOf(proposer, validator), listOf(e, x))),
            BindingSet(listOf(e), listOf(listOf("a"))),
        )

        assertEquals(emptyList<Variable>(), validationAdded)
    }

    @Test
    fun `runs validation-only stages and preserves their input layout`() {
        val validator = object : ExecPattern {
            override val idx = 0
            override val variables = setOf(e)

            override fun join(
                input: BindingSet,
                added: List<Variable>,
                targetVariables: List<Variable>,
            ) = BindingSet(input.variables, input.rows.filter { it[0] == "a" })
        }
        val input = BindingSet(listOf(e), listOf(listOf("a"), listOf("b")))

        val result = GenericJoinEngine().execute(
            listOf(Stage(emptyList(), listOf(validator), listOf(e))),
            input,
        )

        assertEquals(listOf(listOf("a")), result.rows)
    }

    @Test
    fun `rejects stage layouts that do not extend the input exactly`() {
        val pattern = MapPattern(0, setOf(x), values = emptyMap())
        val error = assertThrows(IllegalArgumentException::class.java) {
            GenericJoinEngine().execute(
                listOf(Stage(listOf(x), listOf(pattern), listOf(x))),
                BindingSet(listOf(e), listOf(listOf("a"))),
            )
        }

        assertEquals("Stage target variables must equal input variables plus added variables", error.message)
    }

    @Test
    fun `drops rows without a positive proposal`() {
        val pattern = MapPattern(
            idx = 0,
            patternVariables = setOf(e, x),
            counts = mapOf(listOf("a") to 0, listOf("b") to -1),
            values = emptyMap(),
        )
        val result = GenericJoinEngine().execute(
            listOf(Stage(listOf(x), listOf(pattern), listOf(e, x))),
            BindingSet(listOf(e), listOf(listOf("a"), listOf("b"))),
        )

        assertEquals(emptyList<BindingRow>(), result.rows)
    }

    @Test
    fun `rejects unknown proposer indexes`() {
        val pattern = object : ExecPattern {
            override val idx = 0
            override val variables = setOf(x)

            override fun count(
                input: BindingSet,
                added: List<Variable>,
                proposals: List<Proposal>,
            ) = List(input.rowCount) { Proposal(99, 1) }

            override fun join(
                input: BindingSet,
                added: List<Variable>,
                targetVariables: List<Variable>,
            ) = input
        }

        val error = assertThrows(IllegalArgumentException::class.java) {
            GenericJoinEngine().execute(
                listOf(Stage(listOf(x), listOf(pattern), listOf(x))),
                BindingSet(emptyList(), listOf(emptyList())),
            )
        }
        assertEquals("Unknown proposer index 99", error.message)
    }

    @Test
    fun `rejects proposal tables with the wrong row count`() {
        val pattern = object : ExecPattern {
            override val idx = 0
            override val variables = setOf(x)

            override fun count(
                input: BindingSet,
                added: List<Variable>,
                proposals: List<Proposal>,
            ) = emptyList<Proposal>()

            override fun join(
                input: BindingSet,
                added: List<Variable>,
                targetVariables: List<Variable>,
            ) = input
        }

        val error = assertThrows(IllegalArgumentException::class.java) {
            GenericJoinEngine().execute(
                listOf(Stage(listOf(x), listOf(pattern), listOf(x))),
                BindingSet(emptyList(), listOf(emptyList())),
            )
        }
        assertEquals("Pattern 0 returned 0 proposals, expected 1", error.message)
    }

    @Test
    fun `rejects proposal and validation layout changes`() {
        val wrongProposal = object : ExecPattern {
            override val idx = 0
            override val variables = setOf(x)

            override fun count(
                input: BindingSet,
                added: List<Variable>,
                proposals: List<Proposal>,
            ) = List(input.rowCount) { Proposal(idx, 1) }

            override fun join(
                input: BindingSet,
                added: List<Variable>,
                targetVariables: List<Variable>,
            ) = BindingSet(listOf(x, e), listOf(listOf(1, "a")))
        }
        val proposalError = assertThrows(IllegalArgumentException::class.java) {
            GenericJoinEngine().execute(
                listOf(Stage(listOf(x), listOf(wrongProposal), listOf(e, x))),
                BindingSet(listOf(e), listOf(listOf("a"))),
            )
        }
        assertEquals("Pattern 0 proposed layout [?x, ?e], expected [?e, ?x]", proposalError.message)

        val wrongValidator = object : ExecPattern {
            override val idx = 1
            override val variables = setOf(e)

            override fun join(
                input: BindingSet,
                added: List<Variable>,
                targetVariables: List<Variable>,
            ) = BindingSet(emptyList(), listOf(emptyList()))
        }
        val validationError = assertThrows(IllegalArgumentException::class.java) {
            GenericJoinEngine().execute(
                listOf(Stage(emptyList(), listOf(wrongValidator), listOf(e))),
                BindingSet(listOf(e), listOf(listOf("a"))),
            )
        }
        assertEquals("Pattern 1 changed the layout during validation", validationError.message)
    }

    private class MapPattern(
        override val idx: Int,
        private val patternVariables: Set<Variable>,
        private val counts: Map<BindingRow, Int> = emptyMap(),
        private val values: Map<BindingRow, List<BindingRow>>,
    ) : ExecPattern {
        override val variables = patternVariables
        val proposedInputs = mutableListOf<BindingRow>()

        override fun count(
            input: BindingSet,
            added: List<Variable>,
            proposals: List<Proposal>,
        ): List<Proposal> = updateProposals(
            idx,
            proposals,
            input.rows.map { counts[it] ?: values[it].orEmpty().size },
        )

        override fun join(
            input: BindingSet,
            added: List<Variable>,
            targetVariables: List<Variable>,
        ): BindingSet {
            if (added.isEmpty()) return input
            val extensions = buildList {
                input.rows.forEachIndexed { rowIndex, row ->
                    proposedInputs += row
                    values[row].orEmpty().forEach { add(RowExtension(rowIndex, it)) }
                }
            }
            return input.extend(added, extensions).reorder(targetVariables)
        }
    }
}
