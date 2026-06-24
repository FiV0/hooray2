package org.hooray.engine

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class StageExecutorTest {

    @Test
    fun `zero introduce stage validates rows only`() {
        val input = BindingSet(
            variables = listOf("?e", "?age"),
            rows = listOf(
                listOf("a", 35),
                listOf("b", 40),
            ),
        )
        val validator = FilteringPattern { row -> row[1] == 35 }
        val stage = Stage(
            introduces = emptyList(),
            participants = listOf(validator),
            targetVariables = listOf("?e", "?age"),
        )

        val result = StageExecutor().execute(stage, input)

        assertEquals(listOf("?e", "?age"), result.variables)
        assertEquals(listOf(listOf("a", 35)), result.rows)
    }

    @Test
    fun `single proposer expands rows validates and reorders target layout`() {
        val input = BindingSet(
            variables = listOf("?e"),
            rows = listOf(
                listOf("a"),
                listOf("b"),
            ),
        )
        val proposer = MapProposer(
            idx = 0,
            variables = setOf("?e", "?age"),
            proposedValues = mapOf(
                listOf("a") to listOf(listOf(35)),
                listOf("b") to listOf(listOf(40)),
            ),
        )
        val validator = FilteringPattern { row -> row[0] == 35 }
        val stage = Stage(
            introduces = listOf("?age"),
            participants = listOf(proposer, validator),
            targetVariables = listOf("?age", "?e"),
        )

        val result = StageExecutor().execute(stage, input)

        assertEquals(listOf("?age", "?e"), result.variables)
        assertEquals(listOf(listOf(35, "a")), result.rows)
    }

    @Test
    fun `multi proposer stage partitions input rows by cheapest positive count`() {
        val input = BindingSet(
            variables = listOf("?e"),
            rows = listOf(
                listOf("a"),
                listOf("b"),
                listOf("c"),
            ),
        )
        val proposerA = MapProposer(
            idx = 0,
            variables = setOf("?e", "?x"),
            counts = mapOf(
                listOf("a") to 5,
                listOf("b") to 1,
                listOf("c") to 0,
            ),
            proposedValues = mapOf(
                listOf("b") to listOf(listOf(20)),
            ),
        )
        val proposerB = MapProposer(
            idx = 1,
            variables = setOf("?e", "?x"),
            counts = mapOf(
                listOf("a") to 1,
                listOf("b") to 4,
                listOf("c") to 0,
            ),
            proposedValues = mapOf(
                listOf("a") to listOf(listOf(10)),
            ),
        )
        val stage = Stage(
            introduces = listOf("?x"),
            participants = listOf(proposerA, proposerB),
            targetVariables = listOf("?e", "?x"),
        )

        val result = StageExecutor().execute(stage, input)

        assertEquals(listOf(listOf("b")), proposerA.proposedInputs)
        assertEquals(listOf(listOf("a")), proposerB.proposedInputs)
        assertEquals(
            listOf(
                listOf("a", 10),
                listOf("b", 20),
            ),
            result.rows,
        )
    }

    @Test
    fun `merged shards are validated and full-row distincted`() {
        val input = BindingSet(
            variables = listOf("?e"),
            rows = listOf(
                listOf("a"),
                listOf("a"),
                listOf("b"),
            ),
        )
        val proposer = MapProposer(
            idx = 0,
            variables = setOf("?e", "?x"),
            proposedValues = mapOf(
                listOf("a") to listOf(listOf(10)),
                listOf("b") to listOf(listOf(20)),
            ),
        )
        val validator = FilteringPattern { row -> (row[1] as Int) < 20 }
        val stage = Stage(
            introduces = listOf("?x"),
            participants = listOf(proposer, validator),
            targetVariables = listOf("?e", "?x"),
        )

        val result = StageExecutor().execute(stage, input)

        assertEquals(listOf(listOf("a", 10)), result.rows)
    }

    private class FilteringPattern(
        override val idx: Int = 99,
        private val keep: (BindingRow) -> Boolean,
    ) : ExecPattern {
        override val variables: Set<Any> = emptySet()

        override fun validate(input: BindingSet): BindingSet {
            return BindingSet(input.variables, input.rows.filter(keep))
        }
    }

    private class MapProposer(
        override val idx: Int,
        override val variables: Set<Any>,
        private val counts: Map<BindingRow, Int> = emptyMap(),
        private val proposedValues: Map<BindingRow, List<BindingRow>>,
    ) : ExecPattern {
        val proposedInputs = mutableListOf<BindingRow>()

        override fun count(
            input: BindingSet,
            introduces: List<Any>,
            proposals: List<Proposal>,
        ): List<Proposal> {
            val counts = input.rows.map { row ->
                counts[row] ?: (proposedValues[row]?.size ?: 0)
            }
            return updateProposals(idx, proposals, counts)
        }

        override fun propose(
            input: BindingSet,
            introduces: List<Any>,
            targetVariables: List<Any>,
        ): BindingSet {
            val extensions = mutableListOf<RowExtension>()
            input.rows.forEachIndexed { inputRowIndex, row ->
                proposedInputs.add(row)
                proposedValues[row].orEmpty().forEach { values ->
                    extensions += RowExtension(inputRowIndex, values)
                }
            }
            return input.extend(introduces, extensions).reorder(targetVariables)
        }

        override fun validate(input: BindingSet): BindingSet {
            return input
        }
    }
}
