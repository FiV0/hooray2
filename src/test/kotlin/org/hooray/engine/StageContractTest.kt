package org.hooray.engine

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class StageContractTest {

    @Test
    fun `stage records introduced variables participants and target layout`() {
        val proposer = RecordingPattern(idx = 0, variables = setOf("?e"))
        val validator = RecordingPattern(idx = 1, variables = setOf("?e", "?age"))

        val stage = Stage(
            introduces = listOf("?e"),
            participants = listOf(proposer, validator),
            targetVariables = listOf("?e"),
        )

        assertEquals(listOf("?e"), stage.introduces)
        assertEquals(listOf(proposer, validator), stage.participants)
        assertEquals(listOf("?e"), stage.targetVariables)
        assertTrue(stage.introducesVariables)
    }

    @Test
    fun `zero introduce validation stage is representable`() {
        val validator = RecordingPattern(idx = 0, variables = setOf("?e"))

        val stage = Stage(
            introduces = emptyList(),
            participants = listOf(validator),
            targetVariables = listOf("?e"),
        )

        assertEquals(emptyList<Any>(), stage.introduces)
        assertEquals(listOf(validator), stage.participants)
    }

    @Test
    fun `stage rejects duplicate introduced variables`() {
        val error = assertThrows(IllegalArgumentException::class.java) {
            Stage(
                introduces = listOf("?x", "?x"),
                participants = listOf(RecordingPattern(idx = 0, variables = setOf("?x"))),
                targetVariables = listOf("?x"),
            )
        }

        assertEquals("Stage introduced variables must be distinct", error.message)
    }

    @Test
    fun `stage target must contain introduced variables`() {
        val error = assertThrows(IllegalArgumentException::class.java) {
            Stage(
                introduces = listOf("?x"),
                participants = listOf(RecordingPattern(idx = 0, variables = setOf("?x"))),
                targetVariables = listOf("?a"),
            )
        }

        assertEquals("Stage target variables must contain introduced variables", error.message)
    }

    @Test
    fun `stage rejects duplicate participant indexes`() {
        val error = assertThrows(IllegalArgumentException::class.java) {
            Stage(
                introduces = listOf("?x"),
                participants = listOf(
                    RecordingPattern(idx = 0, variables = setOf("?x")),
                    RecordingPattern(idx = 0, variables = setOf("?x")),
                ),
                targetVariables = listOf("?x"),
            )
        }

        assertEquals("Stage participant indexes must be distinct", error.message)
    }

    private data class RecordingPattern(
        override val idx: Int,
        override val variables: Set<Any>,
    ) : ExecPattern {
        override fun count(
            input: BindingSet,
            introduces: List<Any>,
            proposals: List<Proposal>,
        ): List<Proposal> = updateProposals(idx, proposals, List(input.rowCount) { 1 })

        override fun propose(
            input: BindingSet,
            introduces: List<Any>,
            targetVariables: List<Any>,
        ): BindingSet = input

        override fun validate(input: BindingSet): BindingSet = input
    }
}
