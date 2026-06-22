package org.hooray.engine

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class StageContractTest {

    @Test
    fun `stage records introduced variables participants and target layout`() {
        val proposer = RecordingPattern(variables = setOf("?e"), proposerEligible = true)
        val validator = RecordingPattern(variables = setOf("?e", "?age"), proposerEligible = false)

        val stage = Stage(
            introduces = listOf("?e"),
            participants = listOf(proposer, validator),
            targetVariables = listOf("?e"),
        )

        assertEquals(listOf("?e"), stage.introduces)
        assertEquals(listOf(proposer, validator), stage.participants)
        assertEquals(listOf("?e"), stage.targetVariables)
        assertEquals(StageKind.ORDINARY, stage.kind)
        assertTrue(stage.introducesVariables)
        assertTrue(proposer.proposerEligible)
        assertFalse(validator.proposerEligible)
    }

    @Test
    fun `zero introduce validation stage is representable`() {
        val validator = RecordingPattern(variables = setOf("?e"), proposerEligible = false)

        val stage = Stage(
            introduces = emptyList(),
            participants = listOf(validator),
            targetVariables = listOf("?e"),
        )

        assertFalse(stage.introducesVariables)
        assertEquals(StageKind.ORDINARY, stage.kind)
        assertEquals(listOf(validator), stage.validatorOnlyParticipants)
    }

    @Test
    fun `dedicated or proposal boundary is explicit`() {
        val orPattern = RecordingPattern(variables = setOf("?x", "?c"), proposerEligible = true)

        val stage = Stage(
            introduces = listOf("?x", "?c"),
            participants = listOf(orPattern),
            targetVariables = listOf("?a", "?x", "?c"),
            kind = StageKind.OR_PROPOSAL_BOUNDARY,
        )

        assertEquals(StageKind.OR_PROPOSAL_BOUNDARY, stage.kind)
        assertTrue(stage.introducesVariables)
    }

    @Test
    fun `stage rejects duplicate introduced variables`() {
        val error = assertThrows(IllegalArgumentException::class.java) {
            Stage(
                introduces = listOf("?x", "?x"),
                participants = listOf(RecordingPattern(variables = setOf("?x"), proposerEligible = true)),
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
                participants = listOf(RecordingPattern(variables = setOf("?x"), proposerEligible = true)),
                targetVariables = listOf("?a"),
            )
        }

        assertEquals("Stage target variables must contain introduced variables", error.message)
    }

    private data class RecordingPattern(
        override val variables: Set<Any>,
        override val proposerEligible: Boolean,
    ) : ExecPattern {
        override fun count(input: BindingSet, introduces: List<Any>): List<Int> =
            List(input.rowCount) { 1 }

        override fun propose(
            input: BindingSet,
            introduces: List<Any>,
            targetVariables: List<Any>,
        ): BindingSet = input

        override fun validate(
            input: BindingSet,
            targetVariables: List<Any>,
        ): BindingSet = input
    }
}
