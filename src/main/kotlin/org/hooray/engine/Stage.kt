package org.hooray.engine

enum class StageKind {
    ORDINARY,
    OR_PROPOSAL_BOUNDARY,
}

data class Stage(
    val introduces: List<Any>,
    val participants: List<ExecPattern>,
    val targetVariables: List<Any>,
    val kind: StageKind = StageKind.ORDINARY,
) {
    init {
        require(introduces.toSet().size == introduces.size) {
            "Stage introduced variables must be distinct"
        }
        require(targetVariables.toSet().size == targetVariables.size) {
            "Stage target variables must be distinct"
        }
        require(targetVariables.containsAll(introduces)) {
            "Stage target variables must contain introduced variables"
        }
        require(participants.isNotEmpty()) {
            "Stage must have at least one participant"
        }
    }

    val introducesVariables: Boolean
        get() = introduces.isNotEmpty()

    val proposerEligibleParticipants: List<ExecPattern>
        get() = participants.filter { it.proposerEligible }

    val validatorOnlyParticipants: List<ExecPattern>
        get() = participants.filterNot { it.proposerEligible }
}
