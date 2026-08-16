package org.hooray.engine

/**
 * A stage in a join execution plan.
 *
 * @property added The variables added by this stage.
 * @property participants The patterns that participate in this stage.
 * @property proposerPositions The positions of proposing patterns in [participants].
 * @property targetVariables The variables that are bound after this stage. The resulting binding must contain
 * the variables in that order.
 *
 * If [added] is empty the stages is a validation stage, meaning all participants purely validate the input. If [added]
 * is non-empty, the stage either runs its sole proposer directly or does the usual WCO count and proposal arbitration
 * across its designated proposers. Proposed tuples are validated against every other participant.
 *
 */
interface IStage {
    val added: List<Variable>
    val participants: List<ExecPattern>
    val proposerPositions: List<Int>
    val proposers: List<ExecPattern>
        get() = proposerPositions.map(participants::get)
    val targetVariables: List<Variable>
}

/**
 * A validated value implementation of [IStage].
 */
data class Stage(
    override val added: List<Variable>,
    override val participants: List<ExecPattern>,
    override val proposerPositions: List<Int>,
    override val targetVariables: List<Variable>,
) : IStage {
    init {
        require(added.toSet().size == added.size) {
            "Stage added variables must be distinct"
        }
        require(targetVariables.toSet().size == targetVariables.size) {
            "Stage target variables must be distinct"
        }
        require(targetVariables.containsAll(added)) {
            "Stage target variables must contain added variables"
        }
        require(participants.isNotEmpty()) { "Stage must have at least one participant" }
        require(participants.map { it.idx }.toSet().size == participants.size) {
            "Stage participant indexes must be distinct"
        }
        require(proposerPositions.zipWithNext().all { (left, right) -> left < right }) {
            "Stage proposer positions must be ordered and unique"
        }
        require(proposerPositions.all { it in participants.indices }) {
            "Stage proposer positions must be in bounds"
        }
        require(proposerPositions.isEmpty() == added.isEmpty()) {
            "Stage proposer positions must be empty exactly when added variables are empty"
        }
    }
}
