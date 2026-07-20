package org.hooray.engine

/**
 * A stage in a join execution plan.
 *
 * @property added The variables added by this stage.
 * @property participants The patterns that participate in this stage.
 * @property targetVariables The variables that are bound after this stage. The resulting binding must contain
 * the variables in that order.
 *
 * If [added] is empty the stages is a validation stage, meaning all participants purely validate the input. If [added]
 * is non-empty, the stages does the usual WCO count, propose and validation work, meaning tuples are sharded by the best
 * proposer and validated against all other patterns. If added is non-empty and there is only one participant,
 * the stage simply runs the one proposal immediately.
 *
 */
data class Stage(
    val added: List<Variable>,
    val participants: List<ExecPattern>,
    val targetVariables: List<Variable>
)
{
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
    }
}
