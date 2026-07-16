package org.hooray.engine

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
