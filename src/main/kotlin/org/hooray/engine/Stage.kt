package org.hooray.engine

data class Stage(
    val introduces: List<Variable>,
    val participants: List<ExecPattern>,
    val targetVariables: List<Variable>,
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
        require(participants.isNotEmpty()) { "Stage must have at least one participant" }
        require(participants.map { it.idx }.toSet().size == participants.size) {
            "Stage participant indexes must be distinct"
        }
    }
}
