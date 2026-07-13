package org.hooray.engine

class RelationPattern(
    override val idx: Int,
    private val relation: BindingSet,
) : PlanPattern, ExecPattern {
    override val orderedVariables: List<Variable> = relation.variables
    override val variables: Set<Variable> = orderedVariables.toSet()

    override fun groundingGroups(bound: List<Variable>): List<GroundingGroup> {
        return orderedVariables
            .filterNot { it in bound }
            .map { GroundingGroup(listOf(it)) }
    }

    override fun count(
        input: BindingSet,
        introduces: List<Variable>,
        proposals: List<Proposal>,
    ): List<Proposal> {
        if (introduces.isEmpty() || !variables.containsAll(introduces)) return proposals
        val counts = input.rows.map { row ->
            matchingIntroductions(input.variables, row, introduces).size
        }
        return updateProposals(idx, proposals, counts)
    }

    override fun propose(
        input: BindingSet,
        introduces: List<Variable>,
        targetVariables: List<Variable>,
    ): BindingSet {
        require(introduces.isNotEmpty() && variables.containsAll(introduces)) {
            "Relation pattern cannot introduce variables it does not contain"
        }
        val extensions = buildList {
            input.rows.forEachIndexed { rowIndex, row ->
                matchingIntroductions(input.variables, row, introduces).forEach { values ->
                    add(RowExtension(rowIndex, values))
                }
            }
        }
        return input.extend(introduces, extensions).reorder(targetVariables).distinctRows()
    }

    override fun validate(
        input: BindingSet,
        introduces: List<Variable>,
        targetVariables: List<Variable>,
    ): BindingSet = input.semijoin(relation)

    private fun matchingIntroductions(
        inputVariables: List<Variable>,
        inputRow: BindingRow,
        introduces: List<Variable>,
    ): List<BindingRow> {
        val sharedVariables = inputVariables.filter { it in variables }
        val inputIndexes = sharedVariables.map(inputVariables::indexOf)
        val relationIndexes = sharedVariables.map(relation::columnIndex)
        val introducedIndexes = introduces.map(relation::columnIndex)
        val seen = linkedSetOf<BindingRow>()
        for (relationRow in relation.rows) {
            val matches = inputIndexes.indices.all { index ->
                inputRow[inputIndexes[index]] == relationRow[relationIndexes[index]]
            }
            if (matches) seen += introducedIndexes.map { relationRow[it] }
        }
        return seen.toList()
    }
}
