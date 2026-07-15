package org.hooray.engine

import org.hooray.util.Trie

class RelationPattern(
    override val idx: Int,
    relation: BindingSet,
) : PlanPattern, ExecPattern {
    override val orderedVariables: List<Variable> = relation.variables
    override val variables: Set<Variable> = orderedVariables.toSet()
    private val hasRows = relation.rows.isNotEmpty()
    private val trie = Trie<Any>().apply {
        relation.rows.forEach(::insert)
    }
    private val root = requireNotNull(trie.trieNodeFor(emptyList())) {
        "Trie root must exist"
    }

    override fun groundable(bound: Set<Variable>): List<Variable> =
        orderedVariables.filterNot { it in bound }

    override fun count(
        input: BindingSet,
        introduces: List<Variable>,
        proposals: List<Proposal>,
    ): List<Proposal> {
        if (introduces.isEmpty() || !variables.containsAll(introduces)) return proposals
        val introductionStart = orderedVariables.indexOf(introduces.first())
        val prefixIndexes = orderedVariables
            .take(introductionStart)
            .map(input::columnIndex)
        val counts = input.rows.map { row ->
            countIntroductions(trieNodeFor(row, prefixIndexes), introduces.size)
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
        val introductionStart = orderedVariables.indexOf(introduces.first())
        val prefixIndexes = orderedVariables
            .take(introductionStart)
            .map(input::columnIndex)
        val extensions = buildList {
            input.rows.forEachIndexed { rowIndex, row ->
                introductions(trieNodeFor(row, prefixIndexes), introduces.size).forEach { values ->
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
    ): BindingSet {
        if (!hasRows) return BindingSet(input.variables, emptyList())

        val prefixIndexes = orderedVariables
            .takeWhile { variable -> variable in input.columnIndexes }
            .map(input::columnIndex)
        return BindingSet(
            variables = input.variables,
            rows = input.rows.filter { row -> trieNodeFor(row, prefixIndexes) != null },
        )
    }

    private fun trieNodeFor(
        row: BindingRow,
        prefixIndexes: List<Int>,
    ): Trie.Node<Any>? {
        var node = root
        for (index in prefixIndexes) {
            node = node.children[row[index]] ?: return null
        }
        return node
    }

    private fun countIntroductions(
        node: Trie.Node<Any>?,
        depth: Int,
    ): Int {
        if (node == null) return 0
        if (depth == 0) return 1
        return node.children.values.sumOf { child ->
            countIntroductions(child, depth - 1)
        }
    }

    private fun introductions(
        node: Trie.Node<Any>?,
        depth: Int,
    ): List<BindingRow> {
        if (node == null) return emptyList()
        if (depth == 0) return listOf(emptyList())
        return buildList {
            node.children.forEach { (value, child) ->
                introductions(child, depth - 1).forEach { suffix ->
                    add(listOf(value) + suffix)
                }
            }
        }
    }
}
