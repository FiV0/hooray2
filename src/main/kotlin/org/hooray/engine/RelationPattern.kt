package org.hooray.engine

import org.hooray.util.Trie

/**
 * A pattern that matches a relation (set of rows) with a fixed set of variables.
 *
 * @property idx The index of this pattern in the plan.
 * @property relation The relation to match against.
 *
 * When proposing variables, any prefix of [variables] must appear in the input binding set
 * in that order. There are possibly more variables in the input relation and they interleave
 * the prefix, but variables from the prefix are not shuffled. Validation accepts a bound
 * relation prefix in any input layout.
 */
class RelationPattern(
    override val idx: Int,
    val relation: BindingSet,
) : ExecPattern {
    private val orderedVariables: List<Variable> = relation.variables
    override val variables: Set<Variable> = orderedVariables.toSet()
    private val hasRows = relation.rows.isNotEmpty()
    private val trie = Trie<Any>().apply {
        relation.rows.forEach(::insert)
    }
    private val root = requireNotNull(trie.trieNodeFor(emptyList())) {
        "Trie root must exist"
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

    private fun MutableList<RowExtension>.addIntroductionExtensions(
        inputRowIndex: Int,
        node: Trie.Node<Any>,
        depth: Int,
        values: MutableList<Any>,
    ) {
        if (depth == 0) {
            add(RowExtension(inputRowIndex, values.toList()))
            return
        }
        node.children.forEach { (value, child) ->
            values.add(value)
            addIntroductionExtensions(inputRowIndex, child, depth - 1, values)
            values.removeAt(values.lastIndex)
        }
    }

    private fun isPrefix(prefixVars: List<Variable>): Boolean {
        return prefixVars == orderedVariables.take(prefixVars.size)
    }

    private fun prefixIndexesOrNull(
        input: BindingSet,
        added: List<Variable>,
    ): List<Int>? {
        val inputPrefix = input.variables.filter { variable -> variable in variables }
        if (!isPrefix(inputPrefix + added)) return null
        return inputPrefix.map(input::columnIndex)
    }

    private fun prefixIndexes(
        input: BindingSet,
        added: List<Variable>,
    ): List<Int> {
        return requireNotNull(prefixIndexesOrNull(input, added)) {
            "Relation variables in the input followed by introduced variables must form a relation prefix"
        }
    }

    override fun count(
        input: BindingSet,
        added: List<Variable>,
        proposals: List<Proposal>,
    ): List<Proposal> {
        if (added.isEmpty() || !variables.containsAll(added)) return proposals
        val prefixIndexes = prefixIndexesOrNull(input, added) ?: return proposals
        val counts = input.rows.map { row ->
            countIntroductions(trieNodeFor(row, prefixIndexes), added.size)
        }
        return updateProposals(idx, proposals, counts)
    }

    override fun join(
        input: BindingSet,
        added: List<Variable>,
        targetVariables: List<Variable>,
    ): BindingSet = if (added.isEmpty()) {
        validate(input, added, targetVariables)
    } else {
        propose(input, added, targetVariables)
    }

    private fun propose(
        input: BindingSet,
        added: List<Variable>,
        targetVariables: List<Variable>,
    ): BindingSet {
        require(added.isNotEmpty() && variables.containsAll(added)) {
            "Relation pattern cannot introduce variables it does not contain"
        }
        val prefixIndexes = prefixIndexes(input, added)
        val extensions = buildList {
            val values = ArrayList<Any>(added.size)
            input.rows.forEachIndexed { rowIndex, row ->
                val node = trieNodeFor(row, prefixIndexes) ?: return@forEachIndexed
                addIntroductionExtensions(rowIndex, node, added.size, values)
            }
        }
        return input.extend(added, extensions).reorder(targetVariables)
    }

    private fun validate(
        input: BindingSet,
        added: List<Variable>,
        targetVariables: List<Variable>,
    ): BindingSet {
        require(input.variables.containsAll(added)) {
            "BindingSet must contain all introduced variables for validation"
        }
        if (!hasRows) return BindingSet(input.variables, emptyList())

        val boundVariables = orderedVariables.filter { variable -> variable in input.columnIndexes }
        require(isPrefix(boundVariables)) {
            "Bound relation variables must form a relation prefix"
        }
        val relationIndexes = boundVariables.map(input::columnIndex)
        return BindingSet(
            variables = input.variables,
            rows = input.rows.filter { row -> trieNodeFor(row, relationIndexes) != null },
        )
    }
}
