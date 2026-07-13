package org.hooray.facts

/**
 * A non-empty collection of facts, represented as a trie with one layer per term.
 *
 * Port of datatoad's `Forest<Terms>` (trie.rs). There are as many layers as the
 * collection's arity, each verified to be non-empty and to have as many lists as the
 * preceding layer has items. The first layer has one list, corresponding to an implicit
 * unit 0th column; when there are no layers, the collection holds one 0-ary fact.
 *
 * To represent an optionally empty collection use a `Forest?` or a [FactLSM].
 */
class Forest private constructor(private val layers: MutableList<Layer>) {

    /** The number of columns of facts in the collection. */
    val arity: Int get() = layers.size

    /** The indexed layer. */
    fun layer(index: Int): Layer = layers[index]

    /** The layers, in column order (Rust `Forest::borrow`). */
    fun layers(): List<Layer> = layers

    /** The number of facts in the collection (Rust `Forest::len`). */
    fun len(): Int = layers.lastOrNull()?.itemCount ?: 1

    fun isEmpty(): Boolean = len() == 0

    /**
     * Inserts a new column for all facts, by a conforming layer: non-empty, with as
     * many lists as [len].
     */
    fun pushLayer(layer: Layer) {
        require(!layer.isEmpty()) { "layer must be non-empty" }
        if (layers.isNotEmpty()) require(len() == layer.size) { "layer must have ${len()} lists, has ${layer.size}" }
        layers.add(layer)
    }

    /** Removes the last column from facts, returning the layer with their values. */
    fun popLayer(): Layer? = layers.removeLastOrNull()

    /** Removes all but the first `arity` columns. */
    fun truncate(arity: Int) {
        while (this.arity > arity) popLayer()
    }

    /** Builds a forest of the facts present in `this` or `other` (Rust `Merge::merge`). */
    fun merge(other: Forest): Forest {
        require(arity == other.arity) { "cannot merge forests of arity $arity and ${other.arity}" }
        val reports = ReportQueue()
        reports.pushBoth(0, 0)
        val merged = ArrayList<Layer>(arity)
        for (index in 0 until arity) {
            merged.add(Kernels.union(layers[index], other.layers[index], reports, index + 1 < arity))
        }
        return of(merged)
    }

    /**
     * Filters to the facts satisfying literal and inter-column equalities, or null when
     * none do (Rust `Forest::filter`, trie.rs:157-214).
     *
     * `litFilters` names columns and the literal they must equal; each `varFilters`
     * group names columns whose values must all be equal. Each column should occur in
     * at most one constraint.
     */
    fun filter(litFilters: List<Pair<Int, ByteArray>>, varFilters: List<List<Int>>): Forest? {
        // Plan the constraints applied to each column, to visit them in order.
        val constraints = sortedMapOf<Int, Constraint>()
        for ((col, lit) in litFilters) constraints[col] = LitConstraint(lit)
        for (group in varFilters) {
            if (group.size > 1) {
                val min = group.min()
                for (col in group) if (col > min) constraints[col] = VarConstraint(min)
            }
        }

        var active = IntList(layers[0].itemCount)
        for (item in 0 until layers[0].itemCount) active.push(item)
        var cursor = 0
        for ((col, constraint) in constraints) {
            // Expand `active` from item indexes at layer `cursor` to layer `col`.
            for (layerIndex in cursor + 1..col) {
                val layer = layers[layerIndex]
                val expanded = IntList()
                for (j in 0 until active.size) {
                    for (item in layer.listLower(active[j]) until layer.listUpper(active[j])) {
                        expanded.push(item)
                    }
                }
                active = expanded
            }
            cursor = col
            when (constraint) {
                is VarConstraint -> {
                    // Naively start from all items at layer `idx`, advanced to layer `col`.
                    val idx = constraint.column
                    val itemCount = layers[idx].itemCount
                    val bounds = IntArray(2 * itemCount)
                    for (i in 0 until itemCount) {
                        bounds[2 * i] = i
                        bounds[2 * i + 1] = i + 1
                    }
                    Kernels.advanceBounds(layers.subList(idx + 1, col + 1), bounds)

                    // Align each active item at layer `col` with the layer-`idx` item it extends.
                    val aligned = IntArray(active.size)
                    var activePos = 0
                    for (i in 0 until itemCount) {
                        val upper = bounds[2 * i + 1]
                        while (activePos < active.size && active[activePos] < upper) {
                            aligned[activePos] = i
                            activePos += 1
                        }
                    }
                    Kernels.filterItems(layers[col].values, active, layers[idx].values, aligned)
                }
                is LitConstraint -> {
                    val literal = Terms()
                    literal.pushItem(constraint.literal)
                    val aligned = IntArray(active.size)
                    Kernels.filterItems(layers[col].values, active, literal, aligned)
                }
            }
        }

        // Use `active` at `cursor` to retain lists and items.
        if (active.isEmpty()) return null
        val include = BooleanArray(layers[cursor].itemCount)
        for (j in 0 until active.size) include[active[j]] = true

        val newLayers = ArrayList(layers)
        // If there are additional layers, update the unexplored layers from `active`.
        if (arity > cursor) {
            val bounds = IntArray(2 * active.size)
            for (j in 0 until active.size) {
                bounds[2 * j] = active[j]
                bounds[2 * j + 1] = active[j] + 1
            }
            for (index in cursor + 1 until arity) {
                newLayers[index] = Kernels.retainLists(newLayers[index], bounds)
            }
        }
        // In any case, update prior layers from `cursor` back to the first.
        var retain = include
        for (index in cursor downTo 0) {
            val retainOut = BooleanArray(newLayers[index].size)
            newLayers[index] = Kernels.retainItems(newLayers[index], retain, retainOut)
            retain = retainOut
        }
        return of(newLayers)
    }

    /** The subset of facts that start with some prefix in `others`. */
    fun semijoin(others: List<Forest>): FactLSM = retainJoin(others, semi = true)

    /** The subset of facts that start with no prefix in `others`. */
    fun antijoin(others: List<Forest>): FactLSM = retainJoin(others, semi = false)

    /**
     * Produces the facts of `this` based on their presence in any of `others` (Rust
     * `retain_join`/`retain_inner`, trie.rs:764-794). All of `others` must share one
     * arity, at most [arity].
     */
    fun retainJoin(others: List<Forest>, semi: Boolean): FactLSM {
        if (others.isEmpty()) return if (semi) FactLSM() else FactLSM.of(this)
        val otherArity = others[0].arity
        for (other in others) {
            require(arity >= other.arity) { "others must have arity at most $arity" }
            require(other.arity == otherArity) { "others must share one arity" }
        }

        // Collect intersection reports to determine the paths to retain.
        val include = BooleanArray(layers[otherArity - 1].itemCount) { !semi }
        for (other in others) {
            var match0 = IntList(1).also { it.push(0) }
            var match1 = IntList(1).also { it.push(0) }
            for (index in 0 until otherArity) {
                val matches = Kernels.intersect(layers[index], other.layers[index], match0, match1)
                match0 = matches.first
                match1 = matches.second
            }
            // Mark shared paths appropriately.
            for (p in 0 until match0.size) include[match0[p]] = semi
        }

        return retainCore(otherArity, include)
    }

    /**
     * Retains facts based on a bitmap for a layer of the trie (Rust `retain_core`,
     * trie.rs:799-834). The bitmap sits between `layerIndex - 1` and `layerIndex`,
     * restricting the items of the former and the lists of the latter.
     */
    fun retainCore(layerIndex: Int, include: BooleanArray): FactLSM {
        if (layerIndex > 0) require(include.size == layers[layerIndex - 1].itemCount)
        if (layerIndex < arity) require(include.size == layers[layerIndex].size)

        if (include.all { it }) return FactLSM.of(this)
        if (include.none { it }) return FactLSM()

        val newLayers = ArrayList(layers)
        // If there are additional layers, restrict them to the retained list ranges.
        if (arity > layerIndex) {
            val runs = IntList()
            var runStart = -1
            for (idx in include.indices) {
                if (include[idx]) {
                    if (runStart < 0) runStart = idx
                } else if (runStart >= 0) {
                    runs.push(runStart)
                    runs.push(idx)
                    runStart = -1
                }
            }
            if (runStart >= 0) {
                runs.push(runStart)
                runs.push(include.size)
            }
            val bounds = runs.toIntArray()
            for (index in layerIndex until arity) {
                newLayers[index] = Kernels.retainLists(newLayers[index], bounds)
            }
        }
        // In any case, update prior layers from `layerIndex` back to the first.
        var retain = include
        for (index in layerIndex - 1 downTo 0) {
            if (retain.all { it }) return FactLSM.of(Forest(newLayers))
            val retainOut = BooleanArray(newLayers[index].size)
            newLayers[index] = Kernels.retainItems(newLayers[index], retain, retainOut)
            retain = retainOut
        }
        return FactLSM.of(Forest(newLayers))
    }

    private sealed interface Constraint
    private class LitConstraint(val literal: ByteArray) : Constraint
    private class VarConstraint(val column: Int) : Constraint

    companion object {
        /**
         * Validates and assembles layers into a forest (Rust `TryFrom<Vec<Rc<Layer>>>`):
         * every layer non-empty, and each layer with as many lists as its predecessor
         * has items.
         */
        fun of(layers: List<Layer>): Forest {
            for (layer in layers) require(!layer.isEmpty()) { "layers must be non-empty" }
            for (index in 1 until layers.size) {
                require(layers[index - 1].itemCount == layers[index].size) {
                    "layer $index must have ${layers[index - 1].itemCount} lists, has ${layers[index].size}"
                }
            }
            return Forest(layers.toMutableList())
        }

        /**
         * Forms a forest from columns of identical lengths, or null when the columns are
         * empty (Rust `Forest::from_columns`). An empty column list yields the forest of
         * one 0-ary fact.
         */
        fun fromColumns(columns: List<Terms>): Forest? {
            for (column in columns) {
                require(column.size == columns[0].size) { "columns must have equal lengths" }
            }
            if (columns.any { it.isEmpty() }) return null
            val count = columns.firstOrNull()?.size ?: 0
            val groups = IntArray(count)
            val indexs = IntArray(count) { it }
            val layers = columns.mapIndexed { index, column ->
                Kernels.sort(column, groups, indexs, index == columns.size - 1)
            }
            return of(layers)
        }
    }
}
