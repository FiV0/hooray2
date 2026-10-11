package org.hooray.facts

/**
 * A potentially empty list of non-empty fact containers, each sorted and distinct.
 *
 * Port of datatoad's `FactLSM<Forest<Terms>>` (mod.rs:248-329). The log-structured merge
 * structure merges any containers whose counts are within a factor of two, keeping the
 * container count logarithmic and the contained fact count at most twice the number of
 * distinct facts.
 */
class FactLSM {
    private val layers = mutableListOf<Forest>()

    fun isEmpty(): Boolean {
        // Each container is meant to be non-empty, making the LSM non-empty when `layers` is.
        for (layer in layers) check(!layer.isEmpty())
        return layers.isEmpty()
    }

    fun push(forest: Forest) {
        if (!forest.isEmpty()) {
            layers.add(forest)
            tidy()
        }
    }

    fun contents(): List<Forest> = layers

    /** Flattens the layers into one layer, and takes it. */
    fun flatten(): Forest? {
        sortDescending()
        while (layers.size > 1) {
            val x = layers.removeLast()
            val y = layers.removeLast()
            layers.add(x.merge(y))
            sortDescending()
        }
        return layers.removeLastOrNull()
    }

    fun extend(forests: Iterable<Forest>) {
        for (forest in forests) if (!forest.isEmpty()) layers.add(forest)
        tidy()
    }

    fun extend(forest: Forest?) {
        if (forest != null) push(forest)
    }

    /** Drains `other` into `this`. */
    fun extend(other: FactLSM) {
        layers.addAll(other.layers)
        other.layers.clear()
        tidy()
    }

    /** Ensures layers double in size and at most one is smaller than `size`. */
    fun tidyThrough(size: Int) {
        sortDescending()
        while (layers.size > 1 && layers[layers.size - 2].len() < size) {
            val x = layers.removeLast()
            val y = layers.removeLast()
            layers.add(x.merge(y))
            sortDescending()
        }
        tidy()
    }

    /** Ensures layers double in size. */
    private fun tidy() {
        if (layers.isNotEmpty()) {
            for (layer in layers) {
                require(layer.arity == layers[0].arity) { "all layers must share one arity" }
            }
        }
        sortDescending()
        while (true) {
            var pos = -1
            for (i in 1 until layers.size) {
                if (layers[i - 1].len() < 2 * layers[i].len()) {
                    pos = i - 1
                    break
                }
            }
            if (pos < 0) break
            while (layers.size > pos + 1) {
                val x = layers.removeLast()
                val y = layers.removeLast()
                layers.add(x.merge(y))
                sortDescending()
            }
        }
    }

    private fun sortDescending() {
        layers.sortByDescending { it.len() }
    }

    companion object {
        /** Rust `From<F>`: the empty LSM for an empty forest. */
        fun of(forest: Forest): FactLSM =
            FactLSM().also { if (!forest.isEmpty()) it.layers.add(forest) }
    }
}
