package org.hooray.facts

/**
 * An evolving set of facts, port of datatoad's `FactSet` (mod.rs:207-236).
 *
 * [stable] holds facts processed in prior rounds, [recent] the facts new in the current
 * round (distinct from stable), and [toAdd] facts awaiting a round of their own.
 */
class FactSet {
    val stable = FactLSM()
    var recent: Forest? = null
    val toAdd = FactLSM()

    fun len(): Int = stable.contents().sumOf { it.len() } + (recent?.len() ?: 0)

    fun active(): Boolean = recent != null || !toAdd.isEmpty()

    /** Moves `forests` into [toAdd], with no assumptions on their contents. */
    fun extend(forests: Iterable<Forest>) = toAdd.extend(forests)

    fun advance() {
        // Move recent into stable.
        stable.extend(recent)
        recent = null

        val added = toAdd.flatten()
        if (added != null) {
            // Tidy stable by an amount proportional to the work we are about to do.
            stable.tidyThrough(2 * added.len())
            // Remove from the additions any facts already in stable.
            recent = added.antijoin(stable.contents()).flatten()
        }
    }
}
