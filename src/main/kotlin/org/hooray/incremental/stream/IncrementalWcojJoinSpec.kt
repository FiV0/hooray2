package org.hooray.incremental.stream

import org.hooray.incremental.CompiledTriplePattern

/**
 * Specification of an incremental WCOJ source — the structured value
 * the Clojure analysis phase produces and the Kotlin runtime consumes.
 *
 * `patterns` are the compiled where-clause patterns (one per clause).
 * `levels` is the number of canonical variable positions.
 * `canonicalOrder` is the canonical permutation: position i in a
 * canonical tuple holds the value of variable `canonicalOrder[i]`.
 * In v1 this is always `(0 until levels)` — present in the type so
 * later changes don't reshape the spec.
 */
data class IncrementalWcojJoinSpec(
    val patterns: List<CompiledTriplePattern>,
    val levels: Int,
    val canonicalOrder: List<Int>
) {
    init {
        require(patterns.isNotEmpty()) { "patterns must not be empty" }
        require(levels >= 1) { "levels must be at least 1, got $levels" }
        require(canonicalOrder.size == levels) {
            "canonicalOrder size (${canonicalOrder.size}) must match levels ($levels)"
        }
    }
}

/** Compile a spec into a runnable CircuitSource. */
fun buildWcojSource(spec: IncrementalWcojJoinSpec): IncrementalWcojJoinSource =
    IncrementalWcojJoinSource(spec.patterns, spec.levels)
