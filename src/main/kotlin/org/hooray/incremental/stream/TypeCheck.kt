package org.hooray.incremental.stream

/**
 * Raised when an analysis-pass invariant on a stream spec is violated.
 * Aborts circuit construction with a message that names the offending
 * spec element.
 */
class WcojTypeError(message: String) : IllegalArgumentException(message)

/**
 * Analysis pass over `IncrementalWcojJoinSpec`. Verifies:
 *  - `canonicalOrder` is a permutation of `[0, levels)`;
 *  - every pattern's variable indexes fall inside `[0, levels)`;
 *  - every variable in `[0, levels)` is bound by at least one pattern.
 *
 * In v1 the key type for every join site is `Any` (CompiledTriplePattern
 * carries no static schema), so a strict key-type unification check
 * collapses to these structural invariants. Later, when derived
 * relations gain richer types, this pass will grow real type
 * unification — keeping it a separate function makes that growth
 * non-breaking.
 */
fun typeCheck(spec: IncrementalWcojJoinSpec) {
    val expected = (0 until spec.levels).toSet()

    val canonSet = spec.canonicalOrder.toSet()
    if (canonSet != expected || spec.canonicalOrder.size != spec.levels) {
        throw WcojTypeError(
            "canonicalOrder is not a permutation of [0, ${spec.levels}): ${spec.canonicalOrder}"
        )
    }

    for ((i, pattern) in spec.patterns.withIndex()) {
        val vars = pattern.variableIndexes()
        val outOfRange = vars.filter { it !in expected }
        if (outOfRange.isNotEmpty()) {
            throw WcojTypeError(
                "pattern $i has variable index out of range [0, ${spec.levels}): $outOfRange"
            )
        }
    }

    val bound = spec.patterns.flatMap { it.variableIndexes() }.toSet()
    val unbound = expected - bound
    if (unbound.isNotEmpty()) {
        throw WcojTypeError(
            "variables $unbound are not bound by any pattern"
        )
    }
}
