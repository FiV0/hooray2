package org.hooray.incremental.stream

import org.hooray.algo.Prefix

/**
 * Structured description of a target index layout for `mapIndex`.
 *
 * `keyLevels` and `valueLevels` are tuple positions that form the key and
 * value of the produced `IndexedZSet`. `fixedPrefix` lets a spec pin
 * leading positions to constant values (e.g., for a join branch with a
 * known attribute).
 *
 * The phantom type parameters (`T`, `K`, `V`) carry the input row, key,
 * and value types so callers can wire `mapIndex` without unchecked casts.
 */
data class IndexSpec<T, K, V>(
    val name: String,
    val keyLevels: List<Int>,
    val valueLevels: List<Int>,
    val fixedPrefix: Prefix = emptyList()
)
