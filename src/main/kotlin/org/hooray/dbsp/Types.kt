package org.hooray.dbsp

import org.hooray.incremental.IntegerWeight
import org.hooray.incremental.ZSet

/**
 * The z-set type flowing through a DBSP-standard circuit: [Tuple]s carrying
 * integer weights. Sources, joins, and projections all operate on `TupleZSet`s.
 */
typealias TupleZSet = ZSet<Tuple, IntegerWeight>

/** An empty [TupleZSet]. */
fun emptyTupleZSet(): TupleZSet = ZSet.empty()
