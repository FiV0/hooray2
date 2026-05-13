package org.hooray.incremental.stream

/**
 * Physical index layouts a relation can expose.
 *
 * BaseRelations back AEV/AVE in v1 (matching the current ZSetIndices
 * shape). EAV and VAE are part of the vocabulary so analysis can refer
 * to them and so future relations (e.g., derived) can opt in.
 */
enum class IndexLayout { EAV, AEV, AVE, VAE }

/**
 * Source-of-tuples relation that participates in a circuit. The relation
 * exposes a canonical (natural-order) ZSet stream plus zero or more
 * physical index views. Permutation choices live on the relation —
 * generic Stream<T> does not know which indexes exist.
 */
interface Relation<Row> {
    val id: NodeId
    val canonicalStream: ZSetStream<Row>

    fun availableIndexes(): Set<IndexLayout>
    fun canProvide(layout: IndexLayout): Boolean = layout in availableIndexes()
    fun index(layout: IndexLayout): IndexedZSetStream<*>
}

/**
 * A base relation backed by a triple-pattern view over the circuit's
 * single ZSetIndices input. v1 surfaces AEV and AVE only; EAV and VAE
 * are not in the on-disk index set yet.
 *
 * Actual data flow (input wiring, deltas, integration backing) lands in
 * later operator tasks. This class establishes the relation-shaped
 * boundary so analysis and join expansion can refer to it.
 */
class BaseRelation(
    override val id: NodeId,
    override val label: String
) : Node, Relation<List<Any>> {
    private val self: Node = this

    override val canonicalStream: ZSetStream<List<Any>> = SimpleStream(self)

    private val aevStream: IndexedZSetStream<Any> = SimpleStream(self)
    private val aveStream: IndexedZSetStream<Any> = SimpleStream(self)

    override fun availableIndexes(): Set<IndexLayout> =
        setOf(IndexLayout.AEV, IndexLayout.AVE)

    override fun index(layout: IndexLayout): IndexedZSetStream<*> = when (layout) {
        IndexLayout.AEV -> aevStream
        IndexLayout.AVE -> aveStream
        IndexLayout.EAV, IndexLayout.VAE ->
            throw IllegalArgumentException("BaseRelation $label does not provide $layout")
    }
}
