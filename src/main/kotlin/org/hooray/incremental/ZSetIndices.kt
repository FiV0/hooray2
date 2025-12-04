package org.hooray.incremental

enum class IndexType {
    EAV,
    AEV,
    AVE,
    VAE
}

data class ZSetIndices(
    val eav: IndexedZSet<Any, IntegerWeight>,
    val aev: IndexedZSet<Any, IntegerWeight>,
    val ave: IndexedZSet<Any, IntegerWeight>,
    val vae: IndexedZSet<Any, IntegerWeight>
)

fun ZSetIndices.getByType(indexType: IndexType): IndexedZSet<Any, IntegerWeight> {
    return when (indexType) {
        IndexType.EAV -> eav
        IndexType.AEV -> aev
        IndexType.AVE -> ave
        IndexType.VAE -> vae
    }
}