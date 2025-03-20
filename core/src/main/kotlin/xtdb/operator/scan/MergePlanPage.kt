package xtdb.operator.scan

import org.apache.arrow.vector.VectorLoader
import xtdb.BufferPool
import xtdb.log.proto.TemporalMetadata
import xtdb.metadata.PageMetadata
import xtdb.trie.MemoryHashTrie
import xtdb.util.TemporalBounds
import xtdb.vector.RelationReader
import java.nio.file.Path
import java.util.function.IntPredicate
import kotlin.Long.Companion.MAX_VALUE as MAX_LONG
import kotlin.Long.Companion.MIN_VALUE as MIN_LONG

private val UNBOUND_TEMPORAL_METADATA =
    TemporalMetadata.newBuilder()
        .setMinValidFrom(MIN_LONG)
        .setMaxValidFrom(MAX_LONG)
        .setMinValidTo(MIN_LONG)
        .setMaxValidTo(MAX_LONG)
        .setMinSystemFrom(MIN_LONG)
        .setMaxSystemFrom(MAX_LONG)
        .build()

interface Metadata {
    fun testMetadata(): Boolean
    val temporalMetadata: TemporalMetadata
}

interface MergePlanPage : Metadata {
    fun loadPage(rootCache: RootCache): RelationReader

    class Arrow(
        private val bp: BufferPool,
        private val dataFilePath: Path,
        private val pageIdxPred: IntPredicate,
        private val pageIdx: Int,
        private val pageMetadata: PageMetadata
    ) : MergePlanPage {
        override fun loadPage(rootCache: RootCache): RelationReader =
            bp.getRecordBatch(dataFilePath, pageIdx).use { rb ->
                val root = rootCache.openRoot(dataFilePath)
                VectorLoader(root).load(rb)
                RelationReader.from(root)
            }

        override fun testMetadata() = pageIdxPred.test(pageIdx)

        override val temporalMetadata get() = pageMetadata.temporalMetadata(pageIdx)
    }

    class Memory(
        private val liveRel: RelationReader, private val trie: MemoryHashTrie, private val leaf: MemoryHashTrie.Leaf
    ) : MergePlanPage {
        override fun loadPage(rootCache: RootCache): RelationReader = liveRel.select(leaf.mergeSort(trie))

        override fun testMetadata() = true

        override val temporalMetadata: TemporalMetadata get() = UNBOUND_TEMPORAL_METADATA

    }
}