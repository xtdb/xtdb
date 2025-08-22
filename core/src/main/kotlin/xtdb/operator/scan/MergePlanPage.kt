package xtdb.operator.scan

import org.apache.arrow.vector.VectorLoader
import xtdb.BufferPool
import xtdb.arrow.RelationReader
import xtdb.log.proto.TemporalMetadata
import xtdb.metadata.PageMetadata
import xtdb.metadata.UNBOUND_TEMPORAL_METADATA
import xtdb.trie.ISegment
import xtdb.trie.MemoryHashTrie
import java.nio.file.Path
import java.util.function.IntPredicate

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
        override val temporalMetadata: TemporalMetadata
    ) : MergePlanPage {
        override fun loadPage(rootCache: RootCache): RelationReader =
            bp.getRecordBatch(dataFilePath, pageIdx).use { rb ->
                val root = rootCache.openRoot(dataFilePath)
                VectorLoader(root).load(rb)
                RelationReader.from(root)
            }

        override fun testMetadata() = pageIdxPred.test(pageIdx)
    }

    class Memory(private val page: ISegment.Memory.Page) : MergePlanPage {
        override fun loadPage(rootCache: RootCache) = page.loadPage()

        override fun testMetadata() = true

        override val temporalMetadata: TemporalMetadata get() = UNBOUND_TEMPORAL_METADATA

    }
}