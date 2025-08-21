package xtdb.operator.scan

import org.apache.arrow.vector.VectorLoader
import xtdb.BufferPool
import xtdb.log.proto.TemporalMetadata
import xtdb.metadata.PageMetadata
import xtdb.trie.MemoryHashTrie
import xtdb.arrow.RelationReader
import xtdb.metadata.UNBOUND_TEMPORAL_METADATA
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