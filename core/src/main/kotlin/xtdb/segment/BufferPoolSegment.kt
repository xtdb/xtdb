package xtdb.segment

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Schema
import xtdb.arrow.Relation
import xtdb.arrow.RelationReader
import xtdb.metadata.MetadataPredicate
import xtdb.metadata.PageMetadata
import xtdb.segment.Segment.PageMeta
import xtdb.segment.Segment.PageMeta.Companion.pageMeta
import xtdb.storage.BufferPool
import xtdb.table.TableSlug
import xtdb.trie.ArrowHashTrie
import xtdb.trie.Trie
import xtdb.trie.RecencyMicros
import xtdb.trie.TrieKey
import xtdb.time.LocalDateTimeUtil.asMicros
import xtdb.util.closeOnCatch

class BufferPoolSegment(
    private val al: BufferAllocator, private val bp: BufferPool, private val mm: PageMetadata.Factory,
    val slug: TableSlug, val trieKey: TrieKey, val metadataPredicate: MetadataPredicate? = null,
) : Segment<ArrowHashTrie.Leaf> {
    val dataFilePath = slug.dataFilePath(trieKey)
    private val parsedTrieKey = Trie.parseKey(trieKey)
    private val recency: RecencyMicros = parsedTrieKey.recency?.atStartOfDay()?.asMicros ?: Long.MAX_VALUE

    override val part: ByteArray?
        get() = parsedTrieKey.part?.toArray()

    override val sameSystemTimeResolved = parsedTrieKey.level != 0L

    private val footer = bp.getFooter(dataFilePath)

    override val schema: Schema = footer.schema

    private inner class DataPage(private val dataPageIndex: Int) : Segment.Page<ArrowHashTrie.Leaf> {
        private var rel: Relation? = null

        override val sameSystemTimeResolved get() = this@BufferPoolSegment.sameSystemTimeResolved

        override suspend fun loadDataPage(al: BufferAllocator): RelationReader =
            rel ?: Relation(this@BufferPoolSegment.al, schema).closeOnCatch { rel ->
                bp.getRecordBatch(dataFilePath, dataPageIndex).use { rb -> rel.load(rb) }
                rel
            }.also { rel = it }

        override fun release() {
            rel?.close()
            rel = null
        }
    }

    private val dataPages = List(footer.recordBatches.size) { DataPage(it) }

    inner class Metadata(private val pageMetadata: PageMetadata) : Segment.Metadata<ArrowHashTrie.Leaf> {

        override val trie get() = pageMetadata.trie

        private val pageIdxPredicate = metadataPredicate?.build(pageMetadata)

        private val pages = arrayOfNulls<PageMeta<ArrowHashTrie.Leaf>>(pageMetadata.pageCount)

        override fun page(leaf: ArrowHashTrie.Leaf): PageMeta<ArrowHashTrie.Leaf> {
            val dataPageIndex = leaf.dataPageIndex

            return pages[dataPageIndex]
                ?: pageMeta(
                    dataPages[dataPageIndex], this, leaf,
                    pageMetadata.temporalMetadata(dataPageIndex),
                    minOf(recency, leaf.recencyBound, pageMetadata.maxRecency(dataPageIndex) ?: Long.MAX_VALUE)
                ).also { pages[dataPageIndex] = it }
        }

        override fun testPage(leaf: ArrowHashTrie.Leaf) = pageIdxPredicate?.test(leaf.dataPageIndex) ?: true

        override fun close() = pageMetadata.close()
    }

    override suspend fun openMetadata(): Metadata = Metadata(mm.openPageMetadata(slug.metaFilePath(trieKey)))

    override fun close() {
        dataPages.forEach { it.release() }
    }
}