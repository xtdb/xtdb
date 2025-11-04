package xtdb.segment

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Schema
import xtdb.arrow.Relation
import xtdb.arrow.RelationReader
import xtdb.compactor.resolveSameSystemTimeEvents
import xtdb.metadata.MetadataPredicate
import xtdb.metadata.PageMetadata
import xtdb.segment.Segment.PageMeta
import xtdb.segment.Segment.PageMeta.Companion.pageMeta
import xtdb.storage.BufferPool
import xtdb.table.TableRef
import xtdb.trie.ArrowHashTrie
import xtdb.trie.Trie
import xtdb.trie.Trie.dataFilePath
import xtdb.trie.Trie.metaFilePath
import xtdb.trie.TrieKey

class BufferPoolSegment(
    al: BufferAllocator, private val bp: BufferPool, private val mm: PageMetadata.Factory,
    val table: TableRef, val trieKey: TrieKey, val metadataPredicate: MetadataPredicate? = null,
) : Segment<ArrowHashTrie.Leaf> {
    val dataFilePath = table.dataFilePath(trieKey)
    private val resolveSameSystemTimeEvents = Trie.parseKey(trieKey).level == 0L

    override val part: ByteArray?
        get() = Trie.parseKey(trieKey).part?.toArray()

    override val schema: Schema = bp.getFooter(dataFilePath).schema

    private val dataRel = Relation(al, schema)
    private var currentDataPageIndex: Int = -1

    inner class Metadata(private val pageMetadata: PageMetadata) : Segment.Metadata<ArrowHashTrie.Leaf> {

        override val trie get() = pageMetadata.trie

        private val pageIdxPredicate = metadataPredicate?.build(pageMetadata)

        private val pages = arrayOfNulls<PageMeta<ArrowHashTrie.Leaf>>(pageMetadata.pageCount)

        override fun page(leaf: ArrowHashTrie.Leaf): PageMeta<ArrowHashTrie.Leaf> {
            val dataPageIndex = leaf.dataPageIndex

            return pages[dataPageIndex]
                ?: pageMeta(
                    this@BufferPoolSegment, this, leaf,
                    pageMetadata.temporalMetadata(dataPageIndex)
                ).also { pages[dataPageIndex] = it }
        }

        override fun testPage(leaf: ArrowHashTrie.Leaf) = pageIdxPredicate?.test(leaf.dataPageIndex) ?: true

        override fun close() = pageMetadata.close()
    }

    override fun openMetadata(): Metadata = Metadata(mm.openPageMetadata(table.metaFilePath(trieKey)))

    override fun loadDataPage(al: BufferAllocator, leaf: ArrowHashTrie.Leaf): RelationReader =
        if (currentDataPageIndex == leaf.dataPageIndex) dataRel
        else {
            currentDataPageIndex = leaf.dataPageIndex
            bp.getRecordBatch(dataFilePath, leaf.dataPageIndex).use { rb ->
                if (resolveSameSystemTimeEvents) {
                    dataRel.clear()

                    Relation.fromRecordBatch(al, schema, rb).use { inRel ->
                        resolveSameSystemTimeEvents(inRel, dataRel)
                    }
                } else {
                    dataRel.apply { load(rb) }
                }
            }
        }

    override fun close() {
        dataRel.close()
    }
}