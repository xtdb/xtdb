package xtdb.segment

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Schema
import xtdb.arrow.Relation
import xtdb.arrow.RelationReader
import xtdb.compactor.resolveSameSystemTimeEvents
import xtdb.log.proto.TemporalMetadata
import xtdb.metadata.MetadataPredicate
import xtdb.metadata.PageMetadata
import xtdb.storage.BufferPool
import xtdb.table.TableRef
import xtdb.trie.ArrowHashTrie
import xtdb.trie.Trie
import xtdb.trie.Trie.dataFilePath
import xtdb.trie.Trie.metaFilePath
import xtdb.trie.TrieKey
import xtdb.util.closeOnCatch
import java.nio.file.Path
import java.util.function.IntPredicate

class BufferPoolSegment private constructor(
    al: BufferAllocator, private val bp: BufferPool, override val pageMetadata: PageMetadata,
    table: TableRef, trieKey: TrieKey, val pageIdxPredicate: IntPredicate?,
) : Segment<ArrowHashTrie.Leaf> {
    val dataFilePath = table.dataFilePath(trieKey)
    override val trie get() = pageMetadata.trie
    private val resolveSameSystemTimeEvents = Trie.parseKey(trieKey).level == 0L

    override val schema: Schema = bp.getFooter(dataFilePath).schema

    private val dataRel = Relation(al, schema)
    private var currentDataPageIndex: Int = -1

    inner class Page(
        private val bp: BufferPool,
        val dataFilePath: Path, override val schema: Schema,
        val pageIndex: Int,
        val pageIdxPredicate: IntPredicate?,
        override val temporalMetadata: TemporalMetadata,
        private val resolveSameSystemTimeEvents: Boolean
    ) : Segment.Page {

        private var testMetadata: Boolean? = null

        override fun testMetadata() =
            testMetadata ?: (pageIdxPredicate?.test(pageIndex) ?: true).also { testMetadata = it }

        override fun loadDataPage(al: BufferAllocator): RelationReader =
            if (currentDataPageIndex == pageIndex) dataRel
            else {
                currentDataPageIndex = pageIndex
                bp.getRecordBatch(dataFilePath, pageIndex).use { rb ->
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
    }

    private val pages = arrayOfNulls<Page>(pageMetadata.pageCount)

    override fun page(leaf: ArrowHashTrie.Leaf): Page {
        val dataPageIndex = leaf.dataPageIndex

        return pages[dataPageIndex]
            ?: Page(
                bp, dataFilePath, schema, dataPageIndex,
                pageIdxPredicate, pageMetadata.temporalMetadata(dataPageIndex),
                resolveSameSystemTimeEvents
            ).also { pages[dataPageIndex] = it }
    }

    override fun close() {
        dataRel.close()
        pageMetadata.close()
    }

    companion object {

        @JvmStatic
        @JvmOverloads
        fun open(
            al: BufferAllocator, bp: BufferPool, mm: PageMetadata.Factory,
            table: TableRef, trieKey: TrieKey,
            metadataPredicate: MetadataPredicate? = null
        ) =
            mm.openPageMetadata(table.metaFilePath(trieKey)).closeOnCatch { pm ->
                BufferPoolSegment(al, bp, pm, table, trieKey, metadataPredicate?.build(pm))
            }
    }
}