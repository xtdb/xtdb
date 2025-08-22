package xtdb.trie

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Schema
import xtdb.BufferPool
import xtdb.arrow.Relation
import xtdb.arrow.RelationReader
import xtdb.compactor.resolveSameSystemTimeEvents
import xtdb.log.proto.TemporalMetadata
import xtdb.metadata.MetadataPredicate
import xtdb.metadata.PageMetadata
import xtdb.table.TableRef
import xtdb.trie.Trie.dataFilePath
import xtdb.trie.Trie.metaFilePath
import xtdb.util.closeOnCatch
import java.nio.file.Path
import java.util.function.IntPredicate

interface ISegment<L> : AutoCloseable {
    val trie: HashTrie<L>

    val schema: Schema

    val pageMetadata: PageMetadata?

    /**
     * Implementations of DataPage should be able to out-live the related Segment
     */
    interface Page {
        val schema: Schema

        fun openDataPage(al: BufferAllocator): RelationReader
    }

    fun page(leaf: L): Page

    class BufferPoolSegment private constructor(
        private val bp: BufferPool, override val pageMetadata: PageMetadata,
        table: TableRef, trieKey: TrieKey, val pageIdxPredicate: IntPredicate?,
    ) : ISegment<ArrowHashTrie.Leaf> {
        val dataFilePath = table.dataFilePath(trieKey)
        override val trie get() = pageMetadata.trie
        private val resolveSameSystemTimeEvents = Trie.parseKey(trieKey).level == 0L

        override val schema: Schema = bp.getFooter(dataFilePath).schema

        class Page(
            private val bp: BufferPool,
            val dataFilePath: Path, override val schema: Schema,
            val pageIndex: Int,
            val pageIdxPredicate: IntPredicate?,
            val temporalMetadata: TemporalMetadata,
            private val resolveSameSystemTimeEvents: Boolean

        ) : ISegment.Page {
            override fun openDataPage(al: BufferAllocator): RelationReader =
                bp.getRecordBatch(dataFilePath, pageIndex).use { rb ->
                    Relation.fromRecordBatch(al, schema, rb)
                        .let { standardRel ->
                            if (resolveSameSystemTimeEvents)
                                resolveSameSystemTimeEvents(al, standardRel)
                                    .also { standardRel.close() }
                            else standardRel
                        }
                }
        }

        override fun page(leaf: ArrowHashTrie.Leaf) =
            Page(
                bp, dataFilePath, schema, leaf.dataPageIndex,
                pageIdxPredicate, pageMetadata.temporalMetadata(leaf.dataPageIndex),
                resolveSameSystemTimeEvents
            )

        override fun close() = pageMetadata.close()

        companion object {

            @JvmStatic
            @JvmOverloads
            fun open(
                bp: BufferPool, mm: PageMetadata.Factory,
                table: TableRef, trieKey: TrieKey,
                metadataPredicate: MetadataPredicate? = null
            ) =
                mm.openPageMetadata(table.metaFilePath(trieKey)).closeOnCatch { pm ->
                    BufferPoolSegment(
                        bp, pm, table, trieKey,
                        metadataPredicate?.build(pm)
                    )
                }
        }
    }

    /**
     * @param rel NOTE: borrows `rel`, doesn't close it.
     */
    class Memory(override val trie: MemoryHashTrie, val rel: RelationReader) : ISegment<MemoryHashTrie.Leaf> {
        override val schema: Schema get() = rel.schema

        override val pageMetadata: PageMetadata? = null

        // this one is the exception to the rule - nothing in the Memory class that needs closing,
        // so DataPage can be an inner object.
        inner class Page(val leaf: MemoryHashTrie.Leaf) : ISegment.Page {
            override val schema: Schema get() = this@Memory.schema

            fun loadPage() = rel.select(leaf.mergeSort(trie))

            override fun openDataPage(al: BufferAllocator) = loadPage().openSlice(al)
        }

        override fun page(leaf: MemoryHashTrie.Leaf) = Page(leaf)

        override fun close() = Unit
    }
}