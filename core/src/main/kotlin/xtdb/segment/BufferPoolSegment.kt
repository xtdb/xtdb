package xtdb.segment

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorLoader
import org.apache.arrow.vector.types.pojo.Schema
import xtdb.BufferPool
import xtdb.arrow.Relation
import xtdb.arrow.RelationReader
import xtdb.compactor.resolveSameSystemTimeEvents
import xtdb.log.proto.TemporalMetadata
import xtdb.metadata.MetadataPredicate
import xtdb.metadata.PageMetadata
import xtdb.operator.scan.RootCache
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
    private val bp: BufferPool, override val pageMetadata: PageMetadata,
    table: TableRef, trieKey: TrieKey, val pageIdxPredicate: IntPredicate?,
) : Segment<ArrowHashTrie.Leaf> {
    val dataFilePath = table.dataFilePath(trieKey)
    override val trie get() = pageMetadata.trie
    private val resolveSameSystemTimeEvents = Trie.parseKey(trieKey).level == 0L

    override val schema: Schema = bp.getFooter(dataFilePath).schema

    class Page(
        private val bp: BufferPool,
        val dataFilePath: Path, override val schema: Schema,
        val pageIndex: Int,
        val pageIdxPredicate: IntPredicate?,
        override val temporalMetadata: TemporalMetadata,
        private val resolveSameSystemTimeEvents: Boolean

    ) : Segment.Page {
        override fun testMetadata() = pageIdxPredicate?.test(pageIndex) ?: true

        override fun loadDataPage(rootCache: RootCache): RelationReader =
            bp.getRecordBatch(dataFilePath, pageIndex).use { rb ->
                val root = rootCache.openRoot(dataFilePath)
                VectorLoader(root).load(rb)
                RelationReader.Companion.from(root)
            }

        override fun openDataPage(al: BufferAllocator): RelationReader =
            bp.getRecordBatch(dataFilePath, pageIndex).use { rb ->
                Relation.Companion.fromRecordBatch(al, schema, rb)
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