package xtdb.trie

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Schema
import xtdb.BufferPool
import xtdb.arrow.Relation
import xtdb.arrow.RelationReader
import xtdb.compactor.resolveSameSystemTimeEvents
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

    fun openPage(al: BufferAllocator, leaf: L): RelationReader

    class BufferPoolSegment private constructor(
        private val bp: BufferPool, override val pageMetadata: PageMetadata,
        table: TableRef, trieKey: TrieKey, val pageIdxPredicate: IntPredicate?,
    ) : ISegment<ArrowHashTrie.Leaf> {
        val dataFilePath = table.dataFilePath(trieKey)
        override val trie get() = pageMetadata.trie
        private val level = Trie.parseKey(trieKey).level

        override val schema: Schema = bp.getFooter(dataFilePath).schema

        override fun openPage(al: BufferAllocator, leaf: ArrowHashTrie.Leaf): RelationReader =
            bp.getRecordBatch(dataFilePath, leaf.dataPageIndex).use { rb ->
                Relation.fromRecordBatch(al, schema, rb)
                    .let { standardRel ->
                        if (level == 0L)
                            resolveSameSystemTimeEvents(al, standardRel)
                                .also { standardRel.close() }
                        else standardRel
                    }
            }

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

    class Memory @JvmOverloads constructor(
        override val trie: MemoryHashTrie, val rel: RelationReader, val closeRel: Boolean = true
    ) : ISegment<MemoryHashTrie.Leaf> {
        override val schema: Schema get() = rel.schema

        override val pageMetadata: PageMetadata? = null

        override fun openPage(al: BufferAllocator, leaf: MemoryHashTrie.Leaf) =
            rel.select(leaf.data).openSlice(al)

        override fun close() {
            if (closeRel)
                rel.close()
        }
    }

    class Local(
        al: BufferAllocator, dataFile: Path, metaFile: Path
    ) : ISegment<ArrowHashTrie.Leaf>,
        AutoCloseable {

        override val pageMetadata = PageMetadata.open(al, metaFile)
        override val trie get() = pageMetadata.trie
        private val dataLoader = Relation.loader(al, dataFile)
        override val schema: Schema get() = dataLoader.schema

        override fun openPage(al: BufferAllocator, leaf: ArrowHashTrie.Leaf): RelationReader =
            dataLoader.loadPage(leaf.dataPageIndex, al).openSlice(al)

        override fun close() {
            pageMetadata.close()
            dataLoader.close()
        }
    }
}