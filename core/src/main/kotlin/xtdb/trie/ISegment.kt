package xtdb.trie

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Schema
import xtdb.BufferPool
import xtdb.arrow.Relation
import xtdb.arrow.RelationReader
import xtdb.compactor.resolveSameSystemTimeEvents
import xtdb.table.TableRef
import xtdb.trie.Trie.dataFilePath
import xtdb.trie.Trie.metaFilePath
import xtdb.util.closeOnCatch
import java.nio.file.Path

interface ISegment<L> : AutoCloseable {
    val trie: HashTrie<L>

    val schema: Schema

    fun openPage(leaf: L): RelationReader

    companion object {

        fun BufferPool.openTrie(al: BufferAllocator, metaFilePath: Path): ArrowHashTrie =
            getRecordBatch(metaFilePath, 0)
                .use { rb ->
                    val footer = getFooter(metaFilePath)
                    Relation.fromRecordBatch(al, footer.schema, rb)
                        .use { rel -> rel["nodes"].openSlice(al) }
                }
                .closeOnCatch { ArrowHashTrie(it, true) }

        @JvmStatic
        fun BufferPool.openSegment(al: BufferAllocator, table: TableRef, trieKey: TrieKey) =
            object : ISegment<ArrowHashTrie.Leaf> {
                val dataFile = table.dataFilePath(trieKey)
                val level = Trie.parseKey(trieKey).level

                override val schema: Schema
                    get() = getFooter(dataFile).schema

                override val trie = openTrie(al, table.metaFilePath(trieKey))

                override fun openPage(leaf: ArrowHashTrie.Leaf): RelationReader =
                    getRecordBatch(dataFile, leaf.dataPageIndex).use { rb ->
                        Relation.fromRecordBatch(al, schema, rb)
                            .let { standardRel ->
                                if (level == 0L)
                                    resolveSameSystemTimeEvents(al, standardRel)
                                        .also { standardRel.close() }
                                else standardRel
                            }
                    }

                override fun close() = trie.close()
            }
    }

    class Memory(
        private val al: BufferAllocator,
        override val trie: MemoryHashTrie,
        val rel: RelationReader,
    ) : ISegment<MemoryHashTrie.Leaf> {
        override val schema: Schema get() = rel.schema

        override fun openPage(leaf: MemoryHashTrie.Leaf): RelationReader {
            return rel.select(leaf.data).openSlice(al)
        }

        override fun close() = rel.close()

    }

    class Local(
        private val al: BufferAllocator, dataFile: Path, metaFile: Path
    ) : ISegment<ArrowHashTrie.Leaf>, AutoCloseable {

        private val metaRel: Relation
        override val trie: HashTrie<ArrowHashTrie.Leaf>
        private val dataLoader = Relation.loader(al, dataFile)
        override val schema: Schema get() = dataLoader.schema

        init {
            Relation.loader(al, metaFile).use { loader ->
                loader.loadPage(0, al).closeOnCatch { metaRel ->
                    this.metaRel = metaRel
                    trie = ArrowHashTrie(metaRel["nodes"])
                }
            }
        }

        override fun openPage(leaf: ArrowHashTrie.Leaf): RelationReader =
            dataLoader.loadPage(leaf.dataPageIndex, al)

        override fun close() {
            dataLoader.close()
            metaRel.close()
        }
    }
}