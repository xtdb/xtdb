package xtdb.trie

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Schema
import xtdb.BufferPool
import xtdb.arrow.Relation
import xtdb.arrow.RelationReader
import xtdb.trie.TrieWriter.Companion.dataFilePath
import xtdb.util.closeAll
import java.nio.file.Path

interface DataRel<L> : AutoCloseable {

    val schema: Schema

    fun loadPage(leaf: L): RelationReader

    companion object {
        @JvmStatic
        fun openRels(
            al: BufferAllocator, bp: BufferPool,
            tableName: TableName, trieKeys: Iterable<TrieKey>
        ): List<DataRel<*>> =
            mutableListOf<Arrow>().also { res ->
                try {
                    trieKeys.forEach { trieKey ->
                        val dataFile = dataFilePath(tableName, trieKey)
                        res.add(Arrow(al, bp, dataFile, bp.getFooter(dataFile).schema))
                    }
                } catch (e: Throwable) {
                    res.closeAll()
                    throw e
                }
            }

        @JvmStatic
        fun live(liveRel: RelationReader) = Live(liveRel)
    }

    class Arrow(
        private val al: BufferAllocator, private val bp: BufferPool,
        private val dataFile: Path, override val schema: Schema
    ) : DataRel<ArrowHashTrie.Leaf> {

        private val relsToClose = mutableListOf<Relation>()

        override fun loadPage(leaf: ArrowHashTrie.Leaf): RelationReader =
            bp.getRecordBatch(dataFile, leaf.dataPageIndex).use { rb ->
                Relation.fromRecordBatch(al, schema, rb).also { relsToClose.add(it) }
            }

        override fun close() {
            relsToClose.closeAll()
        }
    }

    class Live(private val liveRel: RelationReader) : DataRel<MemoryHashTrie.Leaf> {
        override val schema get() = liveRel.schema

        override fun loadPage(leaf: MemoryHashTrie.Leaf) = liveRel.select(leaf.data)

        override fun close() {
        }
    }
}
