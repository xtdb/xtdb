package xtdb.trie

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Schema
import xtdb.BufferPool
import xtdb.arrow.Relation
import xtdb.arrow.RelationReader
import xtdb.compactor.resolveSameSystemTimeEvents
import xtdb.table.TableRef
import xtdb.trie.Trie.dataFilePath
import xtdb.util.closeAll
import java.nio.file.Path

interface DataRel<L> : AutoCloseable {

    val schema: Schema

    fun loadPage(leaf: L): RelationReader

    companion object {
        @JvmStatic
        fun openRels(
            al: BufferAllocator, bp: BufferPool,
            table: TableRef, trieKeys: Iterable<TrieKey>
        ): List<DataRel<ArrowHashTrie.Leaf>> =
            mutableListOf<Arrow>().also { res ->
                try {
                    trieKeys.forEach { trieKey ->
                        val parsedKey = Trie.parseKey(trieKey)
                        val dataFile = table.dataFilePath(trieKey)
                        res.add(Arrow(al, bp, dataFile, bp.getFooter(dataFile).schema, parsedKey.level))
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
        private val dataFile: Path, override val schema: Schema, private val level: Long
    ) : DataRel<ArrowHashTrie.Leaf> {

        private val relsToClose = mutableListOf<Relation>()

        override fun loadPage(leaf: ArrowHashTrie.Leaf): RelationReader =
            bp.getRecordBatch(dataFile, leaf.dataPageIndex).use { rb ->
                Relation.fromRecordBatch(al, schema, rb).let { standardRel ->
                    if (level == 0L) {
                        resolveSameSystemTimeEvents(al, standardRel).also {
                            standardRel.close()
                            relsToClose.add(it)
                        }
                    } else {
                        relsToClose.add(standardRel)
                        standardRel
                    }
                }
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
