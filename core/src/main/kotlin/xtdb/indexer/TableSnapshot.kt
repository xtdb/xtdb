package xtdb.indexer

import org.apache.arrow.memory.BufferAllocator
import xtdb.arrow.VectorType
import xtdb.indexer.LiveTable.Companion.logRelTypes
import xtdb.segment.MemorySegment
import xtdb.trie.ColumnName
import xtdb.util.safelyOpening
import kotlin.collections.orEmpty

class TableSnapshot(
    val columnTypes: Map<ColumnName, VectorType>,
    val segment: MemorySegment
) : AutoCloseable {
    val relation get() = segment.rel
    val trie get() = segment.trie

    fun columnType(col: ColumnName): VectorType = columnTypes[col] ?: VectorType.Null

    val types: Map<ColumnName, VectorType> get() = columnTypes

    override fun close() {
        segment.rel.close()
    }

    companion object {
        @JvmStatic
        fun open(al: BufferAllocator, liveTable: LiveTable): TableSnapshot = safelyOpening {
            val wmRel = open { liveTable.liveRelation.openDirectSlice(al) }
            val wmTrie = liveTable.liveTrie.withIidReader(wmRel["_iid"])
            val seg = MemorySegment(wmTrie, wmRel)

            TableSnapshot(seg.rel.logRelTypes.orEmpty(), seg)
        }

        @JvmStatic
        fun openTx(al: BufferAllocator, tableTx: OpenTx.Table): TableSnapshot? {
            if (tableTx.txRelation.rowCount == 0) return null
            return safelyOpening {
                val wmRel = open { tableTx.txRelation.openDirectSlice(al) }
                val wmTrie = tableTx.trie.withIidReader(wmRel["_iid"])
                val seg = MemorySegment(wmTrie, wmRel)

                TableSnapshot(seg.rel.logRelTypes.orEmpty(), seg)
            }
        }
    }
}