package xtdb.indexer

import org.apache.arrow.memory.BufferAllocator
import xtdb.arrow.RelationReader
import xtdb.arrow.VectorType
import xtdb.indexer.LiveTable.Companion.logRelTypes
import xtdb.segment.MemorySegment
import xtdb.trie.ColumnName
import xtdb.trie.MemoryHashTrie
import xtdb.util.safelyOpening
import kotlin.collections.orEmpty

class TableSnapshot(
    val columnTypes: Map<ColumnName, VectorType>,
    val segment: MemorySegment?,
    val txSegment: MemorySegment? = null
) : AutoCloseable {
    val liveRelation get() = segment?.rel
    val liveTrie get() = segment?.trie

    val txRelation: RelationReader? get() = txSegment?.rel
    val txTrie: MemoryHashTrie? get() = txSegment?.trie

    fun columnType(col: ColumnName): VectorType = columnTypes[col] ?: VectorType.Null

    val types: Map<ColumnName, VectorType> get() = columnTypes

    override fun close() {
        segment?.rel?.close()
        txSegment?.rel?.close()
    }

    companion object {
        @JvmStatic
        fun open(al: BufferAllocator, liveTable: LiveTable?, tableTx: OpenTx.Table?): TableSnapshot = safelyOpening {
            val liveRelSeg = liveTable?.let {
                val wmLiveRel = open { liveTable.liveRelation.openDirectSlice(al) }
                val wmLiveTrie = liveTable.liveTrie.withIidReader(wmLiveRel["_iid"])

                MemorySegment(wmLiveTrie, wmLiveRel)
            }

            // Only include tx segment if there's actual tx data
            val txSegment = tableTx?.takeIf { it.txRelation.rowCount > 0 }
                ?.let {
                    val wmTxRel = open { it.txRelation.openDirectSlice(al) }
                    val wmTxTrie = it.trie.withIidReader(wmTxRel["_iid"])
                    MemorySegment(wmTxTrie, wmTxRel)
                }

            val types = txSegment?.rel?.logRelTypes ?: liveRelSeg?.rel?.logRelTypes

            TableSnapshot(types.orEmpty(), liveRelSeg, txSegment)
        }
    }
}