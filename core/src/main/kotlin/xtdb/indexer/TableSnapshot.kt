package xtdb.indexer

import org.apache.arrow.memory.BufferAllocator
import xtdb.arrow.RelationReader
import xtdb.arrow.RelationWriter
import xtdb.arrow.STRUCT_TYPE
import xtdb.arrow.VectorType
import xtdb.arrow.VectorType.Mono
import xtdb.indexer.LiveTable.Companion.logRelTypes
import xtdb.segment.MemorySegment
import xtdb.trie.ColumnName
import xtdb.trie.MemoryHashTrie
import xtdb.util.closeOnCatch
import kotlin.collections.orEmpty

class TableSnapshot(
    val columnTypes: Map<ColumnName, VectorType>,
    val segment: MemorySegment,
    val txSegment: MemorySegment? = null
) : AutoCloseable {
    val liveRelation: RelationReader get() = segment.rel
    val liveTrie: MemoryHashTrie get() = segment.trie

    val txRelation: RelationReader? get() = txSegment?.rel
    val txTrie: MemoryHashTrie? get() = txSegment?.trie

    fun columnType(col: ColumnName): VectorType = columnTypes[col] ?: VectorType.Null

    val types: Map<ColumnName, VectorType> get() = columnTypes

    override fun close() {
        segment.rel.close()
        txSegment?.rel?.close()
    }

    companion object {
        @JvmStatic
        fun open(al: BufferAllocator, liveTable: LiveTable, tableTx: OpenTx.Table?): TableSnapshot {
            // Only include tx segment if there's actual tx data
            val nonEmptyTableTx = tableTx?.takeIf { it.txRelation.rowCount > 0 }

            liveTable.liveRelation.openDirectSlice(al).closeOnCatch { wmLiveRel ->
                val wmLiveTrie = liveTable.liveTrie.withIidReader(wmLiveRel["_iid"])

                val txSegment = nonEmptyTableTx
                    ?.let {
                        it.txRelation.openDirectSlice(al).closeOnCatch { wmTxRel ->
                            val wmTxTrie = it.trie.withIidReader(wmTxRel["_iid"])
                            MemorySegment(wmTxTrie, wmTxRel)
                      }
                }

                // txRel types are a superset (includes any new columns from this tx)
                val types = nonEmptyTableTx?.txRelation?.logRelTypes ?: liveTable.liveRelation.logRelTypes

                return TableSnapshot(types.orEmpty(), MemorySegment(wmLiveTrie, wmLiveRel), txSegment)
            }
        }
    }
}