package xtdb.indexer

import org.apache.arrow.memory.BufferAllocator
import xtdb.arrow.VectorType
import xtdb.indexer.LiveTable.Companion.logRelTypes
import xtdb.segment.MemorySegment
import xtdb.api.TableRef
import xtdb.trie.ColumnName
import xtdb.util.safelyOpening
import kotlin.collections.orEmpty
import xtdb.api.tx.OpenTx

class TableSnapshot(
    val table: TableRef,
    private val columnTypes: Map<ColumnName, VectorType>,
    val segment: MemorySegment
) : AutoCloseable {
    val relation get() = segment.rel
    val trie get() = segment.trie

    // A constant of the segment: `columnTypes` is fixed at construction, so what this segment
    // contributes for a column it doesn't hold is fixed too. Held rather than recomputed because
    // `Snapshot.columnTypes` asks per column, and a wide table asks many times.
    private val absentContribution by lazy { VectorType.absentContribution(columnTypes) }

    /** The live half's contribution — its recorded type for [col], or [VectorType.absentContribution]. */
    fun contributedType(col: ColumnName): VectorType = columnTypes[col] ?: absentContribution

    /**
     * The raw record of what this one segment physically holds — no rule applied and nothing joined in.
     * A column's *declared* type is [xtdb.indexer.Snapshot.columnTypes]; this is only an input to it.
     *
     * Public for `xt.live_columns`, which reports the live index as it stands and would be defeated by
     * merging — the merged question is what `information_schema.columns` answers.
     */
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

            TableSnapshot(liveTable.table, seg.rel.logRelTypes.orEmpty(), seg)
        }

        @JvmStatic
        fun openTx(al: BufferAllocator, tableTx: OpenTx.Table): TableSnapshot? {
            if (tableTx.txRelation.rowCount == 0) return null
            return safelyOpening {
                val wmRel = open { tableTx.txRelation.openDirectSlice(al) }
                val wmTrie = tableTx.trie.withIidReader(wmRel["_iid"])
                val seg = MemorySegment(wmTrie, wmRel)

                TableSnapshot(tableTx.ref, seg.rel.logRelTypes.orEmpty(), seg)
            }
        }
    }
}
