package xtdb.indexer

import org.apache.arrow.memory.BufferAllocator
import xtdb.api.TransactionKey
import xtdb.api.TransactionResult
import xtdb.api.log.MessageId
import xtdb.arrow.Relation
import xtdb.arrow.VectorType
import xtdb.database.ExternalSourceToken
import xtdb.indexer.LiveTable.Companion.logRelTypes
import xtdb.segment.MemorySegment
import xtdb.table.TableRef
import xtdb.trie.ColumnName
import xtdb.trie.MemoryHashTrie
import xtdb.util.closeAll
import xtdb.util.closeAllOnCatch
import xtdb.util.safelyOpening

/**
 * A committed-but-not-yet-durable transaction, staged in memory between the resolver committing it and
 * the replica-log producer confirming it durable. Distinct from [OpenTx] on purpose: once staged a tx
 * is no longer *open* — it can't be written to or queried. It exists only to be (a) read by later txs
 * resolving behind it (read-your-writes across the in-flight batch) and (b) imported into the durable
 * live tables once its replica-log commit lands.
 *
 * It owns independent slices of the tx's relations, taken from the [OpenTx] at [stage] via
 * `openDirectSlice` (which transfers the buffers into the staging allocator), and a trie sharing the
 * tx's heap `rootNode` re-pointed at the slice. So a StagedTx outlives its OpenTx — the resolver closes
 * the OpenTx right after staging — and its lifetime is its own (freed at [close], on promote/teardown).
 *
 * No durability handle: the single async replica-log commit is what confirms durability, and the per-tx
 * replica-log positions for the apply cursor come back from that commit — not from here.
 */
class StagedTx private constructor(
    val txKey: TransactionKey,
    val notifyMsgId: MessageId,
    val txResult: TransactionResult,
    val externalSourceToken: ExternalSourceToken?,
    private val tables: Map<TableRef, Table>,
) : AutoCloseable {

    /** One table's staged writes: an owned slice of the tx's relation plus its iid trie. */
    class Table(val ref: TableRef, val relation: Relation, private val trie: MemoryHashTrie) : AutoCloseable {

        /**
         * A fresh, caller-owned [TableSnapshot] of these staged writes — re-sliced into [al] so it's
         * freed with the enclosing [Snapshot], leaving this table's own slice intact until promote.
         */
        fun openSnapshot(al: BufferAllocator): TableSnapshot? {
            if (relation.rowCount == 0) return null
            return safelyOpening {
                val wmRel = open { relation.openDirectSlice(al) }
                val wmTrie = trie.withIidReader(wmRel["_iid"])
                val seg = MemorySegment(wmTrie, wmRel)
                TableSnapshot(ref, seg.rel.logRelTypes.orEmpty(), seg)
            }
        }

        /**
         * The tx's declared columns for this table, present even at 0 rows (e.g. `CREATE TABLE`).
         * Resolution needs these for table *existence*: [openSnapshot] drops the empty relation, so a
         * freshly-created empty table can't be learned from the snapshot data alone.
         */
        val columnTypes: Map<ColumnName, VectorType> get() = relation.logRelTypes.orEmpty()

        override fun close() = relation.close()
    }

    val allTables: Collection<Table> get() = tables.values

    override fun close() = tables.values.closeAll()

    companion object {
        /**
         * Stage a committed [openTx]: take independent slices of its table relations into [al] (the
         * staging allocator) so they outlive the OpenTx. The caller closes the OpenTx after.
         */
        @JvmStatic
        fun stage(
            al: BufferAllocator, openTx: OpenTx, notifyMsgId: MessageId,
            txResult: TransactionResult, externalSourceToken: ExternalSourceToken?,
        ): StagedTx =
            mutableListOf<Table>().closeAllOnCatch { staged ->
                // Every table the tx touched, including 0-row ones: `CREATE TABLE` declares columns with no
                // rows, and it must register in the durable index on promotion. This matches what
                // `serializeTableData` carries (all `tableTxs`, 0-row included). `Table.openSnapshot`
                // drops the empty relation for row-reads, but the table's existence still reaches resolution
                // via its `columnTypes` (see `Snapshot.open`).
                for ((ref, tableTx) in openTx.tables) {
                    val slice = tableTx.txRelation.openDirectSlice(al)
                    staged.add(Table(ref, slice, tableTx.trie.withIidReader(slice["_iid"])))
                }

                StagedTx(openTx.txKey, notifyMsgId, txResult, externalSourceToken, staged.associateBy { it.ref })
            }
    }
}
