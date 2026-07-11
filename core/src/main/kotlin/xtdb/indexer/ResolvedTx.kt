package xtdb.indexer

import kotlinx.coroutines.CompletableDeferred
import org.apache.arrow.memory.BufferAllocator
import xtdb.api.TransactionKey
import xtdb.api.TransactionResult
import xtdb.api.log.DbOp
import xtdb.api.log.MessageId
import xtdb.api.log.ReplicaMessage
import xtdb.arrow.Relation
import xtdb.arrow.VectorType
import xtdb.database.ExternalSourceToken
import xtdb.indexer.LiveTable.Companion.logRelTypes
import xtdb.segment.MemorySegment
import xtdb.table.TableRef
import xtdb.trie.ColumnName
import xtdb.trie.MemoryHashTrie
import xtdb.util.closeAll
import xtdb.util.safelyOpening

/**
 * A committed-but-not-yet-durable transaction, held in memory between the resolver committing it and the
 * replica-log producer confirming it durable. Carries the salient facts of the [OpenTx] that produced it
 * — its key, result, any db-op, source-log position and external-source token — plus ownership of its
 * table relations. Distinct from [OpenTx] on purpose: a resolved tx is no longer *open* — it can't be
 * written to or queried. It exists only to be (a) read by later txs resolving behind it (read-your-writes
 * across the in-flight batch) and (b) imported into the durable live tables once its replica-log commit
 * lands.
 *
 * The relations are the [OpenTx]'s own, ownership transferred out at [stage] (see `OpenTx.sealTables`) —
 * a reference move, not a slice: re-materializing every vector in the tree cost real time per tx, and the
 * trie already points at the relation's `_iid` vector so it moves as-is. So a ResolvedTx outlives its
 * OpenTx — the resolver closes the (now table-less) OpenTx right after staging — and its lifetime is its
 * own (freed at [close], on promote/teardown).
 *
 * An ext-source tx carries the [pending] deferred its submitter awaits: it is completed with this tx's
 * [txResult] once the replica-log commit settles, or failed on any path where that will never happen
 * (writer throw, append fault, term cancellation, undelivered send).
 * Source-log txs carry null — durability is confirmed by the replica-log commit itself, and nobody
 * awaits durability per-tx on that path.
 */
class ResolvedTx private constructor(
    val txKey: TransactionKey,
    val srcMsgId: MessageId,
    val txResult: TransactionResult,
    val externalSourceToken: ExternalSourceToken?,
    val pending: CompletableDeferred<TransactionResult>?,
    private val dbOp: DbOp?,
    private val tables: Map<TableRef, Table>,
) : AutoCloseable {

    /** One table's staged writes: the tx's own relation (ownership moved at [stage]) plus its iid trie. */
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

    /**
     * Assemble this tx's replica-log message, serializing the table slices to Arrow IPC here — at seal,
     * on the drain path — rather than eagerly at resolve, so the relation→bytes cost stays off the
     * resolver's hot path. The leader imports from the slices directly ([LiveIndex.commitTx]); these
     * bytes exist only for the replica log.
     */
    fun toReplicaMessage(): ReplicaMessage.ResolvedTx {
        val (committed, error) = when (txResult) {
            is TransactionResult.Committed -> true to null
            is TransactionResult.Aborted -> false to txResult.error
        }

        return ReplicaMessage.ResolvedTx(
            txKey.txId, txKey.systemTime, committed, error,
            tableData = tables.entries.associate { (ref, table) -> ref.schemaAndTable to table.relation.asArrowStream },
            dbOp = dbOp,
            externalSourceToken = externalSourceToken,
            srcMsgId = srcMsgId,
        )
    }

    override fun close() = tables.values.closeAll()

    companion object {
        /**
         * Resolve a committed [openTx]: take ownership of its table relations (a reference move — see
         * `OpenTx.sealTables`) so they outlive the OpenTx. The caller closes the OpenTx after.
         */
        @JvmStatic
        fun stage(
            openTx: OpenTx, srcMsgId: MessageId,
            txResult: TransactionResult, dbOp: DbOp?,
            pending: CompletableDeferred<TransactionResult>?,
        ): ResolvedTx =
            ResolvedTx(
                openTx.txKey, srcMsgId, txResult, openTx.externalSourceToken, pending, dbOp,
                // Every table the tx touched, including 0-row ones: `CREATE TABLE` declares columns with no
                // rows, and it must register in the durable index on promotion. `Table.openSnapshot` drops
                // the empty relation for row-reads, but the table's existence still reaches resolution via
                // its `columnTypes` (see `Snapshot.open`), and `toReplicaMessage` serializes it for the replica.
                openTx.sealTables().associateBy { it.ref }
            )
    }
}
