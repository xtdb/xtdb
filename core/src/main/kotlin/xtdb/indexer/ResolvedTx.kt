package xtdb.indexer

import kotlinx.coroutines.CompletableDeferred
import xtdb.api.TransactionKey
import xtdb.api.TransactionResult
import xtdb.api.log.DbOp
import xtdb.types.MessageId
import xtdb.api.log.ReplicaMessage
import xtdb.arrow.VectorType
import xtdb.api.tx.ExternalSourceToken
import xtdb.indexer.LiveTable.Companion.logRelTypes
import xtdb.api.TableRef
import xtdb.trie.ColumnName
import xtdb.util.closeAll
import xtdb.api.tx.OpenTx

/**
 * A committed-but-not-yet-durable transaction, held in memory between the resolver committing it and the
 * replica-log producer confirming it durable. Carries the salient facts of the [OpenTx] that produced it
 * — its key, result, any db-op, source-log position and external-source token — plus ownership of its
 * table relations. Distinct from [OpenTx] on purpose: a resolved tx is no longer *open* — it can't be
 * written to or queried. It exists only to be (a) read by later txs resolving behind it (read-your-writes
 * across the in-flight batch) and (b) imported into the durable live tables once its replica-log commit
 * lands.
 *
 * Each table arrives as the value that table takes if this tx applies: [stage] commits the transient the
 * [OpenTx] was writing into and moves it here (see `OpenTx.sealTables`), so a ResolvedTx outlives its
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
    val dbOp: DbOp?,
    private val tables: MutableMap<TableRef, Table>,
) : AutoCloseable {

    /**
     * One table's staged writes, as the value that table takes if this tx applies.
     *
     * [rowsFrom] is where this tx's own rows start in it — the rows below belong to the transactions it
     * resolved behind, which is why the whole value serves read-your-writes while only the range above
     * [rowsFrom] goes to the replica log.
     */
    class Table(val ref: TableRef, val liveTable: LiveTable, val rowsFrom: Int) : AutoCloseable {

        /**
         * How many rows *this* tx wrote — not what the table now holds, which includes every tx it
         * resolved behind. The block gauge and the replica-log page are both this tx's rows alone.
         */
        val rowCount get() = liveTable.relation.rowCount - rowsFrom

        /**
         * The tx's declared columns for this table, present even at 0 rows (e.g. `CREATE TABLE`).
         * Resolution needs these for table *existence*: a table nothing has written to declares no columns
         * through its segment, so a freshly-created empty one can't be learned from the snapshot data alone.
         */
        val columnTypes: Map<ColumnName, VectorType> get() = liveTable.relation.logRelTypes.orEmpty()

        override fun close() = liveTable.close()
    }

    val allTables: Collection<Table> get() = tables.values

    /** This tx's value for [table], for a later tx resolving behind it to derive its own from. */
    fun liveTable(table: TableRef): LiveTable? = tables[table]?.liveTable

    /**
     * Hands this tx's table values over, the way `OpenTx.sealTables` hands them here: ownership is
     * presence in a map, so a tx discarded on teardown releases everything with no was-applied flag to
     * guard.
     *
     * The result is the caller's to free, and MUST be taken under `closeAllOnCatch` — [LiveIndex.applyTx]
     * drains it as it installs, so what is left in it if that throws is exactly what never landed.
     */
    fun sealTables(): MutableMap<TableRef, LiveTable> =
        tables.mapValuesTo(mutableMapOf()) { it.value.liveTable }.also { tables.clear() }

    /**
     * Assemble this tx's replica-log message, serializing each table's own row range to Arrow IPC here —
     * at seal, on the drain path — rather than eagerly at resolve, so the relation→bytes cost stays off
     * the resolver's hot path. The leader takes the table values directly ([LiveIndex.applyTx]); these
     * bytes exist only for the replica log.
     */
    fun toReplicaMessage(termId: Long): ReplicaMessage.ResolvedTx {
        val (committed, error) = when (txResult) {
            is TransactionResult.Committed -> true to null
            is TransactionResult.Aborted -> false to txResult.error
        }

        return ReplicaMessage.ResolvedTx(
            txKey.txId, txKey.systemTime, committed, error,
            tableData = tables.entries.associate { (ref, table) ->
                ref.schemaAndTable to
                        table.liveTable.relation.asArrowStream(table.rowsFrom, table.rowCount)
            },
            dbOp = dbOp,
            externalSourceToken = externalSourceToken,
            srcMsgId = srcMsgId,
            termId = termId,
        )
    }

    override fun close() = tables.values.closeAll()

    companion object {
        /**
         * Resolve a committed [openTx]: commit each table's transient and take ownership of the result
         * (see `OpenTx.sealTables`) so it outlives the OpenTx. The caller closes the OpenTx after.
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
                // rows, and it must register in the durable index on promotion. Its segment declares no
                // columns, so the table's existence reaches resolution via its `columnTypes` instead
                // (see `Snapshot.open`), and `toReplicaMessage` serializes it for the replica.
                openTx.sealTables().associateByTo(mutableMapOf()) { it.ref }
            )
    }
}
