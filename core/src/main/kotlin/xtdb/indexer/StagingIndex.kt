package xtdb.indexer

import org.apache.arrow.memory.BufferAllocator
import xtdb.api.TransactionKey
import xtdb.api.log.MessageId
import xtdb.api.log.ReplicaMessage
import xtdb.table.TableRef
import xtdb.util.closeAll
import xtdb.util.closeAllOnCatch

/**
 * The leader resolver's staging index: committed-but-not-yet-durable transactions, held between the
 * resolver applying a tx and the replica writer confirming the batch durable on the replica log. The
 * resolver is the sole accessor, so no lock is needed — single-threaded by construction.
 *
 * Standing state owned by [LeaderLogProcessor]: created when it becomes leader, freed in its `close()`
 * (phase two of the two-phase teardown), not by the resolver coroutine.
 *
 * Two views of the same in-flight txs:
 *
 *  - a per-tx queue ([accumulating] takes newly-resolved txs; [committing] is the batch handed to the
 *    replica writer, awaiting its durable confirmation) — drives seal / promote / the replica write.
 *  - a denormalised per-table index ([tables]) — drives resolution read-your-writes: a resolving tx
 *    reads `durable ⊕ staged ⊕ its own writes`, and the staged layer is the parked txs' writes.
 *
 * The resolver hands staging the tx's [OpenTx] at [apply]; staging owns it from then on and closes it
 * on [promote] (once durable) or on [close] (teardown). Its relations are read (copied) on demand by
 * [Table.openSnapshots], so the OpenTx must stay alive until it's promoted.
 */
internal class StagingIndex(latestCompletedTx: TransactionKey?) : AutoCloseable {

    // A staged tx: its serialized form (for the replica write + promote), the OpenTx holding its
    // (queryable) writes, and the msgId to notify on once durable. Staging owns the OpenTx once applied.
    class Staged(
        val resolvedTx: ReplicaMessage.ResolvedTx,
        internal val openTx: OpenTx,
        val notifyMsgId: MessageId,
    )

    /** The staged writes to one table, across the in-flight txs, oldest→newest (precedence order). */
    class Table {
        // Borrowed OpenTx.Table refs — the OpenTxs own them; deindexed before the OpenTx is closed.
        internal val txTables = ArrayDeque<OpenTx.Table>()

        internal fun add(tableTx: OpenTx.Table) = txTables.addLast(tableTx)
        internal fun remove(tableTx: OpenTx.Table) = txTables.remove(tableTx)
        internal val isEmpty get() = txTables.isEmpty()

        /**
         * Fresh, caller-owned [TableSnapshot]s of this table's staged writes, oldest→newest. Copied off
         * the borrowed OpenTx relations, so they outlive them and are closed by the enclosing [Snapshot].
         */
        fun openSnapshots(al: BufferAllocator): List<TableSnapshot> =
            mutableListOf<TableSnapshot>().closeAllOnCatch { snaps ->
                for (tableTx in txTables) TableSnapshot.openTx(al, tableTx)?.let(snaps::add)
                snaps
            }
    }

    private val accumulating = ArrayDeque<Staged>()

    // The batch handed to the replica writer, awaiting durable confirmation; empty when idle.
    private var committing: List<Staged> = emptyList()

    private val tables = HashMap<TableRef, Table>()

    /**
     * The staging index's own latest-completed-tx — the applied / resolution head. Drives the next
     * external-source tx-id and system-time smoothing. Seeded from the durable head
     * ([LiveIndex.latestCompletedTx]) when the index is created, then advanced as txs are applied; it
     * leads the durable head by the in-flight txs, and is equal to it under the serial commit.
     */
    var latestCompletedTx: TransactionKey? = latestCompletedTx
        private set

    /** True while a batch is in flight to the replica writer. */
    val inFlight get() = committing.isNotEmpty()

    /** True while there are resolved txs not yet sealed into a batch. */
    val hasAccumulated get() = accumulating.isNotEmpty()

    private fun indexTables(openTx: OpenTx) {
        for ((tableRef, tableTx) in openTx.tables)
            tables.getOrPut(tableRef) { Table() }.add(tableTx)
    }

    private fun deindexTables(openTx: OpenTx) {
        for ((tableRef, tableTx) in openTx.tables)
            tables[tableRef]?.let { if (it.apply { remove(tableTx) }.isEmpty) tables.remove(tableRef) }
    }

    fun apply(resolvedTx: ReplicaMessage.ResolvedTx, openTx: OpenTx, notifyMsgId: MessageId) {
        accumulating.addLast(Staged(resolvedTx, openTx, notifyMsgId))
        indexTables(openTx)
        latestCompletedTx = TransactionKey(resolvedTx.txId, resolvedTx.systemTime)
    }

    /** Seal the accumulated txs into the committing batch, to be written to the replica log. */
    fun seal(): List<ReplicaMessage.ResolvedTx> {
        check(committing.isEmpty()) { "a batch is already in flight" }
        committing = accumulating.toList()
        accumulating.clear()
        return committing.map { it.resolvedTx }
    }

    /**
     * Promote the next staged tx into the durable live index and close its OpenTx; returns it (so the
     * caller can advance the apply cursor and notify), or null once the committing batch is drained.
     *
     * Promoting one at a time — rather than all-then-return — is what keeps a partial failure safe. The
     * tx is imported before it's removed from [committing], so if [LiveIndex.importTx] throws, the
     * un-imported tail stays in [committing] (freed by [close]) and none is double-closed; and the
     * caller has advanced the cursor only over what actually imported, so a follower resuming from it
     * re-applies only the tail rather than double-importing the head.
     */
    fun promoteNext(liveIndex: LiveIndex): Staged? {
        val staged = committing.firstOrNull() ?: return null
        liveIndex.importTx(staged.resolvedTx)
        committing = committing.drop(1)
        deindexTables(staged.openTx)
        staged.openTx.close()
        return staged
    }

    /** Table refs with staged writes, for the resolution snapshot's per-table build. */
    val tableRefs: Set<TableRef> get() = tables.keys

    /** The staged writes to [tableRef], or null if none are in flight for it. */
    fun table(tableRef: TableRef): Table? = tables[tableRef]

    override fun close() {
        (accumulating.map { it.openTx } + committing.map { it.openTx }).closeAll()
        accumulating.clear()
        committing = emptyList()
        tables.clear()
    }
}
