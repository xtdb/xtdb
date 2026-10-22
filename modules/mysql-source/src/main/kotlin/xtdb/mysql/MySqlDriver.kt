package xtdb.mysql

import java.time.Instant

/**
 * [primaryKey] names the upstream table's primary-key columns, in key order — empty for a table with none.
 *
 * @suppress
 */
sealed interface RowOp {
    val database: String
    val table: String
    val primaryKey: List<String>

    data class Put(override val database: String, override val table: String, override val primaryKey: List<String>, val row: Map<String, Any?>) : RowOp

    /** [before] differs from [row] in its key where the UPDATE changed the key. */
    data class Update(
        override val database: String, override val table: String, override val primaryKey: List<String>,
        val before: Map<String, Any?>, val row: Map<String, Any?>,
    ) : RowOp

    /** [row] is the deleted row's full before-image. */
    data class Delete(override val database: String, override val table: String, override val primaryKey: List<String>, val row: Map<String, Any?>) : RowOp
}

/**
 * Abstracts MySQL I/O — the upstream checks, the consistent snapshot and the binlog stream — behind a
 * testable interface. The real implementation is [MySqlWireDriver].
 *
 * @suppress
 */
interface MySqlDriver : AutoCloseable {

    /**
     * Fails `Incorrect`, naming what to change, unless the upstream writes a binlog this source can read
     * and every included table exists.
     */
    fun checkUpstream()

    /**
     * A consistent read of every included table, as of [position].
     * Closing it releases the connection holding the snapshot open.
     */
    interface SnapshotReader : AutoCloseable {
        val position: GtidSet
        fun batches(): Sequence<List<RowOp.Put>>
    }

    fun openSnapshot(): SnapshotReader

    /** Row changes that land as one XT transaction, and the [position] that transaction's token carries. */
    sealed interface Transaction {
        val ops: List<RowOp>
        val position: GtidSet

        /** A batch of the initial snapshot, which belongs to no upstream transaction. */
        data class Snapshot(override val ops: List<RowOp.Put>, override val position: GtidSet) : Transaction

        /**
         * One committed upstream transaction, in binlog order.
         * [ops] holds only the included tables' rows, so is empty for DDL or a transaction on other tables.
         */
        data class Committed(val gtid: Gtid, val commitTime: Instant, override val ops: List<RowOp>, override val position: GtidSet) : Transaction
    }

    interface ChangeStream : AutoCloseable {
        /** The next transaction in commit order, or null on an idle tick. */
        suspend fun poll(): Transaction.Committed?

        /** Whether the binlog connection is up right now. Safe to read from any thread. */
        val connected: Boolean
    }

    /**
     * Opens the binlog just after [from], presenting each transaction not in [from] once, in commit order.
     * [from] is the whole of where it starts: the stream tracks no position of its own beyond it.
     *
     * Fails `Incorrect` rather than start past a gap — the upstream must have executed everything in [from],
     * and must not have purged anything [from] lacks.
     */
    suspend fun openStream(from: GtidSet): ChangeStream

    /** Every transaction the upstream has executed, `@@global.gtid_executed`. */
    fun queryExecutedGtids(): GtidSet
}
