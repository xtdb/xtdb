package xtdb.postgres

import java.time.Instant

sealed interface RowOp {
    val schema: String
    val table: String
    data class Put(override val schema: String, override val table: String, val row: Map<String, Any?>) : RowOp
    data class Delete(override val schema: String, override val table: String, val row: Map<String, Any?>) : RowOp
}

/**
 * Abstracts PostgreSQL I/O — connection management, replication slot lifecycle,
 * snapshot reads, and change streaming — behind a testable interface.
 *
 * The real implementation ([PgWireDriver]) talks to PostgreSQL via pgjdbc.
 * Tests can substitute a fake that yields predetermined data.
 */
interface PostgresDriver : AutoCloseable {

    /**
     * Creates a replication slot, exports a snapshot, and reads all published tables.
     *
     * Each call to [SnapshotReader.batches] yields batches of put operations — one batch per chunk of rows
     * from each table. Closing the reader releases the replication and JDBC connections.
     */
    interface SnapshotReader : AutoCloseable {
        val slotLsn: Long
        fun batches(): Sequence<List<RowOp.Put>>
    }

    fun openSnapshot(): SnapshotReader

    /**
     * A committed PostgreSQL transaction, containing the row-level operations
     * and metadata needed for indexing and acknowledgement.
     */
    data class Transaction(val lsn: Long, val commitTime: Instant, val ops: List<RowOp>)

    /**
     * Opens a logical replication stream from the given LSN.
     *
     * Reading ([poll]) and acknowledging ([acknowledge]) are deliberately separate so the caller can advance the
     * slot's confirmed-flush LSN behind durable import rather than in lockstep with reads. The underlying pgjdbc
     * stream is not thread-safe, so a single caller must drive both.
     */
    interface ChangeStream : AutoCloseable {
        /**
         * Reads the next committed transaction with DML operations, or null on an idle tick (nothing currently
         * available). Relation/begin/empty-keepalive bookkeeping is handled internally. Does not acknowledge —
         * the caller acks via [acknowledge] once the transaction is durable.
         */
        suspend fun poll(): Transaction?

        /**
         * The latest server WAL position received on the stream (advances on commits and protocol keepalives), used
         * to let Postgres recycle unrelated WAL while our slot is idle. Safe to [acknowledge] ONLY when nothing read
         * via [poll] is still awaiting durability: this position is the physical receive LSN, always below the commit
         * LSN of any transaction whose Commit hasn't arrived yet, and — since the caller submits a transaction the
         * moment [poll] returns it — "nothing awaiting durability" means no already-committed change sits unimported
         * at or below it. (Postgres also holds `restart_lsn` at the start of any in-progress transaction regardless
         * of the confirmed-flush LSN, so an un-committed transaction is always re-sent in full.)
         */
        val walEnd: Long

        /**
         * Advances the slot's confirmed-flush LSN to [lsn] and sends a standby status update. [lsn] MUST NOT exceed
         * the highest LSN the caller has durably imported — PG recycles WAL up to it.
         */
        suspend fun acknowledge(lsn: Long)
    }

    suspend fun openStream(startLsn: Long): ChangeStream

    /**
     * `pg_current_wal_lsn() − confirmed_flush_lsn` for our slot on the source DB.
     * Returns null if the slot is not (yet) visible.
     */
    fun queryWalLagBytes(): Long?

    fun publicationExists(): Boolean
}
