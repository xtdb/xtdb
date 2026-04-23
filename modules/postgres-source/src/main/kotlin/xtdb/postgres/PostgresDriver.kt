package xtdb.postgres

import java.time.Instant

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
     * [ChangeStream.nextTransaction] blocks until a committed transaction with DML operations is available,
     * then passes it to [block]. The LSN is acknowledged back to PostgreSQL only if [block] returns normally.
     * If [block] throws, the LSN is not acknowledged — PG will re-send the changes on the next connection.
     */
    interface ChangeStream : AutoCloseable {
        suspend fun nextTransaction(block: suspend (Transaction) -> Unit)
    }

    suspend fun openStream(startLsn: Long): ChangeStream
}
