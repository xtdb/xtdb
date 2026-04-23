package xtdb.postgres

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
}
