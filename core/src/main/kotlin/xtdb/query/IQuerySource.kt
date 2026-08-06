package xtdb.query

import io.micrometer.core.instrument.MeterRegistry
import org.apache.arrow.memory.BufferAllocator
import xtdb.api.query.PrepareOpts
import xtdb.catalog.DatabaseTableCatalog
import xtdb.database.DatabaseName
import xtdb.database.PartitionState
import xtdb.database.PartitionStorage
import xtdb.indexer.DatabaseSnapshot
import xtdb.api.TableRef
import java.time.Instant

interface IQuerySource : AutoCloseable {

    interface QueryCatalog {
        val databaseNames: Collection<DatabaseName>
        fun databaseOrNull(dbName: DatabaseName): QueryDatabase?
    }

    /** One partition's read-side state — what a single scan branch reads from. */
    interface QueryPartition {
        val storage: PartitionStorage
        val state: PartitionState
    }

    interface QueryDatabase : DatabaseSnapshot.Source {
        val name: DatabaseName

        /**
         * In partition-index order. Slot `i` here, slot `i` of the [DatabaseSnapshot] this database
         * opens, and slot `i` of its basis vector are the same partition — the read path zips the
         * three positionally.
         */
        val partitions: List<QueryPartition>

        /** Historical table metadata across every partition — the planner's database-level view. */
        val tableCatalog: DatabaseTableCatalog
    }

    fun prepareQuery(query: ParsedStatement, dbs: QueryCatalog, opts: PrepareOpts): PreparedQuery
    fun prepareRa(plan: Any, dbs: QueryCatalog, opts: PrepareOpts): PreparedQuery
    fun prepareTxSql(sql: String, dbs: QueryCatalog, opts: PrepareOpts): SqlStatement
    fun preparePatchDocsQuery(table: TableRef, validFrom: Instant?, validTo: Instant?, dbs: QueryCatalog, opts: PrepareOpts): PreparedQuery

    fun interface Factory {
        fun create(allocator: BufferAllocator, meterRegistry: MeterRegistry?, scanEmitter: Any): IQuerySource
    }
}
