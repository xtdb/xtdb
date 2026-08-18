package xtdb.query

import io.micrometer.core.instrument.MeterRegistry
import org.apache.arrow.memory.BufferAllocator
import xtdb.api.query.PrepareOpts
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

        /**
         * True for a transaction's narrowed catalog, which resolves only the transaction's own database.
         *
         * The planner reads this solely to *explain* a table that didn't resolve: a qualified name that would
         * have named another database from a connection reports "not found" here, which is true of this
         * catalog and misleading about the node.
         *
         * It MUST NOT gate resolution. The set of attached databases is node-local — `XTDB_SKIP_DBS`, and an
         * `ATTACH` that some nodes have processed and others haven't — and a resolved-but-unappended tx is
         * re-resolved by the next leader, so a resolution decision taken on this would make the same
         * source-log message abort on one node and write rows on another.
         */
        val txScoped: Boolean
    }

    interface QueryDatabase : DatabaseSnapshot.Source {
        val name: DatabaseName
        val storage: PartitionStorage
        val queryState: PartitionState
    }

    fun prepareQuery(query: ParsedStatement, dbs: QueryCatalog, opts: PrepareOpts): PreparedQuery
    fun prepareRa(plan: Any, dbs: QueryCatalog, opts: PrepareOpts): PreparedQuery
    fun prepareTxSql(sql: String, dbs: QueryCatalog, opts: PrepareOpts): SqlStatement
    fun preparePatchDocsQuery(table: TableRef, validFrom: Instant?, validTo: Instant?, dbs: QueryCatalog, opts: PrepareOpts): PreparedQuery

    fun interface Factory {
        fun create(allocator: BufferAllocator, meterRegistry: MeterRegistry?, scanEmitter: Any): IQuerySource
    }
}
