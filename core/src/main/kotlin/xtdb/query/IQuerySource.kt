package xtdb.query

import io.micrometer.core.instrument.MeterRegistry
import org.apache.arrow.memory.BufferAllocator
import xtdb.api.error.Incorrect
import xtdb.api.query.PrepareOpts
import xtdb.database.DatabaseName
import xtdb.database.PartitionState
import xtdb.database.PartitionStorage
import xtdb.indexer.DatabaseSnapshot
import xtdb.util.closeAllOnCatch
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

        fun databaseOrThrow(dbName: DatabaseName): QueryDatabase =
            databaseOrNull(dbName)
                ?: throw Incorrect("Unknown database: $dbName", "xtdb/unknown-db", mapOf("db-name" to dbName))

        /**
         * Every attached database, resolved once.
         *
         * A database dropped by a concurrent `DETACH` is skipped rather than reported: [databaseNames] and
         * [databaseOrNull] are separate reads, so one can disappear between them, and an operation that never
         * mentioned it should not die of that. One that *did* name it finds no entry here, which is where the
         * anomaly belongs.
         */
        // TODO resolves every attached database, not just the ones a query names; under real
        //   multi-tenancy that wants narrowing, which needs name resolution to stop depending on
        //   the full table-info.
        fun resolveDbs(): Map<DatabaseName, QueryDatabase> =
            databaseNames.mapNotNull { dbName -> databaseOrNull(dbName)?.let { dbName to it } }.toMap()

        /**
         * A snapshot per database in [dbs], at least as fresh as [minBasis] where one is given for that
         * database. **The caller owns every snapshot in the returned map and must close them all.**
         */
        fun openSnapshots(
            dbs: Map<DatabaseName, QueryDatabase>,
            minBasis: Map<DatabaseName, List<Instant?>>?
        ): Map<DatabaseName, DatabaseSnapshot> =
            mutableMapOf<DatabaseName, DatabaseSnapshot>().closeAllOnCatch { snaps ->
                dbs.forEach { (dbName, db) -> snaps[dbName] = db.openSnapshot(minBasis?.get(dbName)) }
                snaps
            }
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
