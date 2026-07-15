package xtdb.query

import io.micrometer.core.instrument.MeterRegistry
import org.apache.arrow.memory.BufferAllocator
import xtdb.api.query.PrepareOpts
import xtdb.database.DatabaseName
import xtdb.database.DatabaseState
import xtdb.database.DatabaseStorage
import xtdb.indexer.DatabaseSnapshot
import xtdb.table.TableRef
import java.time.Instant

interface IQuerySource : AutoCloseable {

    interface QueryCatalog {
        val databaseNames: Collection<DatabaseName>
        fun databaseOrNull(dbName: DatabaseName): QueryDatabase?
    }

    interface QueryDatabase : DatabaseSnapshot.Source {
        val storage: DatabaseStorage
        val queryState: DatabaseState
    }

    fun prepareQuery(query: ParsedStatement, dbs: QueryCatalog, opts: PrepareOpts): PreparedQuery
    fun prepareRa(plan: Any, dbs: QueryCatalog, opts: PrepareOpts): PreparedQuery
    fun prepareTxSql(sql: String, dbs: QueryCatalog, opts: PrepareOpts): SqlStatement
    fun preparePatchDocsQuery(table: TableRef, validFrom: Instant?, validTo: Instant?, dbs: QueryCatalog, opts: PrepareOpts): PreparedQuery

    fun interface Factory {
        fun create(allocator: BufferAllocator, meterRegistry: MeterRegistry?, scanEmitter: Any): IQuerySource
    }
}
