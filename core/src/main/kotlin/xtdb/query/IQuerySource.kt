package xtdb.query

import io.micrometer.core.instrument.MeterRegistry
import org.apache.arrow.memory.BufferAllocator
import xtdb.database.DatabaseName
import xtdb.database.DatabaseState
import xtdb.database.DatabaseStorage
import xtdb.indexer.Snapshot
import xtdb.table.TableRef
import java.time.Instant

interface IQuerySource : AutoCloseable {

    interface QueryCatalog {
        val databaseNames: Collection<DatabaseName>
        fun databaseOrNull(dbName: DatabaseName): QueryDatabase?
    }

    interface QueryDatabase : Snapshot.Source {
        val storage: DatabaseStorage
        val queryState: DatabaseState
    }

    fun prepareQuery(query: Any, dbs: QueryCatalog, opts: Any?): PreparedQuery
    fun prepareDmlQuery(sql: String, dbs: QueryCatalog, opts: Any?): PreparedDmlQuery
    fun preparePatchDocsQuery(table: TableRef, validFrom: Instant?, validTo: Instant?, dbs: QueryCatalog, opts: Any?): PreparedQuery

    fun interface Factory {
        fun create(allocator: BufferAllocator, meterRegistry: MeterRegistry?, scanEmitter: Any): IQuerySource
    }
}
