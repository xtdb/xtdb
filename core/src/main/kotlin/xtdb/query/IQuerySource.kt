package xtdb.query

import xtdb.database.DatabaseName
import xtdb.database.DatabaseState
import xtdb.database.DatabaseStorage
import xtdb.indexer.Snapshot

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

    fun interface Factory {
        fun create(allocator: org.apache.arrow.memory.BufferAllocator, meterRegistry: io.micrometer.core.instrument.MeterRegistry?, scanEmitter: Any): IQuerySource
    }
}
