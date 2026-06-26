package xtdb.query

import io.micrometer.core.instrument.MeterRegistry
import org.apache.arrow.memory.BufferAllocator
import xtdb.arrow.RelationReader
import xtdb.database.DatabaseName
import xtdb.database.DatabaseState
import xtdb.database.DatabaseStorage
import xtdb.indexer.DatabaseSnapshot
import xtdb.table.TableRef
import xtdb.tx.TxOp
import java.time.Instant
import java.time.ZoneId

interface IQuerySource : AutoCloseable {

    interface QueryCatalog {
        val databaseNames: Collection<DatabaseName>
        fun databaseOrNull(dbName: DatabaseName): QueryDatabase?
    }

    interface QueryDatabase : DatabaseSnapshot.Source {
        val storage: DatabaseStorage
        val queryState: DatabaseState
    }

    fun prepareQuery(query: Any, dbs: QueryCatalog, opts: Any?): PreparedQuery
    fun prepareTxSql(sql: String, dbs: QueryCatalog, opts: Any?): SqlStatement

    /**
     * Statically expands an INSERT/PATCH into eager PutDocs/PatchDocs ops at submit time, opened against
     * [allocator]; null when the statement isn't statically expandable (UPDATE/DELETE/etc, or a planning error),
     * in which case the caller submits the raw SQL op instead.
     */
    fun sqlToStaticOps(sql: String, args: RelationReader?, allocator: BufferAllocator, defaultTz: ZoneId?): List<TxOp>?
    fun preparePatchDocsQuery(table: TableRef, validFrom: Instant?, validTo: Instant?, dbs: QueryCatalog, opts: Any?): PreparedQuery

    fun interface Factory {
        fun create(allocator: BufferAllocator, meterRegistry: MeterRegistry?, scanEmitter: Any): IQuerySource
    }
}
