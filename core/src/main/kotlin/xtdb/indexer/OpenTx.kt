package xtdb.indexer

import io.micrometer.tracing.Tracer
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.ArrowType.Struct.INSTANCE as STRUCT_TYPE
import xtdb.NodeBase
import xtdb.ResultCursor
import xtdb.api.TransactionKey
import xtdb.arrow.Relation
import xtdb.arrow.RelationReader
import xtdb.arrow.VectorWriter
import xtdb.database.DatabaseName
import xtdb.database.DatabaseState
import xtdb.database.DatabaseStorage
import xtdb.database.ExternalSourceToken
import xtdb.kw
import xtdb.query.IQuerySource
import xtdb.table.TableRef
import xtdb.time.InstantUtil.asMicros
import xtdb.trie.MemoryHashTrie
import xtdb.trie.Trie
import xtdb.util.closeAll
import java.nio.ByteBuffer
import kotlin.Long.Companion.MAX_VALUE as MAX_LONG
import kotlin.Long.Companion.MIN_VALUE as MIN_LONG

class OpenTx(
    private val allocator: BufferAllocator,
    private val nodeBase: NodeBase,
    private val dbStorage: DatabaseStorage,
    private val dbState: DatabaseState,
    val txKey: TransactionKey,
    val externalSourceToken: ExternalSourceToken?,
    private val tracer: Tracer? = null,
) : AutoCloseable {

    val systemFrom = txKey.systemTime.asMicros

    private val tableTxs = HashMap<TableRef, Table>()

    fun table(table: TableRef): Table =
        tableTxs.getOrPut(table) { Table(allocator, systemFrom) }

    fun table(schemaName: String, tableName: String) =
        table(TableRef(dbState.name, schemaName, tableName))

    val tables: Iterable<Map.Entry<TableRef, Table>> get() = tableTxs.entries

    fun serializeTableData(): Map<String, ByteArray> =
        tableTxs.mapNotNull { (tableRef, tableTx) ->
            tableTx.serializeTxData()?.let { tableRef.schemaAndTable to it }
        }.toMap()

    private fun queryCatalog(): IQuerySource.QueryCatalog {
        val liveIndex = dbState.liveIndex

        val snapSource = object : Snapshot.Source {
            override fun openSnapshot() = liveIndex.openSnapshot(this@OpenTx)
        }

        val queryDb = object : IQuerySource.QueryDatabase {
            override val storage get() = dbStorage
            override val queryState get() = dbState
            override fun openSnapshot() = snapSource.openSnapshot()
        }
        return object : IQuerySource.QueryCatalog {
            override val databaseNames: Collection<DatabaseName> get() = setOf(dbState.name)
            override fun databaseOrNull(dbName: DatabaseName) = queryDb.takeIf { dbName == dbState.name }
        }
    }

    /**
     * Run SQL against the database's live index, with visibility into writes made earlier
     * in this transaction.
     *
     * Positional `?` parameters are supplied through [args] as a single-row [xtdb.arrow.RelationReader].
     * Defaults for [opts] are derived from the tx's system-time and the database's default timezone.
     */
    fun openQuery(
        sql: String,
        args: RelationReader? = null,
        opts: TxIndexer.QueryOpts = TxIndexer.QueryOpts(),
    ): ResultCursor {
        val currentTime = opts.currentTime ?: txKey.systemTime

        val prepareOpts = mapOf(
            "current-time".kw to currentTime,
            "default-tz".kw to opts.defaultTz,
            "default-db".kw to dbState.name,
            "query-text".kw to sql,
        )

        return nodeBase.querySource
            .prepareQuery(sql, queryCatalog(), prepareOpts)
            .openQuery(args, xtdb.query.QueryOpts(currentTime, opts.defaultTz, tracer = tracer))
    }

    override fun close() = tableTxs.values.closeAll()

    class Table(
        allocator: BufferAllocator,
        private val systemFrom: Long,
    ) : AutoCloseable {

        val txRelation: Relation = Trie.openLogDataWriter(allocator)

        private val iidVec = txRelation["_iid"]
        private val systemFromVec = txRelation["_system_from"]
        private val validFromVec = txRelation["_valid_from"]
        private val validToVec = txRelation["_valid_to"]
        private val opVec = txRelation["op"]
        private val putVec by lazy(LazyThreadSafetyMode.NONE) { opVec.vectorFor("put", STRUCT_TYPE, false) }
        private val deleteVec = opVec["delete"]
        private val eraseVec = opVec["erase"]

        val docWriter: VectorWriter by lazy(LazyThreadSafetyMode.NONE) { putVec }

        var trie: MemoryHashTrie = MemoryHashTrie.emptyTrie(iidVec)
            private set

        fun logPut(iid: ByteBuffer, validFrom: Long, validTo: Long, writeDocFun: Runnable) {
            val pos = txRelation.rowCount

            iidVec.writeBytes(iid)
            systemFromVec.writeLong(systemFrom)
            validFromVec.writeLong(validFrom)
            validToVec.writeLong(validTo)

            writeDocFun.run()

            txRelation.endRow()

            trie += pos
        }

        fun logDelete(iid: ByteBuffer, validFrom: Long, validTo: Long) {
            val pos = txRelation.rowCount

            iidVec.writeBytes(iid)
            systemFromVec.writeLong(systemFrom)
            validFromVec.writeLong(validFrom)
            validToVec.writeLong(validTo)
            deleteVec.writeNull()
            txRelation.endRow()

            trie += pos
        }

        fun logErase(iid: ByteBuffer) {
            val pos = txRelation.rowCount

            iidVec.writeBytes(iid)
            systemFromVec.writeLong(systemFrom)
            validFromVec.writeLong(MIN_LONG)
            validToVec.writeLong(MAX_LONG)
            eraseVec.writeNull()
            txRelation.endRow()

            trie += pos
        }

        fun serializeTxData(): ByteArray? =
            if (txRelation.rowCount > 0) txRelation.asArrowStream else null

        override fun close() {
            txRelation.close()
        }
    }
}
