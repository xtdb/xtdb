package xtdb.indexer

import io.micrometer.tracing.Tracer
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.ArrowType.Struct.INSTANCE as STRUCT_TYPE
import xtdb.NodeBase
import xtdb.ResultCursor
import xtdb.api.TransactionKey
import xtdb.arrow.Relation
import xtdb.arrow.RelationReader
import xtdb.arrow.VectorReader
import xtdb.arrow.VectorWriter
import xtdb.database.DatabaseName
import xtdb.database.DatabaseState
import xtdb.database.DatabaseStorage
import clojure.lang.PersistentArrayMap
import xtdb.arrow.RelationAsStructReader
import xtdb.arrow.SingletonListReader
import xtdb.database.ExternalSourceToken
import xtdb.error.Conflict
import xtdb.error.Incorrect
import xtdb.kw
import xtdb.query.IQuerySource
import xtdb.query.PreparedDmlQuery
import xtdb.table.SchemaName
import xtdb.table.TableName
import xtdb.table.TableRef
import xtdb.time.InstantUtil.asMicros
import xtdb.time.InstantUtil.fromMicros
import xtdb.time.microsAsInstant
import xtdb.trie.MemoryHashTrie
import xtdb.trie.Trie
import xtdb.util.asIid
import xtdb.util.closeAll
import java.nio.ByteBuffer
import java.time.Instant
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
        tableTxs.getOrPut(table) { Table(table, allocator, systemFrom) }

    fun table(schemaName: SchemaName, tableName: TableName) =
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

    /**
     * Execute a SQL DML statement (INSERT / UPDATE / PATCH / DELETE / ERASE / ASSERT)
     * against this tx, with visibility into writes made earlier in the same writer call.
     * Positional `?` parameters come from [args], like [openQuery].
     *
     * Writes to forbidden schemas (`xt`, `information_schema`, `pg_catalog`) throw.
     */
    fun executeDml(
        sql: String,
        args: RelationReader? = null,
        opts: TxIndexer.QueryOpts = TxIndexer.QueryOpts(),
    ) {
        val currentTime = opts.currentTime ?: txKey.systemTime
        val currentTimeµs = currentTime.asMicros
        val qOpts = xtdb.query.QueryOpts(currentTime, opts.defaultTz, tracer = tracer)

        val prepareOpts = PersistentArrayMap.create(mapOf(
            "current-time".kw to currentTime,
            "default-tz".kw to opts.defaultTz,
            "default-db".kw to dbState.name,
            "query-text".kw to sql,
            "arg-fields".kw to args?.schema?.fields,
        ))

        val prepared = nodeBase.querySource.prepareDmlQuery(sql, queryCatalog(), prepareOpts)

        checkArgCount(args, prepared.paramCount)

        when (val kind = prepared.kind) {
            is PreparedDmlQuery.Assert -> {
                val msg = kind.message ?: "Assert failed"
                fun fail(): Nothing = throw Conflict(msg, "xtdb/assert-failed", emptyMap<String, Any?>())
                prepared.openQuery(args, qOpts).use { cursor ->
                    if (!cursor.tryAdvance { rel -> if (rel.rowCount == 0) fail() }) fail()
                }
            }

            is PreparedDmlQuery.Put -> {
                checkNotForbidden(kind.table)
                val table = table(kind.table)
                prepared.openQuery(args, qOpts).use { cursor ->
                    cursor.forEachRemaining { rel ->
                        // INSERT/UPDATE plans emit `_id` + flat content cols + optional temporal,
                        // not the canonical `_iid` + `doc` shape `logPuts` expects.
                        // Package content cols into a `doc` struct here.
                        val tempColNames = setOf("_iid", "_system_from", "_system_to", "_valid_from", "_valid_to")
                        val contentRel = RelationReader.from(
                            rel.vectors.filter { it.name !in tempColNames },
                            rel.rowCount,
                        )
                        val putCols = listOfNotNull(
                            rel.vectorFor("_id"),
                            RelationAsStructReader("doc", contentRel),
                            rel.vectorForOrNull("_valid_from"),
                            rel.vectorForOrNull("_valid_to"),
                        )
                        table.logPuts(currentTimeµs, Long.MAX_VALUE, RelationReader.from(putCols, rel.rowCount))
                    }
                }
            }

            is PreparedDmlQuery.Patch -> {
                checkNotForbidden(kind.table)
                val table = table(kind.table)
                prepared.openQuery(args, qOpts).use { cursor ->
                    cursor.forEachRemaining { rel -> table.logPuts(0, Long.MAX_VALUE, rel) }
                }
            }

            is PreparedDmlQuery.Delete -> {
                checkNotForbidden(kind.table)
                val table = table(kind.table)
                prepared.openQuery(args, qOpts).use { cursor ->
                    cursor.forEachRemaining { rel -> table.logDeletes(0, Long.MAX_VALUE, rel) }
                }
            }

            is PreparedDmlQuery.Erase -> {
                checkNotForbidden(kind.table)
                val table = table(kind.table)
                prepared.openQuery(args, qOpts).use { cursor ->
                    cursor.forEachRemaining { rel -> table.logErases(rel.vectorFor("_iid")) }
                }
            }
        }
    }

    /**
     * Bulk-patch docs into [table], keyed by `_iid`, preserving existing columns where
     * the patched doc doesn't supply a new value.
     *
     * [docs] carries one row per doc, columns `_iid` (fixed-size binary) and `doc` (struct).
     * [validFromµs] / [validToµs] default to [Long.MIN_VALUE] / [Long.MAX_VALUE] — the start/end of
     * valid time — on the Instant overload, a null on either side means the same thing.
     */
    fun patchDocs(
        table: Table,
        validFromµs: Long,
        validToµs: Long,
        docs: RelationReader,
    ) {
        checkNotForbidden(table.ref)

        val prepareOpts = PersistentArrayMap.create(mapOf(
            "current-time".kw to txKey.systemTime,
            "default-db".kw to dbState.name,
            "arg-fields".kw to docs.schema.fields,
        ))

        // The planner (plan-patch in sql.clj) requires concrete Instant bounds — no nil support.
        // The MIN/MAX µs bounds become start-of-time / end-of-time Instants; the planner treats
        // them as literals and doesn't round-trip them through `asMicros`, so the lossiness of
        // those sentinel Instants doesn't bite here.
        val pq = nodeBase.querySource.preparePatchDocsQuery(
            table.ref, validFromµs.microsAsInstant, validToµs.microsAsInstant,
            queryCatalog(), prepareOpts,
        )

        val patchArgs = RelationReader.from(listOf(
            SingletonListReader("?patch_docs", RelationAsStructReader("patch_doc", docs))
        ))

        val qOpts = xtdb.query.QueryOpts(txKey.systemTime, tracer = tracer)

        // openQuery takes ownership of the sliced args and closes them when the cursor closes —
        // no separate `.use` on the slice here or it'd double-free.
        pq.openQuery(patchArgs.openSlice(allocator), qOpts).use { cursor ->
            cursor.forEachRemaining { rel -> table.logPuts(validFromµs, validToµs, rel) }
        }
    }

    /**
     * Instant-based overload of [patchDocs] for end-user callers working with `java.time.Instant`
     * (external-source writers typically do). Null on either bound means the same as passing
     * [Long.MIN_VALUE] / [Long.MAX_VALUE] to the µs overload.
     */
    @JvmOverloads
    fun patchDocs(
        table: Table,
        validFrom: Instant? = null,
        validTo: Instant? = null,
        docs: RelationReader,
    ) = patchDocs(
        table,
        validFrom?.asMicros ?: Long.MIN_VALUE,
        validTo?.asMicros ?: Long.MAX_VALUE,
        docs,
    )

    override fun close() = tableTxs.values.closeAll()

    companion object {
        // Must stay in sync with xtdb.log/forbidden-schemas and xtdb.tx.TxWriter's FORBIDDEN_SCHEMAS.
        private val FORBIDDEN_SCHEMAS = setOf("xt", "information_schema", "pg_catalog")

        private fun checkNotForbidden(tableRef: TableRef) {
            if (tableRef.schemaName in FORBIDDEN_SCHEMAS) {
                throw Incorrect(
                    "Cannot write to table: ${tableRef.schemaAndTable}",
                    "xtdb/forbidden-table",
                    mapOf("table" to tableRef),
                )
            }
        }

        private fun checkArgCount(args: RelationReader?, paramCount: Int) {
            if (args == null) {
                if (paramCount != 0) {
                    throw Incorrect(
                        "Arguments list was expected but not provided",
                        "xtdb.indexer/missing-sql-args",
                        mapOf("param-count" to paramCount),
                    )
                }
            } else {
                val argCount = args.vectors.size
                if (argCount != paramCount) {
                    throw Incorrect(
                        "Parameter error: $argCount provided, $paramCount expected",
                        "xtdb.indexer/incorrect-sql-arg-count",
                        mapOf("param-count" to paramCount, "arg-count" to argCount),
                    )
                }
            }
        }
    }

    class Table(
        val ref: TableRef,
        allocator: BufferAllocator,
        private val systemFrom: Long,
    ) : AutoCloseable {

        private fun checkValidTimes(validFrom: Long, validTo: Long) {
            if (validFrom >= validTo) {
                throw Incorrect(
                    "Invalid valid times",
                    "xtdb.indexer/invalid-valid-times",
                    mapOf("valid-from" to fromMicros(validFrom), "valid-to" to fromMicros(validTo)),
                )
            }
        }

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

        /**
         * Batch put: writes every row in [rel], content from `doc`.
         *
         * iid source: `_iid` column (raw bytes, copied directly) if present; otherwise `_id` column
         * (user-facing id — iid computed per row via [xtdb.util.asIid]).
         *
         * Per-row valid times come from the rel's `_valid_from` / `_valid_to` columns when present and non-null;
         * otherwise the [validFrom] / [validTo] defaults apply.
         * Rejects zero-width or inverted ranges — one check over the defaults if the rel has no per-row
         * temporal columns; otherwise per-row.
         */
        fun logPuts(validFrom: Long, validTo: Long, rel: RelationReader) {
            val rowCount = rel.rowCount
            if (rowCount == 0) return

            val iidRdr = rel.vectorForOrNull("_iid")
            val idRdr = if (iidRdr == null) rel.vectorFor("_id") else null
            val docRdr = rel.vectorFor("doc")
            val vfRdr = rel.vectorForOrNull("_valid_from")
            val vtRdr = rel.vectorForOrNull("_valid_to")

            val perRowCheck = vfRdr != null || vtRdr != null
            if (!perRowCheck) checkValidTimes(validFrom, validTo)

            val iidCopier = iidRdr?.rowCopier(iidVec)
            val docCopier = docRdr.rowCopier(docWriter)

            val startPos = txRelation.rowCount
            repeat(rowCount) { idx ->
                val rowValidFrom = if (vfRdr != null && !vfRdr.isNull(idx)) vfRdr.getLong(idx) else validFrom
                val rowValidTo = if (vtRdr != null && !vtRdr.isNull(idx)) vtRdr.getLong(idx) else validTo
                if (perRowCheck) checkValidTimes(rowValidFrom, rowValidTo)

                if (iidCopier != null) iidCopier.copyRow(idx)
                else iidVec.writeBytes(ByteBuffer.wrap(idRdr!!.getObject(idx).asIid))
                systemFromVec.writeLong(systemFrom)
                validFromVec.writeLong(rowValidFrom)
                validToVec.writeLong(rowValidTo)
                docCopier.copyRow(idx)
                txRelation.endRow()
            }
            trie = trie.addRange(startPos, rowCount)
        }

        /**
         * Batch delete: deletes every row in [rel] (keyed by `_iid`).
         *
         * Per-row valid times come from the rel's `_valid_from` / `_valid_to` columns when present and non-null;
         * otherwise the [validFrom] / [validTo] defaults apply.
         * Rejects zero-width or inverted ranges — one check over the defaults if the rel has no per-row
         * temporal columns; otherwise per-row.
         */
        fun logDeletes(validFrom: Long, validTo: Long, rel: RelationReader) {
            val rowCount = rel.rowCount
            if (rowCount == 0) return

            val iidRdr = rel.vectorFor("_iid")
            val vfRdr = rel.vectorForOrNull("_valid_from")
            val vtRdr = rel.vectorForOrNull("_valid_to")

            val perRowCheck = vfRdr != null || vtRdr != null
            if (!perRowCheck) checkValidTimes(validFrom, validTo)

            val iidCopier = iidRdr.rowCopier(iidVec)

            val startPos = txRelation.rowCount
            repeat(rowCount) { idx ->
                val rowValidFrom = if (vfRdr != null && !vfRdr.isNull(idx)) vfRdr.getLong(idx) else validFrom
                val rowValidTo = if (vtRdr != null && !vtRdr.isNull(idx)) vtRdr.getLong(idx) else validTo
                if (perRowCheck) checkValidTimes(rowValidFrom, rowValidTo)

                iidCopier.copyRow(idx)
                systemFromVec.writeLong(systemFrom)
                validFromVec.writeLong(rowValidFrom)
                validToVec.writeLong(rowValidTo)
                deleteVec.writeNull()
                txRelation.endRow()
            }
            trie = trie.addRange(startPos, rowCount)
        }

        /**
         * Batch erase: erases every iid in [iids] across all valid time.
         */
        fun logErases(iids: VectorReader) {
            val rowCount = iids.valueCount
            if (rowCount == 0) return

            val iidCopier = iids.rowCopier(iidVec)

            val startPos = txRelation.rowCount
            repeat(rowCount) { idx ->
                iidCopier.copyRow(idx)
                systemFromVec.writeLong(systemFrom)
                validFromVec.writeLong(MIN_LONG)
                validToVec.writeLong(MAX_LONG)
                eraseVec.writeNull()
                txRelation.endRow()
            }
            trie = trie.addRange(startPos, rowCount)
        }

        fun serializeTxData(): ByteArray? =
            if (txRelation.rowCount > 0) txRelation.asArrowStream else null

        override fun close() {
            txRelation.close()
        }
    }
}
