package xtdb.indexer

import io.micrometer.tracing.Tracer
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.ArrowType.Struct.INSTANCE as STRUCT_TYPE
import xtdb.NodeBase
import xtdb.ResultCursor
import xtdb.api.TransactionKey
import xtdb.api.log.ReplicaMessage
import xtdb.arrow.NULL_TYPE
import xtdb.arrow.Relation
import xtdb.arrow.RelationReader
import xtdb.arrow.VectorReader
import xtdb.arrow.VectorType
import xtdb.arrow.VectorWriter
import xtdb.authz.RoleMembership
import xtdb.database.DatabaseName
import xtdb.database.DatabaseState
import xtdb.database.DatabaseStorage
import clojure.lang.PersistentArrayMap
import xtdb.arrow.RelationAsStructReader
import xtdb.arrow.SingletonListReader
import xtdb.arrow.VectorType.Companion.BOOL
import xtdb.arrow.VectorType.Companion.I64
import xtdb.arrow.VectorType.Companion.INSTANT
import xtdb.arrow.VectorType.Companion.TRANSIT
import xtdb.database.ExternalSourceToken
import xtdb.error.Conflict
import xtdb.error.Incorrect
import xtdb.kw
import xtdb.query.IQuerySource
import xtdb.query.PrepareOpts
import xtdb.query.SqlStatement
import xtdb.table.SchemaName
import xtdb.table.TableName
import xtdb.table.TableRef
import xtdb.time.InstantUtil.asMicros
import xtdb.time.InstantUtil.fromMicros
import xtdb.time.microsAsInstant
import xtdb.trie.MemoryHashTrie
import xtdb.trie.Trie
import xtdb.types.ClojureForm
import xtdb.util.asIid
import xtdb.util.closeAll
import xtdb.util.logger
import xtdb.util.warn
import java.nio.ByteBuffer
import java.time.Instant
import java.time.ZoneId
import kotlin.Long.Companion.MAX_VALUE as MAX_LONG
import kotlin.Long.Companion.MIN_VALUE as MIN_LONG

private val LOG = OpenTx::class.logger

/**
 * The per-transaction write/read handle passed to an external-source [writer][TxIndexer.indexTx].
 *
 * Stage writes per table via [table]; [openQuery] / [executeSql] run SQL against the live index with
 * read-your-writes visibility into those staged writes. [TxIndexer] owns opening, committing and closing the
 * OpenTx — a writer is handed one, it never constructs its own.
 */
class OpenTx(
    private val allocator: BufferAllocator,
    private val nodeBase: NodeBase,
    private val dbStorage: DatabaseStorage,
    private val dbState: DatabaseState,
    val txKey: TransactionKey,
    val externalSourceToken: ExternalSourceToken?,
    private val tracer: Tracer? = null,
) : AutoCloseable {

    data class QueryOpts(
        val currentTime: Instant? = null,
        val defaultTz: ZoneId? = null,
    )

    val systemTime get() = txKey.systemTime
    val systemTimeMicros = systemTime.asMicros

    private val tableTxs = HashMap<TableRef, Table>()

    /**
     * The staging area for writes to [table] in this tx, created on first access.
     *
     * See [Table] for the write protocol.
     */
    // TODO add table(schemaAndTable), make table(TableRef) internal
    fun table(table: TableRef): Table = tableTxs.getOrPut(table) { Table(table) }

    fun table(schemaName: SchemaName, tableName: TableName) = table(TableRef(dbState.name, schemaName, tableName))

    internal val tables: Iterable<Map.Entry<TableRef, Table>> get() = tableTxs.entries

    private fun writeTxRow(error: Throwable?, userMetadata: Map<*, *>?) {
        val txId = txKey.txId
        val systemTimeMicros = txKey.systemTime.asMicros

        val liveTable = table(TableRef(dbState.name, "xt", "txs"))
        val docWriter = liveTable.putDocWriter

        liveTable.writeId(txId)
        liveTable.writeDefaultValidTime()

        docWriter.vectorFor("_id", I64.arrowType, false).writeLong(txId)
        docWriter.vectorFor("system_time", INSTANT.arrowType, false).writeLong(systemTimeMicros)
        docWriter.vectorFor("committed", BOOL.arrowType, false).writeBoolean(error == null)
        docWriter.vectorFor("user_metadata", VectorType.structOf().arrowType, true).writeObject(userMetadata)

        val errorWriter = docWriter.vectorFor("error", TRANSIT.arrowType, true)

        if (error == null) {
            errorWriter.writeNull()
        } else {
            try {
                errorWriter.writeObject(error)
            } catch (e: Exception) {
                error.addSuppressed(e)
                LOG.warn(error, "Error serializing error, tx $txId")
                errorWriter.writeObject(ClojureForm("error serializing error - see server logs"))
            }
        }

        docWriter.endStruct()

        liveTable.endPut()
    }

    private fun serializeTableData(): Map<String, ByteArray> =
        tableTxs.entries.associate { (tableRef, tableTx) -> tableRef.schemaAndTable to tableTx.serializeTxData() }

    // TODO make internal
    @JvmOverloads
    fun commitTx(error: Throwable? = null, userMetadata: Map<*, *>? = null): ReplicaMessage.ResolvedTx {
        writeTxRow(error, userMetadata)

        return ReplicaMessage.ResolvedTx(
            txKey.txId, txKey.systemTime,
            committed = error == null,
            error = error,
            tableData = serializeTableData(),
            dbOp = null,
            externalSourceToken = externalSourceToken,
        )
    }

    private val queryCatalog: IQuerySource.QueryCatalog
        get() {
            val liveIndex = dbState.liveIndex

            val queryDb = object : IQuerySource.QueryDatabase {
                override val storage get() = dbStorage
                override val queryState get() = dbState
                override fun openSnapshot() =
                    DatabaseSnapshot(listOf(liveIndex.openSnapshot(this@OpenTx)))
            }

            return object : IQuerySource.QueryCatalog {
                override val databaseNames: Collection<DatabaseName> get() = setOf(dbState.name)
                override fun databaseOrNull(dbName: DatabaseName) = queryDb.takeIf { dbName == dbState.name }
            }
        }

    /**
     * Run SQL against the database's live index, with visibility into writes made earlier in this transaction.
     *
     * Positional `?` parameters are supplied through [args] as a single-row [xtdb.arrow.RelationReader].
     * Defaults for [opts] are derived from the tx's system-time and the database's default timezone.
     */
    fun openQuery(sql: String, args: RelationReader? = null, opts: QueryOpts = QueryOpts()): ResultCursor {
        val currentTime = opts.currentTime ?: txKey.systemTime

        val prepareOpts = PrepareOpts(
            defaultTz = opts.defaultTz, defaultDb = dbState.name, queryText = sql, currentTime = currentTime,
        )

        return nodeBase.querySource
            .prepareQuery(sql, queryCatalog, prepareOpts)
            .openQuery(args, xtdb.query.QueryOpts(currentTime, opts.defaultTz, tracer = tracer))
    }

    /**
     * Execute a DML/DDL SQL statement against this tx, with visibility into writes made earlier in the same
     * writer call. Positional `?` parameters come from [args], like [openQuery].
     *
     * DML writes to forbidden schemas (`xt`, `information_schema`, `pg_catalog`) will throw.
     */
    fun executeSql(sql: String, args: RelationReader? = null, opts: QueryOpts = QueryOpts(), user: String? = null) {
        val currentTime = opts.currentTime ?: txKey.systemTime
        val currentTimeMicros = currentTime.asMicros
        val qOpts = xtdb.query.QueryOpts(currentTime, opts.defaultTz, tracer = tracer)

        val prepareOpts = PersistentArrayMap.create(
            mapOf(
                "current-time".kw to currentTime,
                "default-tz".kw to opts.defaultTz,
                "default-db".kw to dbState.name,
                "query-text".kw to sql,
                "arg-fields".kw to args?.schema?.fields,
            )
        )

        when (val stmt = nodeBase.querySource.prepareTxSql(sql, queryCatalog, prepareOpts)) {
            is SqlStatement.GrantRole -> writeRoleMembership(stmt.user, stmt.role, currentTime, user, revoke = false)
            is SqlStatement.RevokeRole -> writeRoleMembership(stmt.user, stmt.role, currentTime, user, revoke = true)

            is SqlStatement.CreateTable -> createTable(stmt.table, stmt.colNames)

            is SqlStatement.DmlQuery -> {
                val query = stmt.query
                checkArgCount(args, query.paramCount)

                when (stmt) {
                    is SqlStatement.Assert -> {
                        val msg = stmt.message ?: "Assert failed"

                        fun fail(): Nothing =
                            throw Conflict(msg, "xtdb/assert-failed", emptyMap<String, Any?>())

                        query.openQuery(args, qOpts).use { cursor ->
                            if (!cursor.tryAdvance { rel -> if (rel.rowCount == 0) fail() }) fail()
                        }
                    }

                    is SqlStatement.Put -> {
                        checkNotForbidden(stmt.table)

                        query.openQuery(args, qOpts).use { cursor ->
                            cursor.forEachRemaining { rel ->
                                // defer table() until a row is actually written: a 0-row result (no-match
                                // UPDATE/DELETE, INSERT ... WHERE false) must not register a table — only CREATE TABLE does
                                if (rel.rowCount == 0) return@forEachRemaining

                                // INSERT/UPDATE plans emit `_id` + flat content cols + optional temporal,
                                // not the canonical `_iid` + `doc` shape `logPuts` expects.
                                // Package content cols into a `doc` struct here.
                                val tempColNames =
                                    setOf("_iid", "_system_from", "_system_to", "_valid_from", "_valid_to")

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

                                table(stmt.table)
                                    .writePuts(RelationReader.from(putCols, rel.rowCount), currentTimeMicros, MAX_LONG)
                            }
                        }
                    }

                    is SqlStatement.Patch -> {
                        checkNotForbidden(stmt.table)

                        query.openQuery(args, qOpts).use { cursor ->
                            cursor.forEachRemaining { rel ->
                                if (rel.rowCount > 0)
                                    table(stmt.table).writePuts(rel, currentTimeMicros, MAX_LONG)
                            }
                        }
                    }

                    is SqlStatement.Delete -> {
                        checkNotForbidden(stmt.table)

                        query.openQuery(args, qOpts).use { cursor ->
                            cursor.forEachRemaining { rel ->
                                if (rel.rowCount > 0)
                                    table(stmt.table).writeDeletes(rel, currentTimeMicros, MAX_LONG)
                            }
                        }
                    }

                    is SqlStatement.Erase -> {
                        checkNotForbidden(stmt.table)

                        query.openQuery(args, qOpts).use { cursor ->
                            cursor.forEachRemaining { rel ->
                                if (rel.rowCount > 0)
                                    table(stmt.table).writeErases(rel.vectorFor("_iid"))
                            }
                        }
                    }
                }
            }
        }
    }

    private fun createTable(ref: TableRef, colNames: List<String>) {
        checkNotForbidden(ref)
        table(ref).declareColumns(colNames)
    }

    // GRANT/REVOKE arrive as ordinary SQL but are authority-gated and write below the FORBIDDEN_SCHEMAS
    // guard (the `xt.txs` writeTxRow idiom), so the guard stays in force for user DML. The (user, role) is
    // parsed statically — there's no query to run — and keyed by a deterministic `_iid` so a re-GRANT
    // supersedes; REVOKE is a system-time soft-close.
    private fun writeRoleMembership(
        user: String,
        role: String,
        currentTime: Instant,
        principal: String?,
        revoke: Boolean
    ) {
        if (dbState.name != "xtdb")
            throw Incorrect(
                "Role membership can only be managed on the primary 'xtdb' database.",
                "xtdb/role-membership-non-primary", mapOf("db" to dbState.name),
            )

        if (principal == null || !nodeBase.config.authn.isSuperuser(principal))
            throw Incorrect(
                "Only a superuser may GRANT/REVOKE role membership.",
                "xtdb/not-authorized", mapOf("user" to principal),
            )

        val ts = currentTime.asMicros
        val table = table(TableRef(dbState.name, "xt", "role_membership"))

        table.writeIid(RoleMembership.membershipIid(user, role))
        table.writeValidTimeMicros(ts, MAX_LONG)

        if (revoke) {
            table.endDelete()
        } else {
            table.putDocWriter.apply {
                vectorFor("user", VectorType.UTF8.arrowType, false).writeObject(user)
                vectorFor("role", VectorType.UTF8.arrowType, false).writeObject(role)
                endStruct()
            }

            table.endPut()
        }
    }

    override fun close() = tableTxs.closeAll()

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

    /**
     * Per-table staging area for a tx's writes.
     *
     * Rows are written column-by-column into a shared relation.
     *
     * [writePut] / [writeDelete] / [writeErase] — and the batch [writePuts] / [writeDeletes] / [writeErases] /
     * [patchDocs] — wrap this protocol for callers that don't need the column-level (zero-alloc) control.
     *
     * If you elect to write the values directly, each op must have exactly one value written to each of the columns:
     *
     * * the entity id — [writeId] / (internal only) [writeIid];
     * * the valid-time range — [writeValidTimes] / [writeValidTimeMicros] / [writeDefaultValidTime];
     * * **put only**: the document columns, via [putDocWriter];
     *
     * Then, a terminal [endPut] / [endDelete] / [endErase] — this is what records the row.
     *
     * In columnar style, you may choose to write all your IDs first, then all the valid-times, then all the documents,
     * then the batch [endPuts] / [endDeletes] / [endErases] - it is your responsibility to ensure a consistent number
     * of values in each column.
     *
     * In any event, you must write all the batch's row data before calling the corresponding `end` methods.
     */
    inner class Table internal constructor(val ref: TableRef) : AutoCloseable {

        private fun checkValidTimes(validFrom: Long, validTo: Long) {
            if (validFrom >= validTo) {
                throw Incorrect(
                    "Invalid valid times",
                    "xtdb.indexer/invalid-valid-times",
                    mapOf("valid-from" to fromMicros(validFrom), "valid-to" to fromMicros(validTo)),
                )
            }
        }

        internal val txRelation: Relation = Trie.openLogDataWriter(allocator)

        private val iidVec = txRelation["_iid"]
        private val systemFromVec = txRelation["_system_from"]
        private val validFromVec = txRelation["_valid_from"]
        private val validToVec = txRelation["_valid_to"]
        private val opVec = txRelation["op"]
        private val putVec by lazy(LazyThreadSafetyMode.NONE) { opVec.vectorFor("put", STRUCT_TYPE, false) }
        private val deleteVec = opVec["delete"]
        private val eraseVec = opVec["erase"]

        /** The document-column sink for a put: write the doc's columns here, between the id/valid-time writes and [endPut]. */
        val putDocWriter: VectorWriter by lazy(LazyThreadSafetyMode.NONE) { putVec }

        internal fun declareColumns(colNames: List<String>) {
            for (colName in colNames) putDocWriter.vectorFor(colName, NULL_TYPE, false)
        }

        internal var trie: MemoryHashTrie = MemoryHashTrie.emptyTrie(iidVec)
            private set

        internal fun writeIid(iid: ByteBuffer) = iidVec.writeBytes(iid)
        internal fun writeIid(iid: ByteArray) = iidVec.writeBytes(iid)

        /** Writes the row's entity id. */
        fun writeId(id: Any) = iidVec.writeBytes(id.asIid)

        fun writeValidTimeMicros(validFrom: Long, validTo: Long) {
            validFromVec.writeLong(validFrom)
            validToVec.writeLong(validTo)
        }

        fun writeDefaultValidTime() = writeValidTimeMicros(systemTimeMicros, MAX_LONG)

        @Suppress("IfThenToElvis") // because I wasn't sure that Elvis doesn't box primitives
        fun writeValidTimes(validFrom: Instant? = null, validTo: Instant? = null) =
            writeValidTimeMicros(
                if (validFrom != null) validFrom.asMicros else systemTimeMicros,
                if (validTo != null) validTo.asMicros else MAX_LONG
            )

        private fun endOps(count: Int) {
            val pos = txRelation.rowCount

            repeat(count) { systemFromVec.writeLong(systemTimeMicros) }

            txRelation.endRows(count)

            trie = trie.addRange(pos, count)
        }

        fun endPuts(count: Int) = endOps(count)
        fun endPut() = endPuts(1)

        /**
         * Single document put:
         *
         * - the document must contain an `_id` key
         *
         * */
        fun writePut(doc: Map<String, *>, validFrom: Instant? = null, validTo: Instant? = null) {
            writeId(doc["_id"] ?: throw Incorrect("missing _id", "xtdb.tx/missing-id", mapOf("doc" to doc)))
            writeValidTimes(validFrom, validTo)
            putDocWriter.writeObject(doc)
            endPut()
        }

        /**
         * Batch put: writes every row in [rel], content from `doc`.
         *
         * - The relation MUST contain either an `_iid` or an `_id` column.
         * - Per-row valid times come from the rel's `_valid_from` / `_valid_to` columns when present and non-null;
         *   otherwise the [defaultValidFromMicros] / [defaultValidToMicros] defaults apply.
         *
         *   Zero-width or inverted ranges will be rejected.
         * - The relation MUST contain a `doc` column - a struct vector containing the documents.
         */
        fun writePuts(
            rel: RelationReader, defaultValidFromMicros: Long = systemTimeMicros, defaultValidToMicros: Long = MAX_LONG
        ) {
            val rowCount = rel.rowCount
            if (rowCount == 0) return

            val iidRdr = rel.vectorForOrNull("_iid")

            if (iidRdr != null) {
                iidVec.append(iidRdr)
            } else {
                val idRdr = rel["_id"]

                repeat(rowCount) { idx -> iidVec.writeBytes(idRdr.getObject(idx).asIid) }
            }

            val vfRdr = rel.vectorForOrNull("_valid_from")
            val vtRdr = rel.vectorForOrNull("_valid_to")

            val perRowCheck = vfRdr != null || vtRdr != null
            if (!perRowCheck) checkValidTimes(defaultValidFromMicros, defaultValidToMicros)

            repeat(rowCount) { idx ->
                val rowValidFrom =
                    if (vfRdr != null && !vfRdr.isNull(idx)) vfRdr.getLong(idx) else defaultValidFromMicros

                val rowValidTo =
                    if (vtRdr != null && !vtRdr.isNull(idx)) vtRdr.getLong(idx) else defaultValidToMicros

                if (perRowCheck) checkValidTimes(rowValidFrom, rowValidTo)

                validFromVec.writeLong(rowValidFrom)
                validToVec.writeLong(rowValidTo)
            }

            putDocWriter.append(rel["doc"])

            endPuts(rowCount)
        }

        /**
         * Ends [count] DELETE operations.
         *
         * By the time you call this, you MUST have written:
         *
         * - [count] ids, via either [writeId] / [writeIid]
         * - [count] valid times, via [writeValidTimes] / [writeValidTimeMicros] / [writeDefaultValidTime]
         */
        fun endDeletes(count: Int) {
            repeat(count) { deleteVec.writeNull() }
            endOps(count)
        }

        /**
         * Ends a DELETE operation.
         *
         * @see [endDeletes]
         */
        fun endDelete() = endDeletes(1)

        /** Single delete of [id] over the given valid-time range. */
        fun writeDelete(id: Any, validFrom: Instant? = null, validTo: Instant? = null) {
            writeId(id)
            writeValidTimes(validFrom, validTo)
            endDelete()
        }

        /**
         * Batch delete: deletes every row in [rel] (keyed by `_iid`).
         *
         * Per-row valid times come from the rel's `_valid_from` / `_valid_to` columns when present and non-null;
         * otherwise the [defaultValidFromMicros] / [defaultValidToMicros] defaults apply.
         * Rejects zero-width or inverted ranges — one check over the defaults if the rel has no per-row
         * temporal columns; otherwise per-row.
         */
        fun writeDeletes(
            rel: RelationReader, defaultValidFromMicros: Long = systemTimeMicros, defaultValidToMicros: Long = MAX_LONG
        ) {
            val rowCount = rel.rowCount
            if (rowCount == 0) return

            iidVec.append(rel.vectorFor("_iid"))

            val vfRdr = rel.vectorForOrNull("_valid_from")
            val vtRdr = rel.vectorForOrNull("_valid_to")

            val perRowCheck = vfRdr != null || vtRdr != null
            if (!perRowCheck) checkValidTimes(defaultValidFromMicros, defaultValidToMicros)

            repeat(rowCount) { idx ->
                val rowValidFrom =
                    if (vfRdr != null && !vfRdr.isNull(idx)) vfRdr.getLong(idx) else defaultValidFromMicros

                val rowValidTo =
                    if (vtRdr != null && !vtRdr.isNull(idx)) vtRdr.getLong(idx) else defaultValidToMicros

                if (perRowCheck) checkValidTimes(rowValidFrom, rowValidTo)

                validFromVec.writeLong(rowValidFrom)
                validToVec.writeLong(rowValidTo)
            }

            endDeletes(rowCount)
        }

        /**
         * Ends [count] ERASE operations.
         *
         * By the time you call this, you MUST have written:
         *
         * - [count] ids, via either [writeId] / [writeIid]
         */
        fun endErases(count: Int) {
            repeat(count) { eraseVec.writeNull() }
            endOps(count)
        }

        /**
         * Ends an ERASE operation.
         *
         * @see [endErases]
         */
        fun endErase() = endErases(1)

        /** Single erase of [id] across all valid time, in one call. */
        fun writeErase(id: Any) {
            writeId(id)
            writeValidTimeMicros(MIN_LONG, MAX_LONG)
            endErase()
        }

        /**
         * Batch erase: erases every iid in [iids] across all valid time.
         */
        fun writeErases(iids: VectorReader) {
            val rowCount = iids.valueCount
            if (rowCount == 0) return

            iidVec.append(iids)
            repeat(rowCount) { writeValidTimeMicros(MIN_LONG, MAX_LONG) }
            endErases(rowCount)
        }

        /**
         * Bulk-patch docs into this table, keyed by `_iid`, preserving existing columns where
         * the patched doc doesn't supply a new value.
         *
         * [docs] carries one row per doc, columns `_iid` (fixed-size binary) and `doc` (struct).
         * [validFromMicros] / [validToMicros] default to [MIN_LONG] / [MAX_LONG] — the start/end of
         * valid time — on the Instant overload, a null on either side means the same thing.
         *
         * Reads existing docs through the tx's read-your-writes [queryCatalog].
         */
        fun patchDocs(docs: RelationReader, validFromMicros: Long, validToMicros: Long) {
            checkNotForbidden(ref)

            val prepareOpts = PersistentArrayMap.create(
                mapOf(
                    "current-time".kw to txKey.systemTime,
                    "default-db".kw to dbState.name,
                    "arg-fields".kw to docs.schema.fields,
                )
            )

            // The planner (plan-patch in sql.clj) requires concrete Instant bounds — no nil support.
            // The MIN/MAX µs bounds become start-of-time / end-of-time Instants; the planner treats
            // them as literals and doesn't round-trip them through `asMicros`, so the lossiness of
            // those sentinel Instants doesn't bite here.
            val pq = nodeBase.querySource.preparePatchDocsQuery(
                ref, validFromMicros.microsAsInstant, validToMicros.microsAsInstant,
                queryCatalog, prepareOpts,
            )

            val patchArgs = RelationReader.from(
                listOf(SingletonListReader("?patch_docs", RelationAsStructReader("patch_doc", docs)))
            )

            val qOpts = xtdb.query.QueryOpts(txKey.systemTime, tracer = tracer)

            // openQuery takes ownership of the sliced args and closes them when the cursor closes —
            // no separate `.use` on the slice here, or it'd double-free.
            pq.openQuery(patchArgs.openSlice(allocator), qOpts).use { cursor ->
                cursor.forEachRemaining { rel -> writePuts(rel, validFromMicros, validToMicros) }
            }
        }

        /**
         * Instant-based overload of [patchDocs] for end-user callers working with `java.time.Instant`.
         * Null on either bound means the same as passing [systemTimeMicros] / [MAX_LONG] to the µs overload.
         */
        @JvmOverloads
        fun patchDocs(docs: RelationReader, validFrom: Instant? = null, validTo: Instant? = null) =
            patchDocs(docs, validFrom?.asMicros ?: systemTimeMicros, validTo?.asMicros ?: MAX_LONG)

        internal fun serializeTxData(): ByteArray = txRelation.asArrowStream

        override fun close() {
            txRelation.close()
        }
    }
}
