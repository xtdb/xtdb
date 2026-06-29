@file:UseSerializers(ZoneIdSerde::class)

package xtdb.api

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import org.apache.arrow.adbc.core.*
import org.apache.arrow.adbc.core.AdbcConnection.GetObjectsDepth
import org.apache.arrow.adbc.core.AdbcStatement.QueryResult
import org.apache.arrow.adbc.core.AdbcStatement.UpdateResult
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.UInt4Vector
import org.apache.arrow.vector.VectorLoader
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.VectorUnloader
import org.apache.arrow.vector.complex.DenseUnionVector
import org.apache.arrow.vector.ipc.ArrowReader
import org.apache.arrow.vector.types.pojo.Schema
import xtdb.ResultCursor
import xtdb.ZoneIdSerde
import xtdb.antlr.Sql
import xtdb.api.Authenticator.Factory.SingleRootUser
import xtdb.api.log.Log
import xtdb.api.log.MessageId
import xtdb.api.metrics.HealthzConfig
import xtdb.api.metrics.TracerConfig
import xtdb.api.module.XtdbModule
import xtdb.api.storage.Storage
import xtdb.arrow.*
import xtdb.arrow.VectorType.Companion.ofType
import xtdb.cache.DiskCache
import xtdb.cache.MemoryCache
import xtdb.database.Database
import xtdb.database.DatabaseName
import xtdb.database.encodeTxBasisToken
import xtdb.database.mergeTxBasisTokens
import xtdb.error.Anomaly
import xtdb.error.Fault
import xtdb.error.Incorrect
import xtdb.indexer.DatabaseSnapshot
import xtdb.query.*
import xtdb.table.TableRef
import xtdb.time.asInstant
import xtdb.tx.TxOp
import xtdb.tx.TxOpts
import xtdb.util.closeOnCatch
import xtdb.util.requiringResolve
import xtdb.util.useAll
import java.nio.file.Files
import java.nio.file.Path
import java.time.*
import java.util.Date
import java.util.UUID.randomUUID
import java.util.concurrent.ExecutionException
import kotlin.io.path.extension

private fun parseSkipDbsEnv(skipDbs: String): Set<String> =
    skipDbs.split(",").map { it.trim() }.filter { it.isNotEmpty() }.toSet()

interface Xtdb : DataSource, AdbcDatabase, AutoCloseable {

    val allocator: BufferAllocator

    val serverPort: Int
    val serverReadOnlyPort: Int
    val flightSqlPort: Int

    interface CompactorNode : AutoCloseable

    interface XtdbInternal : Xtdb {
        val dbCatalog: Database.Catalog
    }

    fun <T : XtdbModule> module(type: Class<T>): T?

    fun addMeterRegistry(meterRegistry: MeterRegistry)

    override fun connect(): Connection

    fun prepareSql(sql: String, opts: Any?): PreparedQuery
    fun prepareSql(sql: Sql.DirectlyExecutableStatementContext, opts: Any?): PreparedQuery

    data class SubmittedTx(val txId: MessageId)

    fun submitTx(dbName: DatabaseName, ops: List<TxOp>, opts: TxOpts): SubmittedTx

    data class ExecutedTx(val txId: MessageId, val systemTime: Instant, val committed: Boolean, val error: Throwable?)

    fun executeTx(dbName: DatabaseName, ops: List<TxOp>, opts: Any?): ExecutedTx

    interface Statement : AdbcStatement {
        fun bind(rel: RelationReader): Unit = unsupported("bind(RelationReader) not supported")
    }

    /**
     * A connection-scoped handle onto the node, implementing ADBC's [AdbcConnection] with XTDB's native
     * surface (await-token-threaded reads/writes) alongside the standard one. Shared by the protocol
     * frontends — pgwire uses the native methods; ADBC and Flight SQL use the [AdbcConnection] surface.
     *
     * Each write advances [awaitToken]; each read threads it through, so the planning snapshot can see this
     * connection's own writes. Because writes and reads go through here, no callsite can forget to do either.
     *
     * [awaitToken] is exposed only for the pgwire `SET`/`SHOW AWAIT_TOKEN` SQL feature.
     *
     * [defaultTz] is the connection's timezone, mutable for the pgwire `SET TIME ZONE` SQL feature. It carries
     * only the zone; the current-time source stays with the frontend (pgwire's session clock).
     */
    class Connection(
        private val allocator: BufferAllocator,
        private val dbCat: Database.Catalog,
        private val qSrc: IQuerySource,
        private val sqlPlanner: SqlPlanner,
        private val clock: Clock,
        var defaultTz: ZoneId,
        private val txErrorCounter: Counter?,
        private val txAwaitTimer: Timer?,
        private val txSubmitTimer: Timer?,
        private val txExecuteTimer: Timer?,
        var dbName: DatabaseName,
    ) : AdbcConnection {

        private var _awaitToken: String? = null

        // read-only to the outside: the token advances only through the write methods (submit / execute /
        // attach / detach), so no other code can leave it stale.
        val awaitToken: String? get() = _awaitToken

        // the one sanctioned external writer: pgwire's `SET AWAIT_TOKEN`.
        fun setAwaitToken(token: String?) { _awaitToken = token }

        // the connection's last write, for `SHOW latest_submitted_tx`. system-time/committed/error are null
        // for a fire-and-forget submitTx (no await); populated for an awaited execute/attach/detach.
        data class LastSubmittedTx(
            val txId: MessageId,
            val systemTime: Instant?,
            val committed: Boolean?,
            val error: Throwable?,
        )

        var lastSubmittedTx: LastSubmittedTx? = null
            private set

        // how long to wait for this connection's writes to become visible before giving up, shared by every
        // read this connection prepares or snapshots
        private val awaitTimeout: Duration = Duration.ofMinutes(1)

        private var autoCommit = true
        private var tx: Transaction? = null

        private fun db(dbName: DatabaseName): Database =
            dbCat.databaseOrNull(dbName)
                ?: throw Incorrect("Unknown database: $dbName", "xtdb/unknown-db", mapOf("db-name" to dbName))

        private fun recordTx(dbName: DatabaseName, txId: MessageId) {
            _awaitToken = mergeTxBasisTokens(_awaitToken, mapOf(dbName to listOf(txId)).encodeTxBasisToken())
        }

        // advances the token and captures the last-submitted-tx in one place, so the two can't drift apart.
        private fun SubmittedTx.record(dbName: DatabaseName): SubmittedTx = also {
            recordTx(dbName, txId)
            lastSubmittedTx = LastSubmittedTx(txId, null, null, null)
        }

        private fun ExecutedTx.record(dbName: DatabaseName): ExecutedTx = also {
            recordTx(dbName, txId)
            lastSubmittedTx = LastSubmittedTx(txId, systemTime, committed, error)
        }

        private inline fun <T> Timer?.timed(body: () -> T): T {
            val sample = Timer.start()
            return try {
                body()
            } finally {
                this?.let { sample.stop(it) }
            }
        }

        // unwrap ExecutionException → cause (mirrors util/rethrowing-cause), counting Anomalies on the way out
        private fun doSubmit(ops: List<TxOp>, opts: TxOpts): SubmittedTx =
            try {
                try {
                    db(dbName).submitTxBlocking(ops, opts.withFallbackTz(defaultTz))
                } catch (e: ExecutionException) {
                    throw e.cause ?: e
                }
            } catch (e: Anomaly) {
                txErrorCounter?.increment()
                throw e
            }

        fun submitTx(ops: List<TxOp>, opts: TxOpts = TxOpts()): SubmittedTx =
            txSubmitTimer.timed { doSubmit(ops, opts) }.record(dbName)

        fun executeTx(ops: List<TxOp>, opts: TxOpts = TxOpts()): ExecutedTx =
            txExecuteTimer.timed {
                val txId = doSubmit(ops, opts).txId
                txAwaitTimer.timed { awaitTx(txId) }
            }.record(dbName)

        // attach/detach act on the primary database, not the connection's: the message goes onto the primary's
        // log, and the resulting tx is awaited and recorded against the primary (it.name), not dbName.
        fun attachDb(dbName: DatabaseName, config: Database.Config): ExecutedTx =
            dbCat.primary.let { awaitTx(it.sendAttachDbMessage(dbName, config).msgId, it.name).record(it.name) }

        fun detachDb(dbName: DatabaseName): ExecutedTx =
            dbCat.primary.let { awaitTx(it.sendDetachDbMessage(dbName).msgId, it.name).record(it.name) }

        private fun TransactionResult.toExecutedTx() =
            ExecutedTx(
                txKey.txId, txKey.systemTime,
                this is TransactionResult.Committed,
                (this as? TransactionResult.Aborted)?.error
            )

        private fun awaitTx(txId: MessageId, awaitDb: DatabaseName = dbName): ExecutedTx {
            db(awaitDb).awaitTxBlocking(txId, null)?.let { txRes ->
                if (txRes.txKey.txId == txId) return txRes.toExecutedTx()
            }

            return Relation.openFromRows(allocator, listOf(mapOf("?_0" to txId)))
                .closeOnCatch { args ->
                    prepareSql("SELECT system_time, committed, error FROM xt.txs FOR ALL VALID_TIME WHERE _id = ?", awaitDb)
                        .openQuery(args, QueryOpts(null, defaultTz))
                }
                .use { c ->
                    var result: ExecutedTx? = null
                    c.tryAdvance { rel ->
                        if (rel.rowCount > 0)
                            result = ExecutedTx(
                                txId,
                                (rel["system_time"].getObject(0) as ZonedDateTime).toInstant(),
                                rel["committed"].getObject(0) as Boolean,
                                rel["error"].getObject(0) as? Throwable,
                            )
                    }
                    result ?: throw Fault("tx $txId not found after await", "xtdb/tx-await-missing")
                }
        }

        fun prepareSql(sql: String, defaultDb: DatabaseName = dbName): PreparedQuery {
            dbCat.awaitAll(awaitToken, awaitTimeout)
            return qSrc.prepareQuery(
                sql,
                dbCat,
                PrepareOpts(defaultTz = defaultTz, defaultDb = defaultDb, queryText = sql)
            )
        }

        // pgwire parses once for dispatch, so it prepares from the AST and supplies the original text; tz, db,
        // await-token and await bound all come from the connection. EXPLAIN is read off the AST, not passed in.
        fun prepareSql(ast: Sql.DirectlyExecutableStatementContext, queryText: String): PreparedQuery {
            dbCat.awaitAll(awaitToken, awaitTimeout)
            return qSrc.prepareQuery(
                ast,
                dbCat,
                PrepareOpts(defaultTz = defaultTz, defaultDb = dbName, queryText = queryText)
            )
        }

        fun openSqlQuery(sql: String): ResultCursor =
            prepareSql(sql).openQuery(null, queryOpts())

        fun openSnapshot(): DatabaseSnapshot {
            val db = db(dbName)
            dbCat.awaitAll(awaitToken, awaitTimeout)
            return db.openSnapshot()
        }

        override fun createStatement() = object : Statement {
            private var sql: String? = null
            private var prepared: PreparedQuery? = null
            private var args: RelationReader? = null

            private fun clearArgs() {
                args.also { args = null }?.close()
            }

            private fun openQueryArgs(): RelationReader? =
                args?.openSlice(allocator)?.closeOnCatch { sliced ->
                    RelationReader.from(
                        sliced.vectors.mapIndexed { idx, v -> v.withName("?_$idx") },
                        sliced.rowCount
                    )
                }

            override fun setSqlQuery(sql: String) {
                this.sql = sql
                this.prepared = null
                clearArgs()
            }

            override fun prepare() {
                val sql = this.sql ?: throw Incorrect("SQL query not set", "xtdb.adbc/no-sql")
                prepared = prepareSql(sql)
            }

            override fun getParameterSchema(): Schema {
                val pq = prepared ?: throw Incorrect("call prepare() first", "xtdb.adbc/not-prepared")
                return Schema(
                    (0 until pq.paramCount).map { idx -> "\$$idx" ofType VectorType.fromLegs() }
                )
            }

            override fun executeSchema(): Schema {
                val pq = prepared ?: throw Incorrect("call prepare() first", "xtdb.adbc/not-prepared")

                val paramFields = (0 until pq.paramCount).map { idx -> "?_$idx" ofType VectorType.fromLegs() }

                return Schema(pq.getColumnFields(paramFields))
            }

            override fun bind(root: VectorSchemaRoot) {
                clearArgs()
                args = Relation(allocator, root.schema).closeOnCatch { copy ->
                    Relation.fromRoot(allocator, root).use { src ->
                        src.rowCopier(copy).copyRange(0, src.rowCount)
                    }
                    copy
                }
            }

            override fun bind(rel: RelationReader) {
                clearArgs()
                args = rel.openSlice(allocator)
            }

            override fun executeQuery(): QueryResult {
                val sql = sql ?: throw Incorrect("SQL query not set", "xtdb.adbc/no-sql")
                resolveForQuery()
                if (prepared == null && args != null)
                    throw Incorrect(
                        "call prepare() before executeQuery() when parameters are bound",
                        "xtdb.adbc/bind-without-prepare",
                    )

                val queryArgs = openQueryArgs()
                val cursor = try {
                    prepared?.openQuery(queryArgs, queryOpts()) ?: openSqlQuery(sql)
                } catch (t: Throwable) {
                    queryArgs?.close()
                    throw t
                }
                val schema = Schema(cursor.resultTypes.map { (name, type) -> type.toField(name) })
                return QueryResult(-1, cursorToArrowReader(cursor, schema))
            }

            override fun executeUpdate(): UpdateResult {
                val sql = sql ?: throw Incorrect("SQL query not set", "xtdb.adbc/no-sql")

                val stmt = parseStatements(sql).singleOrNull()
                    ?: throw Incorrect(
                        "executeUpdate expects exactly one SQL statement",
                        "xtdb.adbc/expected-single-statement",
                        mapOf("sql" to sql)
                    )
                when (stmt) {
                    is ParsedStatement.Begin -> beginParsedTx(stmt.txOptions, sql)

                    is ParsedStatement.Commit -> commitTx()
                    is ParsedStatement.Rollback -> rollbackTx()
                    else -> executeDml(TxOp.Sql(sql, openQueryArgs()))
                }
                return UpdateResult(-1)
            }

            override fun close() {
                clearArgs()
                prepared = null
            }
        }

        // Manual-tx DML buffer: consecutive same-SQL ops coalesce, their args appended into the last op's own
        // relation, so a prepared INSERT in a loop becomes one multi-row op.
        private class DmlBuffer(private val al: BufferAllocator) : AutoCloseable {
            private val ops = mutableListOf<TxOp>()

            fun add(op: TxOp) {
                if (op !is TxOp.Sql || op.args == null) {
                    ops.add(op)
                    return
                }

                val target = (ops.lastOrNull() as? TxOp.Sql)?.takeIf { it.sql == op.sql }?.args as? RelationWriter
                if (target != null) {
                    op.args.use(target::append)
                } else {
                    val rel = Relation(al)
                    rel.closeOnCatch { op.args.use(rel::append) }
                    ops.add(TxOp.Sql(op.sql, rel))
                }
            }

            fun drain(): List<TxOp> = ops.toList().also { ops.clear() }

            override fun close() = ops.forEach { it.close() }
        }

        // resolved on the first statement: a query makes it read-only, DML makes it read-write. In XTDB a
        // "read-write" tx is write-only — queries are rejected in a DML tx, DML in a read-only one.
        private sealed interface AccessMode : AutoCloseable

        private data object ReadOnly : AccessMode {
            override fun close() {}
        }

        private inner class ReadWrite(
            val systemTime: Instant? = null,
            val userMetadata: Map<*, *>? = null,
            val async: Boolean = false,
        ) : AccessMode {
            val buffer = DmlBuffer(allocator)

            override fun close() = buffer.close()
        }

        // The read basis pinned at BEGIN and observed by every query in the transaction — the lever for
        // cross-query snapshot isolation. Carried by an unresolved tx and a ReadOnly one (resolving the
        // former to the latter inherits it unchanged); a ReadWrite tx has none — writes don't read.
        private class ReadBasis(val snapshotToken: String?, val snapshotTime: Instant?, val currentTime: Instant)

        private class Transaction(var mode: AccessMode? = null, val readBasis: ReadBasis? = null) : AutoCloseable {
            var failed: Throwable? = null

            override fun close() {
                mode?.close()
            }
        }

        // Pin the begin-time read basis: await this connection's own writes, then snapshot the data
        // (latest-completed system-times) and the clock. A READ ONLY WITH clause overrides any of the three.
        private fun pinReadBasis(
            snapshotToken: String? = null, snapshotTime: Instant? = null, currentTime: Instant? = null
        ): ReadBasis {
            dbCat.awaitAll(awaitToken, awaitTimeout)
            return ReadBasis(snapshotToken ?: dbCat.snapshotToken(), snapshotTime, currentTime ?: clock.instant())
        }

        // Open an explicit transaction (SQL BEGIN). [accessMode] fixes the mode — READ ONLY rejects writes,
        // READ WRITE buffers them; a bare BEGIN leaves it unresolved until the first statement resolves it.
        // [readBasis] pins the begin-time read snapshot (null for an explicit READ WRITE — writes don't read).
        // Coexists with autoCommit — an open tx captures subsequent DML (see executeDml) until COMMIT/ROLLBACK.
        private fun beginTx(accessMode: AccessMode?, readBasis: ReadBasis?) {
            if (tx != null) throw Incorrect("transaction already started", "xtdb/tx-already-open")
            tx = Transaction(accessMode, readBasis)
        }

        // a bare BEGIN pins a basis even while unresolved: if a query resolves it to read-only the basis is
        // inherited; if DML resolves it to read-write the basis is simply ignored.
        fun beginTx() = beginTx(null, pinReadBasis())

        fun beginReadOnly() = beginTx(ReadOnly, pinReadBasis())

        fun beginWriteOnly(systemTime: Instant? = null, userMetadata: Map<*, *>? = null, async: Boolean = false) =
            beginTx(ReadWrite(systemTime, userMetadata, async), null)

        // Coerce a planned BEGIN-option value to an Instant, mirroring xtdb.time/->instant so the connection and
        // pgwire agree on the system-time a SQL temporal literal resolves to. SqlPlanner plans `TIMESTAMP '…'`
        // to a java.time temporal (ZonedDateTime when zoned, else LocalDateTime); a bare string falls back to
        // the SQL-timestamp parse. LocalDate/LocalDateTime are anchored in [defaultTz], as pgwire does.
        private fun coerceInstant(value: Any?): Instant? = when (value) {
            null -> null
            is Instant -> value
            is ZonedDateTime -> value.toInstant()
            is OffsetDateTime -> value.toInstant()
            is LocalDateTime -> value.atZone(defaultTz).toInstant()
            is LocalDate -> value.atStartOfDay(defaultTz).toInstant()
            is Date -> value.toInstant()
            is String -> value.asInstant(defaultTz)
            else -> throw Incorrect(
                "cannot coerce SYSTEM_TIME option to an instant",
                "xtdb.adbc/invalid-system-time",
                mapOf("value" to value, "type" to value::class.java.name)
            )
        }

        // Begin an explicit tx from parsed WITH options, evaluating each option expression via the injected
        // SqlPlanner. BEGIN options carry no bound params, so args is null.
        private fun beginParsedTx(opts: ParsedStatement.TxOptions, sql: String) {
            fun rejectOpt(): Nothing = throw Incorrect(
                "BEGIN ... WITH option is not yet supported here",
                "xtdb.adbc/unsupported-tx-option",
                mapOf("sql" to sql)
            )

            when (opts.accessMode) {
                ParsedStatement.AccessMode.READ_WRITE -> {
                    if (opts.defaultTz != null) throw Incorrect(
                        "BEGIN READ WRITE WITH (TIMEZONE …) is not yet supported",
                        "xtdb.adbc/unsupported-tx-option",
                        mapOf("sql" to sql)
                    )

                    val systemTime = coerceInstant(opts.systemTime?.let { sqlPlanner.evalLiteral(it, null) })
                    val userMetadata = opts.userMetadata?.let { sqlPlanner.evalLiteral(it, null) } as Map<*, *>?
                    val async = (opts.async?.let { sqlPlanner.evalLiteral(it, null) } as Boolean?) ?: false

                    beginWriteOnly(systemTime, userMetadata, async)
                }

                ParsedStatement.AccessMode.READ_ONLY -> {
                    // SNAPSHOT_TOKEN / SNAPSHOT_TIME / CLOCK_TIME override the begin-time auto-pin. AWAIT_TOKEN
                    // (the only other READ ONLY option in the grammar) isn't wired through yet — reject it fail-closed.
                    if (opts.awaitToken != null) rejectOpt()

                    val snapshotToken = opts.snapshotToken?.let { sqlPlanner.evalLiteral(it, null) } as String?
                    val snapshotTime = coerceInstant(opts.snapshotTime?.let { sqlPlanner.evalLiteral(it, null) })
                    val currentTime = coerceInstant(opts.clockTime?.let { sqlPlanner.evalLiteral(it, null) })

                    beginTx(ReadOnly, pinReadBasis(snapshotToken, snapshotTime, currentTime))
                }

                null -> beginTx()
            }
        }

        fun commitTx() {
            val tx = tx ?: return
            this.tx = null
            // a ReadWrite tx commits even when its buffer is empty — an explicit write tx records a transaction
            // (and advances the await-token); a ReadOnly / still-unresolved tx is a no-op.
            val mode = tx.mode as? ReadWrite ?: return
            // defaultTz/user left null: doSubmit fills defaultTz via withFallbackTz, and there's no connection user yet.
            val opts = TxOpts(systemTime = mode.systemTime, userMetadata = mode.userMetadata)
            mode.buffer.drain().useAll { ops -> if (mode.async) submitTx(ops, opts) else executeTx(ops, opts) }
        }

        internal fun executeDml(op: TxOp) {
            // auto-execute only when no explicit tx is open; an open tx (from beginTx) buffers even under autoCommit.
            if (autoCommit && tx == null) {
                listOf(op).useAll { ops -> executeTx(ops) }
                return
            }

            val tx = tx ?: Transaction().also { tx = it }

            when (val mode = tx.mode) {
                is ReadWrite -> mode.buffer.add(op)
                null -> ReadWrite().also { tx.mode = it }.buffer.add(op)
                is ReadOnly -> {
                    op.close()
                    throw Incorrect("Cannot write in a read-only transaction", "xtdb/read-only-tx")
                }
            }
        }

        // The read counterpart of executeDml's access-mode resolution: a query resolves an unresolved tx to
        // read-only, and is rejected in a read-write (DML) tx — in XTDB a write tx is write-only.
        private fun resolveForQuery() {
            val tx = tx
            if (tx == null) {
                // manual mode mirrors executeDml's lazy open: a query opens a read-only tx pinning the
                // begin-time basis, so subsequent reads share its snapshot. Under autocommit there is no open
                // tx — the query reads latest at the connection's current basis, unchanged.
                if (!autoCommit) this.tx = Transaction(ReadOnly, pinReadBasis())
                return
            }
            when (tx.mode) {
                is ReadWrite -> throw Incorrect(
                    "Queries are unsupported in a DML transaction", "xtdb/queries-in-read-write-tx"
                )

                is ReadOnly -> {}
                null -> tx.mode = ReadOnly
            }
        }

        // QueryOpts for a user read: an open tx's pinned basis (snapshot isolation), else null fields so the
        // planner defaults to latest data and the wall-clock now (autocommit / no open tx).
        private fun queryOpts() = tx?.readBasis.let { b ->
            QueryOpts(b?.currentTime, defaultTz, b?.snapshotToken, b?.snapshotTime)
        }

        override fun setAutoCommit(autoCommit: Boolean) {
            if (this.autoCommit == autoCommit) return
            if (autoCommit) commitTx()
            this.autoCommit = autoCommit
        }

        override fun commit() {
            if (autoCommit)
                throw Incorrect("Cannot commit when autoCommit is enabled", "xtdb.adbc/commit-in-autocommit")

            commitTx()
        }

        // SQL ROLLBACK: discards the open tx regardless of autoCommit (unlike [rollback], which rejects rollback in
        // autocommit). The explicit-tx mechanism the SQL tx-control path drives.
        private fun rollbackTx() {
            tx?.close()
            tx = null
        }

        override fun rollback() {
            if (autoCommit)
                throw Incorrect("Cannot rollback when autoCommit is enabled", "xtdb.adbc/rollback-in-autocommit")

            rollbackTx()
        }

        override fun bulkIngest(targetTableName: String, mode: BulkIngestMode): Statement {
            require(mode == BulkIngestMode.CREATE_APPEND) { "Only CREATE_APPEND mode is supported" }

            val splitTable = targetTableName.split('.').reversed()
            val tableName = splitTable.first()
            val schemaName = splitTable.getOrNull(1) ?: "public"

            return object : Statement {
                override fun executeQuery(): QueryResult =
                    throw Incorrect("Bulk ingest does not support queries", "xtdb.adbc/bulk-ingest-no-query")

                override fun prepare(): Unit =
                    throw Incorrect("Bulk ingest does not support prepare", "xtdb.adbc/bulk-ingest-no-prepare")

                private var docs: RelationReader? = null

                override fun close() {
                    docs.also { docs = null }?.close()
                }

                override fun bind(root: VectorSchemaRoot) {
                    this.docs?.close()
                    docs = Relation(allocator, root.schema).closeOnCatch { copy ->
                        Relation.fromRoot(allocator, root).use { src ->
                            src.rowCopier(copy).copyRange(0, src.rowCount)
                        }
                        copy
                    }
                }

                override fun bind(rel: RelationReader) {
                    this.docs = rel.openSlice(allocator)
                }

                override fun executeUpdate(): UpdateResult {
                    val docs = docs.also { this.docs = null } ?: return UpdateResult(0)
                    val rowCount = docs.rowCount.toLong()
                    // TODO valid-from/valid-to for the batch
                    executeDml(TxOp.PutDocs(schemaName, tableName, docs = docs))
                    return UpdateResult(rowCount)
                }
            }
        }

        private fun cursorToArrowReader(cursor: ResultCursor, schema: Schema): ArrowReader =
            object : ArrowReader(allocator) {
                override fun readSchema() = schema

                override fun loadNextBatch(): Boolean =
                    cursor.tryAdvance { inRel ->
                        inRel.openDirectSlice(allocator).use { rel ->
                            val loader = VectorLoader(vectorSchemaRoot)
                            rel.openArrowRecordBatch().use { rb ->
                                loader.load(rb)
                            }
                        }
                    }

                override fun bytesRead() = -1L

                override fun closeReadSource() = cursor.close()
            }

        /**
         * Returns an ArrowReader that yields a single batch, built from a pre-populated VectorSchemaRoot.
         */
        private fun singleBatchReader(
            schema: Schema,
            populate: (BufferAllocator, VectorSchemaRoot) -> Unit
        ): ArrowReader =
            object : ArrowReader(allocator) {
                private var consumed = false

                override fun readSchema() = schema

                override fun loadNextBatch(): Boolean {
                    if (consumed) return false
                    consumed = true

                    VectorSchemaRoot.create(schema, allocator).use { tmpRoot ->
                        tmpRoot.allocateNew()
                        populate(allocator, tmpRoot)

                        val loader = VectorLoader(vectorSchemaRoot)
                        VectorUnloader(tmpRoot).recordBatch.use { rb -> loader.load(rb) }
                    }

                    return true
                }

                override fun bytesRead() = -1L
                override fun closeReadSource() = Unit
            }

        /** Returns an ArrowReader yielding a single batch, built inline via the xtdb.arrow [Relation] API. */
        private fun relationBatchReader(schema: Schema, populate: (Relation) -> Unit): ArrowReader =
            object : ArrowReader(allocator) {
                private var consumed = false

                override fun readSchema() = schema

                override fun loadNextBatch(): Boolean {
                    if (consumed) return false
                    consumed = true

                    Relation(allocator, schema).use { rel ->
                        populate(rel)
                        rel.openArrowRecordBatch().use { rb -> VectorLoader(vectorSchemaRoot).load(rb) }
                    }

                    return true
                }

                override fun bytesRead() = -1L
                override fun closeReadSource() = Unit
            }

        override fun getTableTypes(): ArrowReader =
            relationBatchReader(StandardSchemas.TABLE_TYPES_SCHEMA) { rel ->
                rel.vectorFor("table_type").writeObject("TABLE")
                rel.endRow()
            }

        // stays on VectorSchemaRoot: the ADBC GET_INFO schema uses uint32 (no unsigned vector in xtdb.arrow)
        // and a dense union whose member type-ids are part of the wire contract — building it via the typed
        // API would risk diverging from the standard schema.
        override fun getInfo(infoCodes: IntArray?): ArrowReader {
            val codes = infoCodes?.toSet() ?: AdbcInfoCode.entries.map { it.value }.toSet()

            return singleBatchReader(StandardSchemas.GET_INFO_SCHEMA) { _, root ->
                val infoNameVec = root.getVector("info_name") as UInt4Vector
                val infoValueVec = root.getVector("info_value") as DenseUnionVector
                val stringVec = infoValueVec.getVarCharVector(0)

                var idx = 0

                fun addStringInfo(code: AdbcInfoCode, value: String) {
                    if (code.value !in codes) return
                    infoNameVec.setSafe(idx, code.value)
                    infoValueVec.setTypeId(idx, 0)
                    infoValueVec.offsetBuffer.setInt(idx.toLong() * DenseUnionVector.OFFSET_WIDTH, idx)
                    stringVec.setSafe(idx, value.toByteArray())
                    idx++
                }

                addStringInfo(AdbcInfoCode.VENDOR_NAME, "XTDB")
                addStringInfo(AdbcInfoCode.VENDOR_VERSION, "dev")
                addStringInfo(AdbcInfoCode.DRIVER_NAME, "XTDB ADBC Driver")
                addStringInfo(AdbcInfoCode.DRIVER_VERSION, "dev")

                infoValueVec.valueCount = idx
                root.rowCount = idx
            }
        }

        override fun getTableSchema(catalog: String?, dbSchema: String?, tableName: String): Schema =
            openSnapshot().use { snap ->
                getTableSchema(catalog ?: dbName, TableRef(dbSchema ?: "public", tableName), snap)
            }

        fun getTableSchema(dbSchema: String, tableName: String, snap: DatabaseSnapshot): Schema =
            getTableSchema(dbName, TableRef(dbSchema, tableName), snap)

        private fun getTableSchema(catalog: DatabaseName, table: TableRef, snap: DatabaseSnapshot): Schema {
            val types = dbCat.databaseOrNull(catalog)?.getColumnTypes(table, snap) ?: return Schema(emptyList())
            return Schema(types.entries.map { (name, type) -> type.toField(name) })
        }

        override fun getObjects(
            depth: GetObjectsDepth,
            catalogPattern: String?,
            dbSchemaPattern: String?,
            tableNamePattern: String?,
            tableTypes: Array<out String>?,
            columnNamePattern: String?
        ): ArrowReader {
            val catalogs = dbCat.databaseNames
                .filter { catalogPattern == null || catalogPattern == "%" || it == catalogPattern }

            // build the payload as nested maps/lists matching GET_OBJECTS_SCHEMA; the typed vector tree comes
            // from the schema and writeObject fills present keys (omitted keys pad null). only the connection's
            // current database gets schema detail.
            fun tableObj(table: TableInfo): Map<String, Any?> = buildMap {
                put("table_name", table.name)
                put("table_type", "TABLE")
                if (depth != GetObjectsDepth.TABLES && table.columns.isNotEmpty())
                    put("table_columns", table.columns.mapIndexed { idx, col ->
                        mapOf("column_name" to col.name, "ordinal_position" to idx + 1)
                    })
                // table_constraints omitted -> null
            }

            fun schemaObj(schemaName: String, tables: List<TableInfo>): Map<String, Any?> = buildMap {
                put("db_schema_name", schemaName)
                if (depth != GetObjectsDepth.DB_SCHEMAS)
                    put("db_schema_tables", tables.map(::tableObj))
            }

            return relationBatchReader(StandardSchemas.GET_OBJECTS_SCHEMA) { rel ->
                val catalogNameVec = rel.vectorFor("catalog_name")
                val dbSchemasVec = rel.vectorFor("catalog_db_schemas")

                for (catalog in catalogs) {
                    catalogNameVec.writeObject(catalog)

                    if (depth == GetObjectsDepth.CATALOGS || catalog != dbName) {
                        // at CATALOGS depth, or for databases other than the current one, omit schema detail
                        dbSchemasVec.writeNull()
                    } else {
                        val schemas =
                            querySchemas(dbSchemaPattern, tableNamePattern, tableTypes, columnNamePattern, depth)
                        dbSchemasVec.writeObject(schemas.map { (name, tables) -> schemaObj(name, tables) })
                    }

                    rel.endRow()
                }
            }
        }

        private data class ColumnInfo(val name: String, val dataType: String)
        private data class TableInfo(val name: String, val columns: List<ColumnInfo>)

        private fun querySchemas(
            dbSchemaPattern: String?,
            tableNamePattern: String?,
            tableTypes: Array<out String>?,
            columnNamePattern: String?,
            depth: GetObjectsDepth
        ): Map<String, List<TableInfo>> {
            // XTDB only has table_type='TABLE' currently
            if (tableTypes != null && "TABLE" !in tableTypes) return emptyMap()

            val conditions = mutableListOf<String>()
            dbSchemaPattern?.let { conditions.add("table_schema LIKE '${it.replace("'", "''")}'") }
            tableNamePattern?.let { conditions.add("table_name LIKE '${it.replace("'", "''")}'") }

            val where = if (conditions.isNotEmpty()) "WHERE ${conditions.joinToString(" AND ")}" else ""

            if (depth == GetObjectsDepth.DB_SCHEMAS) {
                val sql = "SELECT DISTINCT table_schema FROM information_schema.tables $where ORDER BY table_schema"
                val result = linkedMapOf<String, List<TableInfo>>()
                openSqlQuery(sql).use { cursor ->
                    cursor.forEachRemaining { rel ->
                        for (i in 0 until rel.rowCount) {
                            result[rel["table_schema"].getObject(i).toString()] = emptyList()
                        }
                    }
                }
                return result
            }

            val needColumns = depth == GetObjectsDepth.ALL
            val sql = if (needColumns) {
                val colConditions = conditions.toMutableList()
                columnNamePattern?.let { colConditions.add("column_name LIKE '${it.replace("'", "''")}'") }
                val colWhere = if (colConditions.isNotEmpty()) "WHERE ${colConditions.joinToString(" AND ")}" else ""
                "SELECT table_schema, table_name, column_name, data_type FROM information_schema.columns $colWhere ORDER BY table_schema, table_name, ordinal_position"
            } else {
                "SELECT DISTINCT table_schema, table_name FROM information_schema.tables $where ORDER BY table_schema, table_name"
            }

            val result = linkedMapOf<String, MutableList<TableInfo>>()

            if (needColumns) {
                val tableColumns = linkedMapOf<Pair<String, String>, MutableList<ColumnInfo>>()
                openSqlQuery(sql).use { cursor ->
                    cursor.forEachRemaining { rel ->
                        for (i in 0 until rel.rowCount) {
                            val schema = rel["table_schema"].getObject(i).toString()
                            val table = rel["table_name"].getObject(i).toString()
                            val column = rel["column_name"].getObject(i).toString()
                            val dataType = rel["data_type"].getObject(i).toString()
                            tableColumns.getOrPut(schema to table) { mutableListOf() }
                                .add(ColumnInfo(column, dataType))
                        }
                    }
                }
                for ((key, cols) in tableColumns) {
                    result.getOrPut(key.first) { mutableListOf() }
                        .add(TableInfo(key.second, cols))
                }
            } else {
                openSqlQuery(sql).use { cursor ->
                    cursor.forEachRemaining { rel ->
                        for (i in 0 until rel.rowCount) {
                            val schema = rel["table_schema"].getObject(i).toString()
                            val table = rel["table_name"].getObject(i).toString()
                            result.getOrPut(schema) { mutableListOf() }
                                .add(TableInfo(table, emptyList()))
                        }
                    }
                }
            }

            return result
        }

        override fun getCurrentCatalog(): String = dbName

        override fun setCurrentCatalog(catalog: String) {
            dbName = catalog
        }

        override fun getCurrentDbSchema(): String = "public"

        override fun close() {
            rollbackTx()
        }
    }

    @Serializable
    data class Config(
        var server: ServerConfig? = ServerConfig(),
        var flightSql: FlightSqlConfig? = FlightSqlConfig(),
        var logClusters: Map<RemoteAlias, Remote.Factory<*>> = emptyMap(),
        var remotes: Map<RemoteAlias, Remote.Factory<*>> = emptyMap(),
        var log: Log.Factory = Log.inMemoryLog,
        var storage: Storage.Factory = Storage.inMemory(),
        val memoryCache: MemoryCache.Factory = MemoryCache.Factory(),
        var diskCache: DiskCache.Factory? = null,
        var healthz: HealthzConfig? = null,
        var defaultTz: ZoneId = ZoneOffset.UTC,
        val indexer: IndexerConfig = IndexerConfig(),
        val compactor: CompactorConfig = CompactorConfig(),
        var authn: Authenticator.Factory = SingleRootUser(),
        var garbageCollector: GarbageCollectorConfig = GarbageCollectorConfig(),
        var tracer: TracerConfig = TracerConfig(),
        var readOnlyDatabases: Boolean = false,
        var skipDbs: Set<String> = System.getenv("XTDB_SKIP_DBS")?.let(::parseSkipDbsEnv).orEmpty(),
        var nodeId: String = System.getenv("XTDB_NODE_ID") ?: randomUUID().toString().takeWhile { it != '-' }
    ) {
        var allocator: BufferAllocator? = null

        private val modules: MutableList<XtdbModule.Factory> = mutableListOf()

        fun logClusters(clusters: Map<RemoteAlias, Remote.Factory<*>>) = apply { logClusters += clusters }

        fun logCluster(alias: RemoteAlias, cluster: Remote.Factory<*>) =
            apply { logClusters += alias to cluster }

        fun remotes(remotes: Map<RemoteAlias, Remote.Factory<*>>) = apply { this.remotes += remotes }

        fun remote(alias: RemoteAlias, remote: Remote.Factory<*>) =
            apply { this.remotes += alias to remote }

        fun log(log: Log.Factory) = apply { this.log = log }
        fun storage(storage: Storage.Factory) = apply { this.storage = storage }

        fun diskCache(diskCache: DiskCache.Factory?) = apply { this.diskCache = diskCache }

        @JvmSynthetic
        fun server(configure: ServerConfig.() -> Unit) = apply { (server ?: ServerConfig()).configure() }

        @JvmSynthetic
        fun flightSql(configure: FlightSqlConfig.() -> Unit) = apply { (flightSql ?: FlightSqlConfig()).configure() }

        @JvmSynthetic
        fun indexer(configure: IndexerConfig.() -> Unit) = apply { indexer.configure() }

        @JvmSynthetic
        fun compactor(configure: CompactorConfig.() -> Unit) = apply { compactor.configure() }

        fun healthz(healthz: HealthzConfig) = apply { this.healthz = healthz }

        fun tracer(tracer: TracerConfig) = apply { this.tracer = tracer }

        @JvmSynthetic
        fun tracer(configure: TracerConfig.() -> Unit) = tracer(TracerConfig().also(configure))

        fun garbageCollector(garbageCollector: GarbageCollectorConfig) =
            apply { this.garbageCollector = garbageCollector }

        @JvmSynthetic
        fun garbageCollector(configure: GarbageCollectorConfig.() -> Unit) =
            garbageCollector(GarbageCollectorConfig().also(configure))

        fun readOnlyDatabases(readOnlyDatabases: Boolean = true) = apply { this.readOnlyDatabases = readOnlyDatabases }

        fun skipDbs(skipDbs: Set<String>) = apply { this.skipDbs = skipDbs }

        fun defaultTz(defaultTz: ZoneId) = apply { this.defaultTz = defaultTz }

        fun authn(authn: Authenticator.Factory) = apply { this.authn = authn }

        fun nodeId(nodeId: String) = apply { this.nodeId = nodeId }

        fun getModules(): List<XtdbModule.Factory> = modules
        fun module(module: XtdbModule.Factory) = apply { this.modules += module }
        fun modules(vararg modules: XtdbModule.Factory) = apply { this.modules += modules }
        fun modules(modules: List<XtdbModule.Factory>) = apply { this.modules += modules }

        fun open(): Xtdb = requiringResolve("xtdb.node.impl/open-node").invoke(this) as Xtdb

        fun openCompactor() = requiringResolve("xtdb.node.impl/open-compactor").invoke(this) as CompactorNode
    }

    companion object {
        @JvmStatic
        fun readConfig(path: Path): Config {
            if (path.extension != "yaml") {
                throw IllegalArgumentException("Invalid config file type - must be '.yaml'")
            } else if (!path.toFile().exists()) {
                throw IllegalArgumentException("Provided config file does not exist")
            }

            val yamlString = Files.readString(path)
            return nodeConfig(yamlString)
        }

        @JvmStatic
        @JvmOverloads
        fun openNode(config: Config = Config()) = config.open()

        /**
         * Opens a node using a YAML configuration file - will throw an exception if the specified path does not exist
         * or is not a valid `.yaml` extension file.
         */
        @JvmStatic
        fun openNode(path: Path): Xtdb = readConfig(path).open()

        @JvmSynthetic
        fun openNode(configure: Config.() -> Unit) = openNode(Config().also(configure))
    }
}

inline fun <reified T : XtdbModule> Xtdb.module(): T? = module(T::class.java)
