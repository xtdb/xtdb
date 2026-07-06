@file:UseSerializers(ZoneIdSerde::class)

package xtdb.api

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import io.micrometer.tracing.Tracer
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
import xtdb.ICursor
import xtdb.ResultCursor
import xtdb.ZoneIdSerde
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
import xtdb.util.closeAll
import xtdb.util.closeAllOnCatch
import xtdb.util.closeOnCatch
import xtdb.util.requiringResolve
import xtdb.util.useAll
import java.nio.file.Files
import java.nio.file.Path
import java.time.*
import java.util.Date
import java.util.SequencedMap
import java.util.function.Consumer
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
        // mutable so a frontend can seed it (pgwire pushes the server clock at startup); current-time only,
        // and only really exercised by tests pinning a fixed instant.
        var clock: Clock,
        // the query tracer, gated by the node's query-tracing config (null = off); threaded into every read's
        // QueryOpts so the connection — not each frontend — owns query tracing.
        private val tracer: Tracer?,
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

        // the await-token a frontend should observe: an open tx's bound (from BEGIN ... WITH (AWAIT_TOKEN …))
        // shadows the connection's own, for SHOW AWAIT_TOKEN / LATEST_SUBMITTED_TX inside that tx.
        val effectiveAwaitToken: String? get() = tx?.awaitToken ?: awaitToken

        // the authenticated user, threaded into every write's [TxOpts] for the audit trail. Established by the
        // frontend at connect-time (pgwire's startup handshake); null until a frontend sets it.
        var user: String? = null

        // how long to wait for this connection's writes to become visible before giving up, shared by every
        // read this connection prepares or snapshots
        private val awaitTimeout: Duration = Duration.ofMinutes(1)

        private var autoCommit = true
        private var tx: Transaction? = null

        // session parameters (SET <param> = <value>) — stored raw; param-specific interpretation (e.g.
        // pgwire's fallback_output_format) stays with the consumer. SHOW <param> reads back from here.
        private val _sessionParameters = mutableMapOf<String, String?>()

        // read view for the frontend (pgwire's serialization env / ParameterStatus echo); writes go through SET.
        val sessionParameters: Map<String, String?> get() = _sessionParameters

        // the sanctioned external writer, for a frontend's SET (pgwire funnels its startup defaults + SET here).
        fun setSessionParameter(name: String, value: String?) { _sessionParameters[name] = value }

        // the default access mode for a subsequently-opened bare BEGIN, set by SET SESSION CHARACTERISTICS;
        // null leaves a bare BEGIN unresolved (resolved by its first statement). Readable for the frontend.
        var defaultAccessMode: ParsedStatement.AccessMode? = null
            private set

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
                parseStatement(sql),
                dbCat,
                PrepareOpts(defaultTz = defaultTz, defaultDb = defaultDb)
            )
        }

        // pgwire (and ADBC) parse once for dispatch, so they prepare from the classified statement; its query text
        // and EXPLAIN flag derive from the AST. tz, db, await-token and await bound come from the connection.
        fun prepareSql(stmt: ParsedStatement): PreparedQuery {
            dbCat.awaitAll(awaitToken, awaitTimeout)
            return qSrc.prepareQuery(
                stmt,
                dbCat,
                PrepareOpts(defaultTz = defaultTz, defaultDb = dbName)
            )
        }

        fun openSqlQuery(sql: String): ResultCursor =
            openQuery(prepareSql(sql), null)

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
                // SHOW is answered from connection state, ahead of the access-mode gate — it is session
                // introspection, valid in any transaction (pgwire's verify-permissibility doesn't gate it).
                if (prepared == null)
                    (parseStatements(sql).singleOrNull() as? ParsedStatement.ShowVariable)
                        ?.let { return showVariable(it.variable) }
                if (prepared == null && args != null)
                    throw Incorrect(
                        "call prepare() before executeQuery() when parameters are bound",
                        "xtdb.adbc/bind-without-prepare",
                    )

                val queryArgs = openQueryArgs()
                val cursor = try {
                    prepared?.let { openQuery(it, queryArgs) } ?: openQuery(prepareSql(sql), null)
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
                    is ParsedStatement.Begin -> beginParsedTx(stmt.txOptions)

                    is ParsedStatement.Commit -> when (stmt.mode) {
                        ParsedStatement.CommitMode.SYNC -> commitSync()
                        ParsedStatement.CommitMode.ASYNC -> commitAsync()
                        null -> if (isTxAsync) commitAsync() else commitSync()
                    }
                    is ParsedStatement.Rollback -> rollbackTx()

                    is ParsedStatement.SetSessionParameter ->
                        setSessionParameter(stmt.name, sqlPlanner.evalLiteral(stmt.value, null)?.toString())
                    is ParsedStatement.SetTimeZone -> setTimeZone(coerceZoneId(sqlPlanner.evalLiteral(stmt.zone, null)))
                    is ParsedStatement.SetAwaitToken -> _awaitToken = coerceAwaitToken(sqlPlanner.evalLiteral(stmt.token, null))
                    is ParsedStatement.SetSessionCharacteristics -> defaultAccessMode = stmt.accessMode
                    is ParsedStatement.SetTransaction -> {} // isolation is always serializable — accepted, no-op
                    is ParsedStatement.SetRole -> {} // accepted, no-op (as pgwire)

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

            val isEmpty get() = ops.isEmpty()

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

        // [txDefaultTz] anchors this tx's DML and reads; [sessionDefaultTz] is copied back to the connection on
        // COMMIT. SET TIME ZONE writes both (a session operator); `WITH (TIMEZONE)` writes only [txDefaultTz], so
        // the override stays tx-scoped. [awaitToken] is a tx-scoped BEGIN ... WITH (AWAIT_TOKEN) override.
        private class Transaction(
            var mode: AccessMode? = null, val readBasis: ReadBasis? = null,
            var txDefaultTz: ZoneId, var sessionDefaultTz: ZoneId, val awaitToken: String? = null
        ) : AutoCloseable {
            var failed: Throwable? = null

            override fun close() {
                mode?.close()
            }
        }

        // Pin the begin-time read basis: await this connection's own writes, then snapshot the data
        // (latest-completed system-times) and the clock. A READ ONLY WITH clause overrides any of the three.
        private fun pinReadBasis(
            snapshotToken: String? = null, snapshotTime: Instant? = null, currentTime: Instant? = null,
            awaitToken: String? = this.awaitToken
        ): ReadBasis {
            dbCat.awaitAll(awaitToken, awaitTimeout)
            return ReadBasis(snapshotToken ?: dbCat.snapshotToken(), snapshotTime, currentTime ?: clock.instant())
        }

        // Open an explicit transaction (SQL BEGIN). [accessMode] fixes the mode — READ ONLY rejects writes,
        // READ WRITE buffers them; a bare BEGIN leaves it unresolved until the first statement resolves it.
        // [readBasis] pins the begin-time read snapshot (null for an explicit READ WRITE — writes don't read).
        // Coexists with autoCommit — an open tx captures subsequent DML (see executeDml) until COMMIT/ROLLBACK.
        private fun beginTx(accessMode: AccessMode?, readBasis: ReadBasis?, tz: ZoneId = defaultTz, awaitToken: String? = null) {
            if (tx != null) throw Incorrect("transaction already started", "xtdb/tx-already-open")
            tx = Transaction(accessMode, readBasis, txDefaultTz = tz, sessionDefaultTz = defaultTz, awaitToken = awaitToken)
        }

        // a bare BEGIN takes the session default access mode (SET SESSION CHARACTERISTICS); with none set it
        // stays unresolved, pinning a basis anyway — a query resolving it to read-only inherits the basis, DML
        // resolving it to read-write ignores it.
        // @JvmOverloads: a frontend (pgwire) calls these reflectively with the shorter arities, taking the
        // default session tz — the defaulted `tz` param alone wouldn't emit those JVM overloads.
        @JvmOverloads
        fun beginTx(tz: ZoneId = defaultTz) = when (defaultAccessMode) {
            ParsedStatement.AccessMode.READ_ONLY -> beginReadOnly(tz)
            ParsedStatement.AccessMode.READ_WRITE -> beginWriteOnly(tz = tz)
            null -> beginTx(null, pinReadBasis(), tz)
        }

        // the default access mode a bare BEGIN (explicit or implicit) takes; set by SET SESSION CHARACTERISTICS.
        fun setSessionCharacteristics(accessMode: ParsedStatement.AccessMode?) {
            defaultAccessMode = accessMode
        }

        @JvmOverloads
        fun beginReadOnly(tz: ZoneId = defaultTz) = beginTx(ReadOnly, pinReadBasis(), tz)

        @JvmOverloads
        fun beginWriteOnly(systemTime: Instant? = null, userMetadata: Map<*, *>? = null, async: Boolean = false,
                           tz: ZoneId = defaultTz) =
            beginTx(ReadWrite(systemTime, userMetadata, async), null, tz)

        // tx status for the frontend's ready-state + commit/abort decisions. A failed tx rejects further work
        // until ROLLBACK (Postgres' "current transaction is aborted").
        val isTxOpen: Boolean get() = tx != null
        val isTxFailed: Boolean get() = tx?.failed != null
        // resolved access-mode of the open tx (an unresolved bare tx is neither); the frontend's permissibility
        // checks read these to keep their own error messages + special-cases (e.g. pgwire's pgjdbc type query).
        val isTxReadWrite: Boolean get() = tx?.mode is ReadWrite
        val isTxReadOnly: Boolean get() = tx?.mode is ReadOnly
        // the begin-time async flag (WITH (ASYNC)) of the open write tx; false when unset / not a write tx.
        // A bare COMMIT (no SYNC/ASYNC) and the programmatic commit() commit through it.
        val isTxAsync: Boolean get() = (tx?.mode as? ReadWrite)?.async ?: false

        // mark the open tx failed (first error wins), driven by the frontend when a statement throws mid-tx.
        fun failTx(cause: Throwable) {
            tx?.let { it.failed = it.failed ?: cause }
        }

        // Coerce a planned BEGIN-option value to an Instant, mirroring xtdb.time/->instant so the connection and
        // pgwire agree on the system-time a SQL temporal literal resolves to. SqlPlanner plans `TIMESTAMP '…'`
        // to a java.time temporal (ZonedDateTime when zoned, else LocalDateTime); a bare string falls back to
        // the SQL-timestamp parse. LocalDate/LocalDateTime are anchored in [tz] (the tx's zone), as pgwire does.
        private fun coerceInstant(value: Any?, tz: ZoneId): Instant? = when (value) {
            null -> null
            is Instant -> value
            is ZonedDateTime -> value.toInstant()
            is OffsetDateTime -> value.toInstant()
            is LocalDateTime -> value.atZone(tz).toInstant()
            is LocalDate -> value.atStartOfDay(tz).toInstant()
            is Date -> value.toInstant()
            is String -> value.asInstant(tz)
            else -> throw Incorrect(
                "cannot coerce SYSTEM_TIME option to an instant",
                "xtdb.adbc/invalid-system-time",
                mapOf("value" to value, "type" to value::class.java.name)
            )
        }

        private fun coerceZoneId(value: Any?): ZoneId = when (value) {
            is ZoneId -> value
            is String -> ZoneId.of(value)
            else -> throw Incorrect(
                "cannot coerce TIME ZONE to a zone",
                "xtdb.adbc/invalid-time-zone",
                mapOf("value" to value)
            )
        }

        private fun coerceAwaitToken(value: Any?): String? = when (value) {
            null -> null
            is String -> value
            else -> throw Incorrect(
                "AWAIT_TOKEN must be a string",
                "xtdb.adbc/invalid-await-token",
                mapOf("value" to value)
            )
        }

        // SET TIME ZONE — a session operator. Mid-tx it writes both the tx zone and the session zone it commits to,
        // so it persists on COMMIT and reverts on ROLLBACK (which just drops the tx). Rejected once a write tx has
        // buffered ops: those ops captured temporal literals under the old zone, so changing it mid-buffer would be
        // inconsistent.
        fun setTimeZone(zone: ZoneId) {
            (tx?.mode as? ReadWrite)?.let {
                if (!it.buffer.isEmpty) throw Incorrect(
                    "Cannot SET TIME ZONE in a read-write transaction with buffered writes",
                    "xtdb/set-tz-in-write-tx"
                )
            }
            tx?.let { it.txDefaultTz = zone; it.sessionDefaultTz = zone } ?: run { defaultTz = zone }
        }

        // Begin an explicit tx from parsed WITH options, evaluating each option expression via the injected
        // SqlPlanner with the supplied bound args (null for the ADBC path, whose BEGIN options are literals;
        // pgwire passes its portal args, since a BEGIN option can be a parameter placeholder). Public for the
        // frontends. WITH (TIMEZONE) overrides the tx zone (tx-scoped); WITH (AWAIT_TOKEN) sets a READ ONLY tx's
        // await bound.
        fun beginParsedTx(opts: ParsedStatement.TxOptions, args: List<*>? = null) {
            val tz = opts.defaultTz?.let { coerceZoneId(sqlPlanner.evalLiteral(it, args)) } ?: defaultTz

            when (opts.accessMode) {
                ParsedStatement.AccessMode.READ_WRITE -> {
                    val systemTime = coerceInstant(opts.systemTime?.let { sqlPlanner.evalLiteral(it, args) }, tz)
                    val userMetadata = opts.userMetadata?.let { sqlPlanner.evalLiteral(it, args) } as Map<*, *>?
                    val async = (opts.async?.let { sqlPlanner.evalLiteral(it, args) } as Boolean?) ?: false

                    beginWriteOnly(systemTime, userMetadata, async, tz)
                }

                ParsedStatement.AccessMode.READ_ONLY -> {
                    // SNAPSHOT_TOKEN / SNAPSHOT_TIME / CLOCK_TIME override the begin-time auto-pin; AWAIT_TOKEN
                    // sets the await bound for this tx's snapshot, defaulting to the connection's own token.
                    val awaitTok = opts.awaitToken?.let { coerceAwaitToken(sqlPlanner.evalLiteral(it, args)) } ?: awaitToken
                    val snapshotToken = opts.snapshotToken?.let { sqlPlanner.evalLiteral(it, args) } as String?
                    val snapshotTime = coerceInstant(opts.snapshotTime?.let { sqlPlanner.evalLiteral(it, args) }, tz)
                    val currentTime = coerceInstant(opts.clockTime?.let { sqlPlanner.evalLiteral(it, args) }, tz)

                    beginTx(ReadOnly, pinReadBasis(snapshotToken, snapshotTime, currentTime, awaitTok), tz, awaitTok)
                }

                null -> beginTx(tz)
            }
        }

        // Drain the open write tx into its expanded ops + submit opts, clearing the tx and persisting a mid-tx
        // SET TIME ZONE. null when there's nothing to submit — no tx, or a ReadOnly / still-unresolved one (a
        // no-op commit). The caller owns the returned ops and MUST close them (useAll). A ReadWrite tx drains
        // even when its buffer is empty — an explicit write tx still records a transaction (advancing the token).
        private fun drainWriteTx(): Pair<List<TxOp>, TxOpts>? {
            val tx = tx ?: return null
            this.tx = null
            defaultTz = tx.sessionDefaultTz   // a mid-tx SET TIME ZONE persists; a WITH (TIMEZONE) override doesn't
            val mode = tx.mode as? ReadWrite ?: return null
            val opts = TxOpts(systemTime = mode.systemTime, user = user, userMetadata = mode.userMetadata, defaultTz = tx.txDefaultTz)
            return expandStaticOps(mode.buffer.drain(), tx.txDefaultTz) to opts
        }

        // Commit the open write tx asynchronously — submit without awaiting, so the result carries only the txId.
        // Records the await-token and the (detail-less) last-submitted-tx. null when there's nothing to commit.
        fun commitAsync(): SubmittedTx? =
            drainWriteTx()?.let { (ops, opts) -> ops.useAll { submitTx(it, opts) } }

        // Commit the open write tx synchronously — submit and await the outcome, recording its full detail
        // (committed flag, system-time, error) as the last-submitted-tx. null when there's nothing to commit.
        fun commitSync(): ExecutedTx? =
            drainWriteTx()?.let { (ops, opts) -> ops.useAll { executeTx(it, opts) } }

        private fun expandStaticOps(ops: List<TxOp>, tz: ZoneId): List<TxOp> =
            mutableListOf<TxOp>().closeAllOnCatch { out ->
                ops.forEachIndexed { idx, op ->
                    val expanded = try {
                        (op as? TxOp.Sql)?.let { sqlPlanner.toStaticOps(it.sql, it.args, allocator, tz) }
                    } catch (t: Throwable) {
                        ops.subList(idx, ops.size).closeAll()
                        throw t
                    }

                    if (expanded != null) {
                        op.close()
                        out.addAll(expanded)
                    } else out.add(op)
                }

                out
            }

        fun executeDml(op: TxOp) {
            // auto-execute only when no explicit tx is open; an open tx (from beginTx) buffers even under autoCommit.
            if (autoCommit && tx == null) {
                expandStaticOps(listOf(op), defaultTz).useAll { ops -> executeTx(ops, TxOpts(user = user)) }
                return
            }

            val tx = tx ?: Transaction(txDefaultTz = defaultTz, sessionDefaultTz = defaultTz).also { tx = it }

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
        // read-only, and is rejected in a read-write (DML) tx — in XTDB a write tx is write-only. Applied by
        // openQuery, so every read gates through one place.
        private fun resolveForQuery() {
            val tx = tx
            if (tx == null) {
                // manual mode mirrors executeDml's lazy open: a query opens a read-only tx pinning the
                // begin-time basis, so subsequent reads share its snapshot. Under autocommit there is no open
                // tx — the query reads latest at the connection's current basis, unchanged.
                if (!autoCommit) this.tx = Transaction(ReadOnly, pinReadBasis(), txDefaultTz = defaultTz, sessionDefaultTz = defaultTz)
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

        // QueryOpts for a user read: an open tx's pinned basis (snapshot isolation), else latest data at the
        // connection's clock (autocommit / no open tx) — current-time is the connection's clock, never the
        // planner's own, so a frontend-pinned clock is authoritative.
        private fun queryOpts() = tx.let { t ->
            val b = t?.readBasis
            QueryOpts(b?.currentTime ?: clock.instant(), t?.txDefaultTz ?: defaultTz, b?.snapshotToken, b?.snapshotTime, tracer)
        }

        // Open a cursor on an already-prepared query: first resolves the tx's access mode and basis (rejecting
        // a read in a write tx, resolving an unresolved tx to read-only), then opens at the connection's basis,
        // tz and tracer. A frontend owns the cursor for wire serialization, but the gate and the QueryOpts stay
        // the connection's — no callsite reconstructs them, nor decides the gate.
        fun openQuery(pq: PreparedQuery, args: RelationReader?): ResultCursor {
            resolveForQuery()
            return pq.openQuery(args, queryOpts())
        }

        // Open a read WITHOUT the access-mode gate — for a frontend's driver-compatibility probe that must be
        // permitted in any transaction (pgwire allows the pgjdbc type-metadata query inside a write tx, where
        // the gate would otherwise reject it). Reads at the connection's current basis; inside a write tx that
        // is latest-committed data, not the tx's buffered writes.
        fun openUncheckedQuery(pq: PreparedQuery, args: RelationReader?): ResultCursor = pq.openQuery(args, queryOpts())

        // Answer a SHOW from connection state. SHOW LATEST_SUBMITTED_TX reports this connection's own last write;
        // SHOW AWAIT_TOKEN reads the connection's await-token; SHOW of any other identifier reads a session
        // parameter. (SHOW TIME ZONE / SNAPSHOT_TOKEN / CLOCK_TIME are grammar `showVariable`s — classified as
        // Query and answered by the engine from the basis, not here.) The node-level status SHOWs
        // (latest_completed_txs, …) are node introspection, not connection state — they stay in pgwire for now.
        private fun showVariable(variable: String): QueryResult =
            when (variable) {
                // 0 rows until this connection has submitted a tx (matches the `tx_id IS NOT NULL` filter);
                // system-time/committed/error are null for a fire-and-forget submit. error is a Throwable → transit.
                "latest_submitted_tx" -> showResult(
                    linkedMapOf("tx_id" to VectorType.maybe(VectorType.I64),
                                "system_time" to VectorType.maybe(VectorType.INSTANT),
                                "committed" to VectorType.maybe(VectorType.BOOL),
                                "error" to VectorType.maybe(VectorType.TRANSIT),
                                "await_token" to VectorType.maybe(VectorType.UTF8)),
                    lastSubmittedTx?.let {
                        listOf(mapOf("tx_id" to it.txId, "system_time" to it.systemTime,
                                     "committed" to it.committed, "error" to it.error,
                                     "await_token" to effectiveAwaitToken))
                    } ?: emptyList())

                "await_token" -> showResult(variable, effectiveAwaitToken)
                else -> showResult(variable, sessionParameters[variable])
            }

        // A one-row, single-(nullable-utf8)-column result — the common SHOW shape.
        private fun showResult(col: String, value: String?): QueryResult =
            showResult(linkedMapOf(col to VectorType.maybe(VectorType.UTF8)), listOf(mapOf(col to value)))

        // A SHOW result of arbitrary typed columns and 0-or-more rows — the SHOW counterpart of the query
        // engine's cursor, so executeQuery can return connection state as an Arrow result set.
        private fun showResult(types: SequencedMap<String, VectorType>, rows: List<Map<String, Any?>>): QueryResult {
            val rel = Relation(allocator, types).closeOnCatch { r -> rows.forEach { r.writeRow(it) }; r }
            val cursor = object : ResultCursor {
                override val resultTypes get() = types
                override val cursorType get() = "show"
                override val childCursors get() = emptyList<ICursor>()

                private var done = false
                override fun tryAdvance(c: Consumer<in RelationReader>): Boolean {
                    if (done) return false
                    done = true
                    c.accept(rel)
                    return true
                }

                override fun close() = rel.close()
            }
            val schema = Schema(cursor.resultTypes.map { (name, type) -> type.toField(name) })
            return QueryResult(-1, cursorToArrowReader(cursor, schema))
        }

        override fun setAutoCommit(autoCommit: Boolean) {
            if (this.autoCommit == autoCommit) return
            if (autoCommit) {
                if (isTxAsync) commitAsync() else commitSync()
            }
            this.autoCommit = autoCommit
        }

        override fun commit() {
            if (autoCommit)
                throw Incorrect("Cannot commit when autoCommit is enabled", "xtdb.adbc/commit-in-autocommit")

            if (isTxAsync) commitAsync() else commitSync()
        }

        // SQL ROLLBACK: discards the open tx regardless of autoCommit (unlike [rollback], which rejects rollback in
        // autocommit). The explicit-tx mechanism the SQL tx-control path (and the pgwire frontend) drives.
        fun rollbackTx() {
            // defaultTz was never touched while the tx was open, so dropping the tx reverts any mid-tx SET TIME ZONE
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
