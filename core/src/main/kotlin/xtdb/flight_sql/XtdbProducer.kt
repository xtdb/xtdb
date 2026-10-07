package xtdb.flight_sql

import xtdb.InternalApi
import xtdb.encodeToBytes
import xtdb.query.ParsedStatement
import clojure.lang.Keyword
import com.google.protobuf.Any as ProtoAny
import com.google.protobuf.ByteString
import com.google.protobuf.Message
import org.apache.arrow.flight.*
import org.apache.arrow.flight.FlightProducer.*
import org.apache.arrow.flight.sql.FlightSqlProducer
import org.apache.arrow.flight.sql.NoOpFlightSqlProducer
import org.apache.arrow.flight.sql.impl.FlightSql.*
import org.apache.arrow.flight.sql.impl.FlightSql.ActionEndTransactionRequest.EndTransaction
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementIngest.TableDefinitionOptions.TableExistsOption
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementIngest.TableDefinitionOptions.TableNotExistOption
import xtdb.database.DatabaseName
import org.apache.arrow.adbc.core.AdbcConnection.GetObjectsDepth
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.UInt4Vector
import org.apache.arrow.vector.VarBinaryVector
import org.apache.arrow.vector.VarCharVector
import org.apache.arrow.vector.VectorLoader
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.VectorUnloader
import org.apache.arrow.vector.complex.DenseUnionVector
import org.apache.arrow.vector.holders.NullableIntHolder
import org.apache.arrow.vector.holders.NullableVarCharHolder
import org.apache.arrow.vector.ipc.ArrowReader
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.adbc.core.BulkIngestMode
import xtdb.api.Xtdb
import xtdb.api.error.*
import xtdb.api.error.Anomaly.Companion.toAnomaly
import xtdb.arrow.Relation
import xtdb.asBytes
import xtdb.util.closeAll
import xtdb.util.closeOnCatch
import xtdb.util.logger
import xtdb.util.serializeAsMessageInterruptibly
import xtdb.util.warn
import xtdb.util.XtdbVersion
import java.util.*
import java.util.concurrent.Callable
import java.util.concurrent.ConcurrentHashMap

private typealias TxHandle = ByteString
private typealias PreparedStatementHandle = ByteString

private val LOGGER = XtdbProducer::class.logger

private fun newHandle(): TxHandle = ByteString.copyFrom(UUID.randomUUID().asBytes)

private fun packResult(res: Message) = Result(ProtoAny.pack(res).toByteArray())

private val DO_PUT_UPDATE_MSG =
    DoPutUpdateResult.newBuilder()
        .setRecordCount(-1)
        .build()
        .toByteArray()

private fun StreamListener<PutResult>.sendDoPutUpdateRes(allocator: BufferAllocator) {
    PutResult.metadata(
        allocator
            .buffer(DO_PUT_UPDATE_MSG.size.toLong())
            .apply { writeBytes(DO_PUT_UPDATE_MSG) }
    ).use { res ->
        onNext(res)
    }

    onCompleted()
}

/**
 * Whether this statement is DML, as the connection classified it when the SQL was set.
 *
 * Asked of the statement rather than re-parsed here, so the producer's routing decisions and the
 * connection's dispatch can't disagree about what a statement is.
 */
@OptIn(InternalApi::class)
private val Xtdb.Statement.isDml get() = parsedStatement is ParsedStatement.Dml

/**
 * Translate a throwable into a [FlightRuntimeException] carrying the XTDB anomaly's
 * message and a status code mapped from its category, so the client sees the real
 * error rather than gRPC's generic "error servicing your request".
 *
 * A throwable we've already shaped into a Flight status (e.g. the DML-via-query
 * guard) passes through untouched. Mirrors the pgwire `ex->pgw-err` mapping.
 */
private val ERROR_CODE: Keyword = Keyword.intern("xtdb.error", "code")
private val NOT_A_QUERY: Keyword = Keyword.intern("xtdb", "not-a-query")

private fun Anomaly.flightDescription(): String {
    val msg = message ?: toString()
    return if (data.valAt(ERROR_CODE) == NOT_A_QUERY) "$msg - see https://github.com/xtdb/xtdb/issues/5861" else msg
}

private fun Throwable.asFlightException(): FlightRuntimeException =
    this as? FlightRuntimeException ?: toAnomaly().let { anom ->
        val status = when (anom) {
            is Incorrect, is Conflict -> CallStatus.INVALID_ARGUMENT
            is Unsupported -> CallStatus.UNIMPLEMENTED
            is Forbidden -> CallStatus.UNAUTHORIZED
            is NotFound -> CallStatus.NOT_FOUND
            is Interrupted -> CallStatus.CANCELLED
            is Busy -> CallStatus.RESOURCE_EXHAUSTED
            is Unavailable -> CallStatus.UNAVAILABLE
            is Fault -> CallStatus.INTERNAL
        }
        status.withDescription(anom.flightDescription()).withCause(anom).toRuntimeException()
    }

private fun Xtdb.Connection.LastSubmittedTx.toCommitResult(awaitToken: String?) = Result(
    encodeToBytes(
        mapOf(
            "txId" to txId, "systemTime" to systemTime, "committed" to committed, "error" to error,
            "awaitToken" to awaitToken
        )
    )
)

/** Run [block], rethrowing any error as a [FlightRuntimeException] (see [asFlightException]). */
private inline fun <R> flightCall(block: () -> R): R =
    try { block() } catch (t: Throwable) { throw t.asFlightException() }

/** Run [block], signalling any error to this listener as a [FlightRuntimeException]. */
private inline fun StreamListener<*>.reportingErrors(block: () -> Unit) =
    try { block() } catch (t: Throwable) { onError(t.asFlightException()) }

/** Run [block], signalling any error to this stream as a [FlightRuntimeException]. */
private inline fun ServerStreamListener.reportingErrors(block: () -> Unit) =
    try { block() } catch (t: Throwable) { error(t.asFlightException()) }

private fun FlightStream.toRelation(allocator: BufferAllocator): Relation =
    Relation(allocator, root.schema).closeOnCatch { acc ->
        Relation(allocator, root.schema).use { batch ->
            val copier = batch.rowCopier(acc)
            while (next()) {
                batch.loadFromArrow(root)
                copier.copyRange(0, batch.rowCount)
            }
        }
        acc
    }

private class PreparedStatement(val dbName: DatabaseName, val sql: String, val xtdbStmt: Xtdb.Statement) : AutoCloseable {
    @Volatile
    var params: QueryParams? = null

    override fun close() = xtdbStmt.close()
}

private fun Xtdb.Statement.requireQuery() {
    // see #5082 — Python ADBC's cursor.execute() routes DML through the query path
    if (isDml) throw CallStatus.INVALID_ARGUMENT
        .withDescription("DML statements should be submitted via executeUpdate, not executeQuery (in Python ADBC, use cursor.executescript())")
        .toRuntimeException()
}

internal fun FlightServer.Builder.withErrorLoggingMiddleware(): FlightServer.Builder =
    this.middleware(FlightServerMiddleware.Key.of("error-logger")) { info, incomingHeaders, reqCtx ->
        object : FlightServerMiddleware {
            override fun onBeforeSendingHeaders(outgoingHeaders: CallHeaders?) {}
            override fun onCallCompleted(status: CallStatus?) {}
            override fun onCallErrored(e: Throwable) {
                LOGGER.warn(e, "FSQL server error")
            }
        }
    }

class DatabaseMiddleware(val dbName: DatabaseName?) : FlightServerMiddleware {
    override fun onBeforeSendingHeaders(outgoingHeaders: CallHeaders?) {}
    override fun onCallCompleted(status: CallStatus?) {}
    override fun onCallErrored(e: Throwable?) {}

    companion object {
        val KEY: FlightServerMiddleware.Key<DatabaseMiddleware> = FlightServerMiddleware.Key.of("database")
    }
}

internal fun FlightServer.Builder.withDatabaseMiddleware(): FlightServer.Builder =
    this.middleware(DatabaseMiddleware.KEY) { _, incomingHeaders, _ ->
        DatabaseMiddleware(incomingHeaders.get("x-xtdb-database"))
    }

val SESSION_KEY: FlightServerMiddleware.Key<ServerSessionMiddleware> =
    FlightServerMiddleware.Key.of("flight-sql-session")

internal fun FlightServer.Builder.withSessionMiddleware(): FlightServer.Builder =
    this.middleware(SESSION_KEY, ServerSessionMiddleware.Factory(Callable { UUID.randomUUID().toString() }))

private fun SessionOptionValue.asStringOrNull(): String? =
    acceptVisitor(object : NoOpSessionOptionValueVisitor<String?>() {
        override fun visit(value: String) = value
    })

class XtdbProducer(private val node: Xtdb) : NoOpFlightSqlProducer(), AutoCloseable {
    private val allocator = node.allocator.newChildAllocator("flight-sql", 0, Long.MAX_VALUE)

    // key is (session id, db)
    private val sessionConns = ConcurrentHashMap<Pair<String, DatabaseName>, Xtdb.Connection>()

    // fallback for clients without a session cookie
    private val defaultConns = ConcurrentHashMap<DatabaseName, Xtdb.Connection>()

    // a tx owns a dedicated connection: autoCommit/pendingOps are connection-scoped, so flipping
    // them on a pooled connection would buffer other clients' autocommit writes into this tx.
    private data class TxConn(val sessionId: String?, val conn: Xtdb.Connection)
    private val txConns = ConcurrentHashMap<TxHandle, TxConn>()
    private val stmts = ConcurrentHashMap<PreparedStatementHandle, PreparedStatement>()

    private fun newConnection(dbName: DatabaseName): Xtdb.Connection =
        (node.connect()).also { it.setCurrentCatalog(dbName) }

    private fun sessionMiddleware(ctx: CallContext?): ServerSessionMiddleware? =
        ctx?.getMiddleware(SESSION_KEY)

    /**
     * The database for this call: the `x-xtdb-database` header takes precedence (the
     * explicit per-call selector used by the Java/raw clients), falling back to the
     * session's `catalog` option (how the Go-driver-based ADBC clients select a db),
     * and finally the default `xtdb`.
     */
    private fun resolveDb(ctx: CallContext?): DatabaseName {
        ctx?.getMiddleware(DatabaseMiddleware.KEY)?.dbName?.let { return it }

        sessionMiddleware(ctx)
            ?.takeIf { it.hasSession() }
            ?.session?.getSessionOption("catalog")?.asStringOrNull()
            ?.let { return it }

        return "xtdb"
    }

    /**
     * The connection for an autocommit call: per-session when the caller carries a
     * session cookie, otherwise the shared anonymous connection for the database.
     */
    private fun connectionFor(ctx: CallContext?, dbName: DatabaseName): Xtdb.Connection {
        val mw = sessionMiddleware(ctx)
        return if (mw != null && mw.hasSession())
            sessionConns.computeIfAbsent(mw.session.id to dbName) { newConnection(dbName) }
        else
            defaultConns.computeIfAbsent(dbName) { newConnection(dbName) }
    }

    // the session presenting the call, or null for a cookieless caller.
    private fun currentSessionId(ctx: CallContext?): String? =
        sessionMiddleware(ctx)?.takeIf { it.hasSession() }?.session?.id

    // a transaction is owned by the session that opened it: a handle only resolves under
    // its own session (cookieless == cookieless). A handle presented under any other session
    // is NOT_FOUND - from that session's view the transaction doesn't exist.
    private fun txConnFor(ctx: CallContext?, txHandle: TxHandle): TxConn =
        txConns[txHandle]
            ?.takeIf { it.sessionId == currentSessionId(ctx) }
            ?: throw CallStatus.NOT_FOUND.withDescription("unknown transaction").toRuntimeException()

    private fun txOrSessionConnection(ctx: CallContext?, txHandle: TxHandle?): Xtdb.Connection =
        if (txHandle != null) txConnFor(ctx, txHandle).conn
        else connectionFor(ctx, resolveDb(ctx))

    override fun close() {
        stmts.closeAll()
        txConns.values.forEach { it.conn.close() }
        txConns.clear()
        sessionConns.values.forEach { it.close() }
        sessionConns.clear()
        defaultConns.values.forEach { it.close() }
        defaultConns.clear()
        allocator.close()
    }

    /**
     * Run a non-query statement on the connection's own statement, so it is classified and dispatched
     * there (DML, transaction control, the session `SET` surface) rather than submitted as a raw SQL
     * op — which only DML is.
     */
    private fun execUpdate(sql: String, ctx: CallContext?, txHandle: TxHandle?) {
        txOrSessionConnection(ctx, txHandle).createStatement().use { stmt ->
            stmt.setSqlQuery(sql)
            stmt.executeUpdate()
        }
    }

    override fun acceptPutStatement(
        cmd: CommandStatementUpdate,
        ctx: CallContext?,
        flightStream: FlightStream?,
        ackStream: StreamListener<PutResult>
    ): Runnable = Runnable {
        ackStream.reportingErrors {
            execUpdate(
                cmd.query,
                ctx,
                if (cmd.hasTransactionId()) cmd.transactionId else null
            )

            ackStream.sendDoPutUpdateRes(allocator)
        }
    }

    override fun acceptPutStatementBulkIngest(
        cmd: CommandStatementIngest,
        ctx: CallContext?,
        flightStream: FlightStream,
        ackStream: StreamListener<PutResult>
    ): Runnable = Runnable {
        ackStream.reportingErrors {
            if (cmd.hasTableDefinitionOptions()) {
                val tdo = cmd.tableDefinitionOptions
                val ifExists = tdo.ifExists
                if (ifExists != TableExistsOption.TABLE_EXISTS_OPTION_UNSPECIFIED
                    && ifExists != TableExistsOption.TABLE_EXISTS_OPTION_APPEND
                ) throw CallStatus.INVALID_ARGUMENT
                    .withDescription("Bulk ingest only supports append-on-exists for now (got $ifExists)")
                    .toRuntimeException()
                val ifNotExist = tdo.ifNotExist
                if (ifNotExist != TableNotExistOption.TABLE_NOT_EXIST_OPTION_UNSPECIFIED
                    && ifNotExist != TableNotExistOption.TABLE_NOT_EXIST_OPTION_CREATE
                ) throw CallStatus.INVALID_ARGUMENT
                    .withDescription("Bulk ingest cannot honour fail-if-not-exist: XTDB auto-creates tables on insert (got $ifNotExist)")
                    .toRuntimeException()
            }

            val dbName = resolveDb(ctx)

            if (cmd.hasCatalog() && cmd.catalog.isNotEmpty() && cmd.catalog != dbName)
                throw CallStatus.INVALID_ARGUMENT
                    .withDescription("Bulk ingest catalog must match the connection catalog (got '${cmd.catalog}', connection '$dbName')")
                    .toRuntimeException()

            val tableName = cmd.table
            if ('.' in tableName) throw CallStatus.INVALID_ARGUMENT
                .withDescription("Bulk ingest table name must not contain '.'; use the schema field for schema-qualified targets (got '$tableName')")
                .toRuntimeException()

            val dbSchemaName = if (cmd.hasSchema()) cmd.schema else "public"
            val txHandle = if (cmd.hasTransactionId()) cmd.transactionId else null

            txOrSessionConnection(ctx, txHandle)
                .bulkIngest("$dbSchemaName.$tableName", BulkIngestMode.CREATE_APPEND)
                .use { stmt ->
                    while (flightStream.next()) {
                        stmt.bind(flightStream.root)
                        stmt.executeUpdate()
                    }
                }

            ackStream.sendDoPutUpdateRes(allocator)
        }
    }

    override fun acceptPutPreparedStatementQuery(
        cmd: CommandPreparedStatementQuery,
        ctx: CallContext?,
        flightStream: FlightStream,
        ackStream: StreamListener<PutResult>
    ): Runnable = Runnable {
        ackStream.reportingErrors {
            val ps = requireNotNull(stmts[cmd.preparedStatementHandle]) { "invalid ps-id" }
            flightStream.next()
            ps.params = QueryParams.of(flightStream.root)
            ackStream.onCompleted()
        }
    }

    override fun acceptPutPreparedStatementUpdate(
        cmd: CommandPreparedStatementUpdate,
        ctx: CallContext?,
        flightStream: FlightStream,
        ackStream: StreamListener<PutResult>
    ): Runnable = Runnable {
        ackStream.reportingErrors {
            val ps = requireNotNull(stmts[cmd.preparedStatementHandle]) { "invalid ps-id" }
            flightStream.toRelation(node.allocator).use { acc ->
                ps.xtdbStmt.bind(acc)
                ps.xtdbStmt.executeUpdate()
            }
            ackStream.sendDoPutUpdateRes(allocator)
        }
    }

    @OptIn(InternalApi::class)
    private fun queryFlightInfo(
        stmt: Xtdb.Statement, dbName: DatabaseName, sql: String, params: QueryParams?, descriptor: FlightDescriptor
    ): FlightInfo {
        val ticket = QueryTicket(dbName, sql, params, stmt.queryBasis())

        val schema =
            if (params == null) stmt.executeSchema()
            else stmt.executeSchema(params.read(allocator) { root ->
                root.schema.fields.mapIndexed { idx, f -> Field("?_$idx", f.fieldType, f.children) }
            })

        val flightTicket = Ticket(
            ProtoAny.pack(TicketStatementQuery.newBuilder().setStatementHandle(ticket.encode()).build()).toByteArray()
        )

        return FlightInfo(schema, descriptor, listOf(FlightEndpoint(flightTicket)), /* bytes = */ -1, /* records = */ -1)
    }

    override fun getFlightInfoStatement(
        cmd: CommandStatementQuery,
        ctx: CallContext?,
        descriptor: FlightDescriptor
    ): FlightInfo = flightCall {
        val sql = cmd.queryBytes.toStringUtf8()
        val conn = txOrSessionConnection(ctx, if (cmd.hasTransactionId()) cmd.transactionId else null)

        conn.createStatement().use { stmt ->
            stmt.setSqlQuery(sql)
            stmt.requireQuery()
            queryFlightInfo(stmt, conn.dbName, sql, null, descriptor)
        }
    }

    @OptIn(InternalApi::class)
    override fun getStreamStatement(
        ticket: TicketStatementQuery, ctx: CallContext?, listener: ServerStreamListener
    ) = listener.reportingErrors {
        val t = QueryTicket.decode(ticket.statementHandle)

        connectionFor(ctx, t.dbName).createStatement().use { stmt ->
            stmt.setSqlQuery(t.sql)
            stmt.requireQuery()
            stmt.prepare()
            t.params?.read(allocator) { stmt.bind(it) }
            streamArrowReader(stmt.executeQueryAt(t.basis).reader, listener)
        }
    }

    override fun getFlightInfoPreparedStatement(
        cmd: CommandPreparedStatementQuery, ctx: CallContext?, descriptor: FlightDescriptor
    ): FlightInfo = flightCall {
        val ps = requireNotNull(stmts[cmd.preparedStatementHandle]) { "invalid ps-id" }
        queryFlightInfo(ps.xtdbStmt, ps.dbName, ps.sql, ps.params, descriptor)
    }

    override fun createPreparedStatement(
        req: ActionCreatePreparedStatementRequest,
        ctx: CallContext?,
        listener: StreamListener<Result>
    ) = listener.reportingErrors {
        val psId = newHandle()
        val sql = req.queryBytes.toStringUtf8()
        val txHandle = if (req.hasTransactionId()) req.transactionId else null
        val conn = txOrSessionConnection(ctx, txHandle)
        conn.createStatement().closeOnCatch { xtdbStmt ->
            xtdbStmt.setSqlQuery(sql)
            xtdbStmt.prepare()

            val resultBuilder = ActionCreatePreparedStatementResult.newBuilder()
                .setPreparedStatementHandle(psId)
                .setParameterSchema(
                    ByteString.copyFrom(xtdbStmt.parameterSchema.serializeAsMessageInterruptibly())
                )

            if (!xtdbStmt.isDml) {
                resultBuilder.setDatasetSchema(
                    ByteString.copyFrom(xtdbStmt.executeSchema().serializeAsMessageInterruptibly())
                )
            }

            stmts[psId] = PreparedStatement(conn.dbName, sql, xtdbStmt)
            listener.onNext(packResult(resultBuilder.build()))
            listener.onCompleted()
        }
    }

    override fun getSchemaPreparedStatement(
        cmd: CommandPreparedStatementQuery, ctx: CallContext?, descriptor: FlightDescriptor
    ): SchemaResult = flightCall {
        val ps = requireNotNull(stmts[cmd.preparedStatementHandle]) { "invalid ps-id" }
        SchemaResult(ps.xtdbStmt.executeSchema())
    }

    override fun getSchemaStatement(
        cmd: CommandStatementQuery, ctx: CallContext?, descriptor: FlightDescriptor
    ): SchemaResult = flightCall {
        val sql = cmd.query
        val dbName = resolveDb(ctx)
        connectionFor(ctx, dbName).createStatement().use { stmt ->
            stmt.setSqlQuery(sql)

            if (stmt.isDml) throw CallStatus.INVALID_ARGUMENT
                .withDescription("executeSchema only supports queries (DML returns a row count, not a schema)")
                .toRuntimeException()

            stmt.prepare()
            SchemaResult(stmt.executeSchema())
        }
    }

    override fun closePreparedStatement(
        req: ActionClosePreparedStatementRequest,
        ctx: CallContext?,
        listener: StreamListener<Result>
    ) {
        stmts.remove(req.preparedStatementHandle)?.close()
        listener.onCompleted()
    }

    // -- Session options --
    // `catalog` selects the db for Go-driver ADBC clients, which can't send the
    // `x-xtdb-database` header. `schema` is not settable (resolved by qualification).

    // must not mint a session: a cookieless client can't reclaim it, so it would leak per call.
    override fun getSessionOptions(
        request: GetSessionOptionsRequest,
        ctx: CallContext?,
        listener: StreamListener<GetSessionOptionsResult>
    ) = listener.reportingErrors {
        val opts = mapOf(
            "catalog" to SessionOptionValueFactory.makeSessionOptionValue(resolveDb(ctx)),
            "schema" to SessionOptionValueFactory.makeSessionOptionValue("public"),
        )
        listener.onNext(GetSessionOptionsResult(opts))
        listener.onCompleted()
    }

    override fun setSessionOptions(
        request: SetSessionOptionsRequest,
        ctx: CallContext?,
        listener: StreamListener<SetSessionOptionsResult>
    ) {
        try {
            // .session mints the session (emits Set-Cookie); a cookieless client can't persist it
            val session = sessionMiddleware(ctx)?.session
                ?: throw CallStatus.INTERNAL
                    .withDescription("FlightSQL session middleware not configured")
                    .toRuntimeException()

            val knownDbs = node.databaseNames
            val errors = mutableMapOf<String, SetSessionOptionsResult.Error>()

            for ((name, value) in request.sessionOptions) {
                when (name) {
                    "catalog" -> {
                        val catalog = value.asStringOrNull()
                        when {
                            catalog == null ->
                                errors[name] = SetSessionOptionsResult.Error(SetSessionOptionsResult.ErrorValue.INVALID_VALUE)

                            catalog.isEmpty() -> session.eraseSessionOption(name)

                            catalog !in knownDbs ->
                                errors[name] = SetSessionOptionsResult.Error(SetSessionOptionsResult.ErrorValue.INVALID_VALUE)

                            else -> session.setSessionOption(name, SessionOptionValueFactory.makeSessionOptionValue(catalog))
                        }
                    }

                    // not settable; tolerate a no-op confirming the fixed `public`
                    "schema" -> {
                        val schema = value.asStringOrNull()
                        if (schema != "public")
                            errors[name] = SetSessionOptionsResult.Error(SetSessionOptionsResult.ErrorValue.INVALID_VALUE)
                    }

                    else ->
                        errors[name] = SetSessionOptionsResult.Error(SetSessionOptionsResult.ErrorValue.INVALID_NAME)
                }
            }

            listener.onNext(SetSessionOptionsResult(errors))
            listener.onCompleted()
        } catch (t: Throwable) {
            listener.onError(t.asFlightException())
        }
    }

    override fun closeSession(
        request: CloseSessionRequest,
        ctx: CallContext?,
        listener: StreamListener<CloseSessionResult>
    ) {
        try {
            val mw = sessionMiddleware(ctx)
            if (mw == null || !mw.hasSession()) {
                listener.onError(
                    CallStatus.NOT_FOUND
                        .withDescription("No session to close")
                        .toRuntimeException()
                )
                return
            }

            val sessionId = mw.session.id
            // remove before close so an in-flight call can't resolve a connection being closed.
            txConns.entries.removeIf { (_, txConn) ->
                if (txConn.sessionId == sessionId) {
                    txConn.conn.close()
                    true
                } else false
            }
            sessionConns.keys.filter { it.first == sessionId }.forEach { key ->
                sessionConns.remove(key)?.close()
            }

            mw.closeSession()

            listener.onNext(CloseSessionResult(CloseSessionResult.Status.CLOSED))
            listener.onCompleted()
        } catch (t: Throwable) {
            listener.onError(t.asFlightException())
        }
    }

    // -- Metadata endpoints --

    private fun metadataFlightInfo(cmd: Message, schema: Schema, descriptor: FlightDescriptor): FlightInfo {
        val ticket = Ticket(ProtoAny.pack(cmd).toByteArray())
        return FlightInfo(schema, descriptor, listOf(FlightEndpoint(ticket)), -1, -1)
    }

    private fun streamArrowReader(reader: ArrowReader, listener: ServerStreamListener) {
        reader.use { rdr ->
            VectorSchemaRoot.create(rdr.vectorSchemaRoot.schema, allocator).use { vsr ->
                val loader = VectorLoader(vsr)
                listener.start(vsr)

                while (rdr.loadNextBatch()) {
                    VectorUnloader(rdr.vectorSchemaRoot).recordBatch.use { rb ->
                        loader.load(rb)
                        listener.putNext()
                    }
                }

                listener.completed()
            }
        }
    }

    override fun getFlightInfoTableTypes(
        request: CommandGetTableTypes, ctx: CallContext?, descriptor: FlightDescriptor
    ): FlightInfo = metadataFlightInfo(request, FlightSqlProducer.Schemas.GET_TABLE_TYPES_SCHEMA, descriptor)

    override fun getStreamTableTypes(ctx: CallContext?, listener: ServerStreamListener) = listener.reportingErrors {
        streamArrowReader(connectionFor(ctx, resolveDb(ctx)).getTableTypes(), listener)
    }

    override fun getFlightInfoSqlInfo(
        request: CommandGetSqlInfo, ctx: CallContext?, descriptor: FlightDescriptor
    ): FlightInfo = metadataFlightInfo(request, FlightSqlProducer.Schemas.GET_SQL_INFO_SCHEMA, descriptor)

    override fun getStreamSqlInfo(
        command: CommandGetSqlInfo, ctx: CallContext?, listener: ServerStreamListener
    ) {
        val requestedCodes = command.infoList.toSet()

        singleBatchStream(FlightSqlProducer.Schemas.GET_SQL_INFO_SCHEMA, listener) { root ->
            val infoNameVec = root.getVector("info_name") as UInt4Vector
            val valueVec = root.getVector("value") as DenseUnionVector

            var idx = 0

            fun addString(code: Int, value: String) {
                if (requestedCodes.isNotEmpty() && code !in requestedCodes) return
                infoNameVec.setSafe(idx, code)
                // setTypeId selects the union leg for this row; setSafe(holder) then appends to that
                // leg's child vector and writes the row's child-local offset. This is the Arrow-blessed
                // way to build a DenseUnionVector — it keeps each child's valueCount and the offset
                // buffer consistent, which a strict (C++/Go) consumer requires.
                valueVec.setTypeId(idx, STRING_LEG)
                NullableVarCharHolder().also { h ->
                    val bytes = value.toByteArray()
                    h.isSet = 1
                    h.buffer = allocator.buffer(bytes.size.toLong()).also { it.setBytes(0, bytes) }
                    h.start = 0
                    h.end = bytes.size
                    valueVec.setSafe(idx, h)
                    h.buffer.close()
                }
                idx++
            }

            fun addInt32(code: Int, value: Int) {
                if (requestedCodes.isNotEmpty() && code !in requestedCodes) return
                infoNameVec.setSafe(idx, code)
                valueVec.setTypeId(idx, INT32_BITMASK_LEG)
                NullableIntHolder().also { h -> h.isSet = 1; h.value = value; valueVec.setSafe(idx, h) }
                idx++
            }

            addString(SqlInfo.FLIGHT_SQL_SERVER_NAME_VALUE, "XTDB")
            addString(SqlInfo.FLIGHT_SQL_SERVER_VERSION_VALUE, XtdbVersion.version)
            // The ADBC Go driver (underlying the Python adbc_driver_flightsql package) reads this code
            // at connect; without it set_autocommit(False) raises NOT_IMPLEMENTED. The value is the
            // SqlSupportedTransaction enum — TRANSACTION means begin/commit/rollback are supported.
            addInt32(
                SqlInfo.FLIGHT_SQL_SERVER_TRANSACTION_VALUE,
                SqlSupportedTransaction.SQL_SUPPORTED_TRANSACTION_TRANSACTION_VALUE
            )

            valueVec.valueCount = idx
            root.rowCount = idx
        }
    }

    companion object {
        // GET_SQL_INFO `value` dense-union leg type ids (FlightSqlProducer.Schemas.GET_SQL_INFO_SCHEMA).
        private const val STRING_LEG: Byte = 0
        private const val INT32_BITMASK_LEG: Byte = 3

        /**
         * An XTDB-specific action committing a Flight SQL transaction, as `EndTransaction` does, but returning the
         * commit's outcome: Flight SQL's `EndTransaction` returns nothing.
         *
         * The body is the transaction handle `BeginTransaction` returned.
         * A commit that submits a transaction sends one result, ahead of completion or of the commit's error, as a
         * JSON object:
         * - `txId`;
         * - `systemTime` and `committed`, both null for an async commit, which doesn't wait for the outcome;
         * - `error`, null unless the transaction aborted: its `category` (an anomaly category, or `error` for any
         *   other throwable), and `code`, `message` and `data` where present;
         * - `awaitToken`, which bounds a read to one that sees this transaction.
         *
         * A commit that submits nothing sends no result.
         * A handle that is unknown, already ended, or begun under another session fails with `NOT_FOUND`.
         */
        const val COMMIT_TRANSACTION_ACTION = "xtdb.CommitTransaction"
    }

    override fun getFlightInfoCatalogs(
        request: CommandGetCatalogs, ctx: CallContext?, descriptor: FlightDescriptor
    ): FlightInfo = metadataFlightInfo(request, FlightSqlProducer.Schemas.GET_CATALOGS_SCHEMA, descriptor)

    override fun getStreamCatalogs(ctx: CallContext?, listener: ServerStreamListener) = listener.reportingErrors {
        // through the shared catalogNames (no filter) so all three metadata handlers enumerate catalogs one way
        val dbNames = connectionFor(ctx, resolveDb(ctx)).catalogNames(null, exact = true)

        singleBatchStream(FlightSqlProducer.Schemas.GET_CATALOGS_SCHEMA, listener) { root ->
            val vec = root.getVector("catalog_name") as VarCharVector
            for ((idx, name) in dbNames.withIndex()) {
                vec.setSafe(idx, name.toByteArray())
            }
            root.rowCount = dbNames.size
        }
    }

    override fun getFlightInfoSchemas(
        request: CommandGetDbSchemas, ctx: CallContext?, descriptor: FlightDescriptor
    ): FlightInfo = metadataFlightInfo(request, FlightSqlProducer.Schemas.GET_SCHEMAS_SCHEMA, descriptor)

    override fun getStreamSchemas(
        command: CommandGetDbSchemas, ctx: CallContext?, listener: ServerStreamListener
    ) = listener.reportingErrors {
        val dbName = resolveDb(ctx)
        val catalogFilter = if (command.hasCatalog()) command.catalog else null
        val schemaFilter = if (command.hasDbSchemaFilterPattern()) command.dbSchemaFilterPattern else null

        val conn = connectionFor(ctx, dbName)
        // FlightSQL's catalog is an exact filter (absent = every catalog); each row keeps its own catalog.
        val rows = conn.catalogNames(catalogFilter, exact = true).flatMap { catalog ->
            conn.querySchemas(catalog, schemaFilter, null, null, null, GetObjectsDepth.DB_SCHEMAS)
                .keys.map { catalog to it }
        }

        singleBatchStream(FlightSqlProducer.Schemas.GET_SCHEMAS_SCHEMA, listener) { root ->
            val catalogVec = root.getVector("catalog_name") as VarCharVector
            val schemaVec = root.getVector("db_schema_name") as VarCharVector
            rows.forEachIndexed { idx, (catalog, name) ->
                catalogVec.setSafe(idx, catalog.toByteArray())
                schemaVec.setSafe(idx, name.toByteArray())
            }
            root.rowCount = rows.size
        }
    }

    override fun getFlightInfoTables(
        request: CommandGetTables, ctx: CallContext?, descriptor: FlightDescriptor
    ): FlightInfo {
        val schema = if (request.includeSchema) FlightSqlProducer.Schemas.GET_TABLES_SCHEMA
        else FlightSqlProducer.Schemas.GET_TABLES_SCHEMA_NO_SCHEMA
        return metadataFlightInfo(request, schema, descriptor)
    }

    override fun getStreamTables(
        command: CommandGetTables, ctx: CallContext?, listener: ServerStreamListener
    ) = listener.reportingErrors {
        val dbName = resolveDb(ctx)
        val catalogFilter = if (command.hasCatalog()) command.catalog else null
        val schemaFilter = if (command.hasDbSchemaFilterPattern()) command.dbSchemaFilterPattern else null
        val tableFilter = if (command.hasTableNameFilterPattern()) command.tableNameFilterPattern else null
        val typeFilters = command.tableTypesList.takeIf { it.isNotEmpty() }

        val schema = if (command.includeSchema) FlightSqlProducer.Schemas.GET_TABLES_SCHEMA
        else FlightSqlProducer.Schemas.GET_TABLES_SCHEMA_NO_SCHEMA

        val conn = connectionFor(ctx, dbName)

        // querySchemas owns the TABLE-type filter (XTDB only has TABLE) and the LIKE-escaped filters.
        // FlightSQL's catalog is an exact filter (absent = every catalog); each row keeps its own catalog.
        val rows = conn.catalogNames(catalogFilter, exact = true).flatMap { catalog ->
            conn.querySchemas(catalog, schemaFilter, tableFilter, typeFilters?.toTypedArray(), null, GetObjectsDepth.TABLES)
                .flatMap { (dbSchemaName, ts) -> ts.map { Triple(catalog, dbSchemaName, it.name) } }
        }

        singleBatchStream(schema, listener) { root ->
            val catalogVec = root.getVector("catalog_name") as VarCharVector
            val schemaVec = root.getVector("db_schema_name") as VarCharVector
            val tableVec = root.getVector("table_name") as VarCharVector
            val typeVec = root.getVector("table_type") as VarCharVector
            val tableSchemaVec =
                if (command.includeSchema) root.getVector("table_schema") as VarBinaryVector else null

            rows.forEachIndexed { idx, (catalog, dbSchemaName, tableName) ->
                catalogVec.setSafe(idx, catalog.toByteArray())
                schemaVec.setSafe(idx, dbSchemaName.toByteArray())
                tableVec.setSafe(idx, tableName.toByteArray())
                typeVec.setSafe(idx, "TABLE".toByteArray())

                if (tableSchemaVec != null) {
                    tableSchemaVec.setSafe(
                        idx,
                        conn.getTableSchema(catalog, dbSchemaName, tableName)
                            .serializeAsMessageInterruptibly()
                    )
                }
            }
            root.rowCount = rows.size
        }
    }

    private fun singleBatchStream(schema: Schema, listener: ServerStreamListener, populate: (VectorSchemaRoot) -> Unit) {
        VectorSchemaRoot.create(schema, allocator).use { root ->
            root.allocateNew()
            populate(root)
            listener.start(root)
            listener.putNext()
            listener.completed()
        }
    }

    override fun beginTransaction(
        req: ActionBeginTransactionRequest,
        ctx: CallContext?,
        listener: StreamListener<ActionBeginTransactionResult>
    ) = listener.reportingErrors {
        val txHandle = newHandle()
        val dbName = resolveDb(ctx)
        val session = connectionFor(ctx, dbName)
        val conn = newConnection(dbName).apply {
            awaitToken = session.awaitToken
            defaultTz = session.defaultTz
            setAutoCommit(false)
        }
        txConns[txHandle] = TxConn(currentSessionId(ctx), conn)

        listener.onNext(
            ActionBeginTransactionResult.newBuilder()
                .setTransactionId(txHandle)
                .build()
        )

        listener.onCompleted()
    }

    // resolve under the calling session only, then claim it atomically (remove(k,v) lets
    // exactly one concurrent ender win) - a wrong-session caller never reaches the remove.
    private fun claimTx(ctx: CallContext?, txHandle: TxHandle): Xtdb.Connection {
        val tx = txConnFor(ctx, txHandle)
        if (!txConns.remove(txHandle, tx))
            throw CallStatus.NOT_FOUND.withDescription("unknown transaction").toRuntimeException()
        return tx.conn
    }

    override fun endTransaction(
        req: ActionEndTransactionRequest,
        ctx: CallContext?,
        listener: StreamListener<Result>
    ) = listener.reportingErrors {
        claimTx(ctx, req.transactionId).use { conn ->
            if (req.action == EndTransaction.END_TRANSACTION_COMMIT) conn.commit() else conn.rollback()
        }
        listener.onCompleted()
    }

    override fun doAction(context: CallContext, action: Action, listener: StreamListener<Result>) {
        if (action.type != COMMIT_TRANSACTION_ACTION) return super.doAction(context, action, listener)

        listener.reportingErrors {
            claimTx(context, ByteString.copyFrom(action.body)).use { conn ->
                try {
                    conn.commit()
                } finally {
                    conn.lastSubmittedTx?.let { listener.onNext(it.toCommitResult(conn.awaitToken)) }
                }
            }
            listener.onCompleted()
        }
    }
}
