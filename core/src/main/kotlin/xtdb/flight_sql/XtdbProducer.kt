package xtdb.flight_sql

import xtdb.query.ParsedStatement
import xtdb.query.parseStatement
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
import org.apache.arrow.adbc.core.AdbcStatement
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
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.adbc.core.BulkIngestMode
import xtdb.api.Xtdb
import xtdb.arrow.Relation
import xtdb.arrow.VectorType
import xtdb.asBytes
import xtdb.ResultCursor
import xtdb.tx.TxOp
import xtdb.util.closeAll
import xtdb.util.closeOnCatch
import xtdb.util.logger
import xtdb.util.serializeAsMessageInterruptibly
import xtdb.util.warn
import java.util.*
import java.util.concurrent.Callable
import java.util.concurrent.ConcurrentHashMap

private fun resultTypesToSchema(types: SequencedMap<String, VectorType>) =
    Schema(types.map { (name, type) -> type.toField(name) })

private typealias TxHandle = ByteString
private typealias PreparedStatementHandle = ByteString
private typealias TicketHandle = ByteString

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

private fun isDml(sql: String): Boolean = parseStatement(sql) is ParsedStatement.Dml

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

private val AdbcStatement.QueryResult.schema: Schema
    get() = reader.vectorSchemaRoot.schema

private class PreparedStatement(
    val xtdbStmt: Xtdb.Statement,
    var queryResult: AdbcStatement.QueryResult? = null
) : AutoCloseable {
    override fun close() {
        queryResult?.close()
        queryResult = null
        xtdbStmt.close()
    }
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
    private val tickets = ConcurrentHashMap<TicketHandle, ResultCursor>()

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

    private fun execDml(op: TxOp, ctx: CallContext?, txHandle: TxHandle?) {
        txOrSessionConnection(ctx, txHandle).executeDml(op)
    }

    private fun handleGetStream(cursor: ResultCursor, listener: ServerStreamListener) {
        try {
            VectorSchemaRoot.create(resultTypesToSchema(cursor.resultTypes), allocator).use { vsr ->
                val rootLoader = VectorLoader(vsr)
                listener.start(vsr)

                cursor.forEachRemaining { inRel ->
                    inRel.openDirectSlice(allocator).use { rel ->
                        rel.openArrowRecordBatch().use { rb ->
                            rootLoader.load(rb)
                            listener.putNext()
                        }
                    }
                }

                listener.completed()
            }
        } catch (t: Throwable) {
            LOGGER.warn(t, "Error in handleGetStream")
            throw t
        }
    }

    override fun acceptPutStatement(
        cmd: CommandStatementUpdate,
        ctx: CallContext?,
        flightStream: FlightStream?,
        ackStream: StreamListener<PutResult>
    ): Runnable = Runnable {
        try {
            execDml(
                TxOp.Sql(cmd.query),
                ctx,
                if (cmd.hasTransactionId()) cmd.transactionId else null
            )

            ackStream.sendDoPutUpdateRes(allocator)
        } catch (t: Throwable) {
            ackStream.onError(t)
        }
    }

    override fun acceptPutStatementBulkIngest(
        cmd: CommandStatementIngest,
        ctx: CallContext?,
        flightStream: FlightStream,
        ackStream: StreamListener<PutResult>
    ): Runnable = Runnable {
        try {
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
        } catch (t: Throwable) {
            ackStream.onError(t)
        }
    }

    override fun acceptPutPreparedStatementQuery(
        cmd: CommandPreparedStatementQuery,
        ctx: CallContext?,
        flightStream: FlightStream,
        ackStream: StreamListener<PutResult>
    ): Runnable = Runnable {
        val ps = requireNotNull(stmts[cmd.preparedStatementHandle]) { "invalid ps-id" }
        try {
            flightStream.next()
            ps.queryResult?.close()
            ps.xtdbStmt.bind(flightStream.root)
            ps.queryResult = ps.xtdbStmt.executeQuery()
            ackStream.onCompleted()
        } catch (t: Throwable) {
            ackStream.onError(t)
        }
    }

    override fun acceptPutPreparedStatementUpdate(
        cmd: CommandPreparedStatementUpdate,
        ctx: CallContext?,
        flightStream: FlightStream,
        ackStream: StreamListener<PutResult>
    ): Runnable = Runnable {
        val ps = requireNotNull(stmts[cmd.preparedStatementHandle]) { "invalid ps-id" }
        try {
            flightStream.toRelation(node.allocator).use { acc ->
                ps.xtdbStmt.bind(acc)
                ps.xtdbStmt.executeUpdate()
            }
            ackStream.sendDoPutUpdateRes(allocator)
        } catch (t: Throwable) {
            ackStream.onError(t)
        }
    }

    override fun getFlightInfoStatement(
        cmd: CommandStatementQuery,
        ctx: CallContext?,
        descriptor: FlightDescriptor
    ): FlightInfo {
        try {
            val sql = cmd.queryBytes.toStringUtf8()
            val dbName = resolveDb(ctx)

            // see #5082 — Python ADBC's cursor.execute() routes DML through the query path
            if (isDml(sql)) {
                throw CallStatus.INVALID_ARGUMENT
                    .withDescription("DML statements should be submitted via executeUpdate, not executeQuery (in Python ADBC, use cursor.executescript())")
                    .toRuntimeException()
            }
            val ticketHandle = newHandle()
            val cursor = connectionFor(ctx, dbName).openSqlQuery(sql)
            val ticket = Ticket(
                ProtoAny.pack(
                    TicketStatementQuery.newBuilder()
                        .setStatementHandle(ticketHandle)
                        .build()
                ).toByteArray()
            )
            tickets[ticketHandle] = cursor
            return FlightInfo(
                resultTypesToSchema(cursor.resultTypes),
                descriptor,
                listOf(FlightEndpoint(ticket)),
                /* bytes = */ -1, /* records = */ -1
            )
        } catch (t: Throwable) {
            LOGGER.log(System.Logger.Level.ERROR, "Error getting flight info for statement", t)
            throw t
        }
    }

    override fun getStreamStatement(
        ticket: TicketStatementQuery, ctx: CallContext?, listener: ServerStreamListener
    ) {
        val cursor = requireNotNull(tickets.remove(ticket.statementHandle)) { "unknown ticket-id" }
        try {
            handleGetStream(cursor, listener)
        } finally {
            cursor.close()
        }
    }

    override fun getFlightInfoPreparedStatement(
        cmd: CommandPreparedStatementQuery, ctx: CallContext?, descriptor: FlightDescriptor
    ): FlightInfo {
        val psId = cmd.preparedStatementHandle
        val ps = requireNotNull(stmts[psId]) { "invalid ps-id" }

        val queryResult = ps.queryResult ?: ps.xtdbStmt.executeQuery().also { ps.queryResult = it }

        val ticket = Ticket(
            ProtoAny.pack(
                CommandPreparedStatementQuery.newBuilder()
                    .setPreparedStatementHandle(psId)
                    .build()
            ).toByteArray()
        )

        return FlightInfo(
            queryResult.schema,
            descriptor,
            listOf(FlightEndpoint(ticket)),
            -1,
            -1
        )
    }

    override fun getStreamPreparedStatement(
        ticket: CommandPreparedStatementQuery, ctx: CallContext?, listener: ServerStreamListener
    ) {
        val ps = requireNotNull(stmts[ticket.preparedStatementHandle]) { "invalid ps-id" }
        val queryResult = checkNotNull(ps.queryResult) { "no cursor open for ps-id" }
        ps.queryResult = null
        streamArrowReader(queryResult.reader, listener)
    }

    override fun createPreparedStatement(
        req: ActionCreatePreparedStatementRequest,
        ctx: CallContext?,
        listener: StreamListener<Result>
    ) {
        val psId = newHandle()
        val sql = req.queryBytes.toStringUtf8()
        val txHandle = if (req.hasTransactionId()) req.transactionId else null
        txOrSessionConnection(ctx, txHandle).createStatement().closeOnCatch { xtdbStmt ->
            xtdbStmt.setSqlQuery(sql)
            xtdbStmt.prepare()

            val resultBuilder = ActionCreatePreparedStatementResult.newBuilder()
                .setPreparedStatementHandle(psId)
                .setParameterSchema(
                    ByteString.copyFrom(xtdbStmt.parameterSchema.serializeAsMessageInterruptibly())
                )

            if (!isDml(sql)) {
                resultBuilder.setDatasetSchema(
                    ByteString.copyFrom(xtdbStmt.executeSchema().serializeAsMessageInterruptibly())
                )
            }

            stmts[psId] = PreparedStatement(xtdbStmt)
            listener.onNext(packResult(resultBuilder.build()))
            listener.onCompleted()
        }
    }

    override fun getSchemaPreparedStatement(
        cmd: CommandPreparedStatementQuery, ctx: CallContext?, descriptor: FlightDescriptor
    ): SchemaResult {
        val ps = requireNotNull(stmts[cmd.preparedStatementHandle]) { "invalid ps-id" }
        return SchemaResult(ps.xtdbStmt.executeSchema())
    }

    override fun getSchemaStatement(
        cmd: CommandStatementQuery, ctx: CallContext?, descriptor: FlightDescriptor
    ): SchemaResult {
        val sql = cmd.query
        val dbName = resolveDb(ctx)
        if (isDml(sql)) {
            throw CallStatus.INVALID_ARGUMENT
                .withDescription("executeSchema only supports queries (DML returns a row count, not a schema)")
                .toRuntimeException()
        }
        connectionFor(ctx, dbName).createStatement().use { stmt ->
            stmt.setSqlQuery(sql)
            stmt.prepare()
            return SchemaResult(stmt.executeSchema())
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
    ) {
        try {
            val opts = mapOf(
                "catalog" to SessionOptionValueFactory.makeSessionOptionValue(resolveDb(ctx)),
                "schema" to SessionOptionValueFactory.makeSessionOptionValue("public"),
            )
            listener.onNext(GetSessionOptionsResult(opts))
            listener.onCompleted()
        } catch (t: Throwable) {
            listener.onError(t)
        }
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
            listener.onError(t)
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
            listener.onError(t)
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

    override fun getStreamTableTypes(ctx: CallContext?, listener: ServerStreamListener) {
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
            addString(SqlInfo.FLIGHT_SQL_SERVER_VERSION_VALUE, "dev")
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

    private companion object {
        // GET_SQL_INFO `value` dense-union leg type ids (FlightSqlProducer.Schemas.GET_SQL_INFO_SCHEMA).
        const val STRING_LEG: Byte = 0
        const val INT32_BITMASK_LEG: Byte = 3
    }

    override fun getFlightInfoCatalogs(
        request: CommandGetCatalogs, ctx: CallContext?, descriptor: FlightDescriptor
    ): FlightInfo = metadataFlightInfo(request, FlightSqlProducer.Schemas.GET_CATALOGS_SCHEMA, descriptor)

    override fun getStreamCatalogs(ctx: CallContext?, listener: ServerStreamListener) {
        val dbNames = node.databaseNames

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
    ) {
        val dbName = resolveDb(ctx)
        val catalogFilter = if (command.hasCatalog()) command.catalog else null
        val schemaFilter = if (command.hasDbSchemaFilterPattern()) command.dbSchemaFilterPattern else null

        if (catalogFilter != null && catalogFilter != dbName) {
            singleBatchStream(FlightSqlProducer.Schemas.GET_SCHEMAS_SCHEMA, listener) { root ->
                root.rowCount = 0
            }
            return
        }

        val sql = buildString {
            append("SELECT DISTINCT table_schema FROM information_schema.tables")
            schemaFilter?.let { append(" WHERE table_schema LIKE '${it.replace("'", "''")}'") }
            append(" ORDER BY table_schema")
        }

        connectionFor(ctx, dbName).openSqlQuery(sql).use { cursor ->
            singleBatchStream(FlightSqlProducer.Schemas.GET_SCHEMAS_SCHEMA, listener) { root ->
                val catalogVec = root.getVector("catalog_name") as VarCharVector
                val schemaVec = root.getVector("db_schema_name") as VarCharVector
                var idx = 0
                cursor.forEachRemaining { rel ->
                    for (i in 0 until rel.rowCount) {
                        catalogVec.setSafe(idx, dbName.toByteArray())
                        schemaVec.setSafe(idx, rel.get("table_schema").getObject(i).toString().toByteArray())
                        idx++
                    }
                }
                root.rowCount = idx
            }
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
    ) {
        val dbName = resolveDb(ctx)
        val catalogFilter = if (command.hasCatalog()) command.catalog else null
        val schemaFilter = if (command.hasDbSchemaFilterPattern()) command.dbSchemaFilterPattern else null
        val tableFilter = if (command.hasTableNameFilterPattern()) command.tableNameFilterPattern else null
        val typeFilters = command.tableTypesList.takeIf { it.isNotEmpty() }

        if (catalogFilter != null && catalogFilter != dbName) {
            val schema = if (command.includeSchema) FlightSqlProducer.Schemas.GET_TABLES_SCHEMA
            else FlightSqlProducer.Schemas.GET_TABLES_SCHEMA_NO_SCHEMA
            singleBatchStream(schema, listener) { root -> root.rowCount = 0 }
            return
        }

        // XTDB only has TABLE type
        if (typeFilters != null && "TABLE" !in typeFilters) {
            val schema = if (command.includeSchema) FlightSqlProducer.Schemas.GET_TABLES_SCHEMA
            else FlightSqlProducer.Schemas.GET_TABLES_SCHEMA_NO_SCHEMA
            singleBatchStream(schema, listener) { root -> root.rowCount = 0 }
            return
        }

        val conditions = mutableListOf<String>()
        schemaFilter?.let { conditions.add("table_schema LIKE '${it.replace("'", "''")}'") }
        tableFilter?.let { conditions.add("table_name LIKE '${it.replace("'", "''")}'") }
        val where = if (conditions.isNotEmpty()) "WHERE ${conditions.joinToString(" AND ")}" else ""

        val sql = "SELECT DISTINCT table_schema, table_name FROM information_schema.tables $where ORDER BY table_schema, table_name"

        val tablesConn = connectionFor(ctx, dbName)
        tablesConn.openSqlQuery(sql).use { cursor ->
            val schema = if (command.includeSchema) FlightSqlProducer.Schemas.GET_TABLES_SCHEMA
            else FlightSqlProducer.Schemas.GET_TABLES_SCHEMA_NO_SCHEMA

            singleBatchStream(schema, listener) { root ->
                val catalogVec = root.getVector("catalog_name") as VarCharVector
                val schemaVec = root.getVector("db_schema_name") as VarCharVector
                val tableVec = root.getVector("table_name") as VarCharVector
                val typeVec = root.getVector("table_type") as VarCharVector
                val tableSchemaVec =
                    if (command.includeSchema) root.getVector("table_schema") as VarBinaryVector else null

                val conn = tablesConn
                val snap = if (command.includeSchema) conn.openSnapshot() else null

                try {
                    var idx = 0
                    cursor.forEachRemaining { rel ->
                        for (i in 0 until rel.rowCount) {
                            val dbSchemaName = rel.get("table_schema").getObject(i).toString()
                            val tableName = rel.get("table_name").getObject(i).toString()
                            catalogVec.setSafe(idx, dbName.toByteArray())
                            schemaVec.setSafe(idx, dbSchemaName.toByteArray())
                            tableVec.setSafe(idx, tableName.toByteArray())
                            typeVec.setSafe(idx, "TABLE".toByteArray())

                            if (snap != null && tableSchemaVec != null) {
                                tableSchemaVec.setSafe(
                                    idx,
                                    conn.getTableSchema(dbSchemaName, tableName, snap)
                                        .serializeAsMessageInterruptibly()
                                )
                            }

                            idx++
                        }
                    }
                    root.rowCount = idx
                } finally {
                    snap?.close()
                }
            }
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
    ) {
        val txHandle = newHandle()
        val conn = (node.connect()).also { it.setCurrentCatalog(resolveDb(ctx)) }
        conn.setAutoCommit(false)
        txConns[txHandle] = TxConn(currentSessionId(ctx), conn)

        listener.onNext(
            ActionBeginTransactionResult.newBuilder()
                .setTransactionId(txHandle)
                .build()
        )

        listener.onCompleted()
    }

    override fun endTransaction(
        req: ActionEndTransactionRequest,
        ctx: CallContext?,
        listener: StreamListener<Result>
    ) {
        val txHandle = req.transactionId
        // resolve under the calling session only, then claim it atomically (remove(k,v) lets
        // exactly one concurrent ender win) - a wrong-session caller never reaches the remove.
        val tx = txConnFor(ctx, txHandle)
        if (!txConns.remove(txHandle, tx))
            return listener.onError(
                CallStatus.NOT_FOUND.withDescription("unknown transaction").toRuntimeException()
            )
        val conn = tx.conn

        try {
            if (req.action == EndTransaction.END_TRANSACTION_COMMIT) conn.commit() else conn.rollback()
            listener.onCompleted()
        } catch (t: Throwable) {
            listener.onError(t)
        } finally {
            conn.close()
        }
    }
}
