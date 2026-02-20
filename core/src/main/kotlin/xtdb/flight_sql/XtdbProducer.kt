package xtdb.flight_sql

import clojure.lang.IPersistentMap
import clojure.lang.IPersistentVector
import clojure.lang.PersistentArrayMap
import clojure.lang.PersistentArrayMap.EMPTY
import com.google.protobuf.Any as ProtoAny
import com.google.protobuf.ByteString
import com.google.protobuf.Message
import org.apache.arrow.flight.*
import org.apache.arrow.flight.FlightProducer.*
import org.apache.arrow.flight.sql.NoOpFlightSqlProducer
import org.apache.arrow.flight.sql.impl.FlightSql.*
import org.apache.arrow.flight.sql.impl.FlightSql.ActionEndTransactionRequest.EndTransaction
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorLoader
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.VectorUnloader
import org.apache.arrow.vector.types.pojo.Schema
import xtdb.adbc.XtdbConnection
import xtdb.api.Xtdb
import xtdb.arrow.ArrowUnloader
import xtdb.arrow.Relation
import xtdb.arrow.RelationReader
import xtdb.arrow.Vector
import xtdb.arrow.VectorType
import xtdb.asBytes
import xtdb.IResultCursor
import xtdb.arrow.VectorType.Companion.ofType
import xtdb.arrow.withName
import xtdb.kw
import xtdb.query.PreparedQuery
import xtdb.tx.TxOp
import xtdb.util.closeAll
import xtdb.util.closeOnCatch
import xtdb.util.safeMapIndexed
import xtdb.util.logger
import xtdb.util.requiringResolve
import xtdb.util.warn
import java.io.ByteArrayOutputStream
import java.nio.channels.Channels
import java.util.*
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

private fun cljMap(vararg pairs: Pair<Any, Any?>): IPersistentMap = PersistentArrayMap.create(pairs.toMap())

private fun isDml(sql: String): Boolean {
    // TODO multi-db
    val defaultDbOpts = cljMap("default-db".kw to "xtdb")
    val parsed = requiringResolve("xtdb.sql.parse/parse-statement")
        .invoke(sql, defaultDbOpts)

    val opKeyword = (parsed as? IPersistentVector)?.nth(0) as? clojure.lang.Keyword
    return opKeyword?.name in setOf("insert", "update", "delete", "erase", "create-user", "alter-user")
}

private fun FlightStream.toRows(allocator: BufferAllocator): List<List<Any?>> {
    val rows = ArrayList<List<Any?>>()

    Relation.fromRoot(allocator, root).use { rel ->
        while (next()) {
            rel.loadFromArrow(root)
            rows.addAll(rel.toTuples())
        }
    }

    return rows
}

private fun FlightStream.toBytes(allocator: BufferAllocator): ByteArray =
    ByteArrayOutputStream().use { out ->
        val rootUnl = VectorUnloader(root)
        Relation(allocator, root.schema.fields.mapIndexed { idx, f -> f.withName("?_$idx") })
            .use { rel ->
                rel.startUnload(Channels.newChannel(out), ArrowUnloader.Mode.STREAM).use { unl ->
                    while (next()) {
                        rootUnl.recordBatch.use { rb -> rel.load(rb) }
                        unl.writePage()
                    }
                    unl.end()

                    out.toByteArray()
                }
            }
    }

private class PreparedStatement(
    val sql: String,
    val txHandle: TxHandle?,
    val prepdQuery: PreparedQuery?,
    var cursor: IResultCursor? = null
) : AutoCloseable {
    override fun close() {
        cursor?.close()
        cursor = null
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

class XtdbProducer(private val node: Xtdb) : NoOpFlightSqlProducer(), AutoCloseable {
    private val allocator = node.allocator.newChildAllocator("flight-sql", 0, Long.MAX_VALUE)

    private val connections = ConcurrentHashMap<TxHandle, XtdbConnection>()
    private val stmts = ConcurrentHashMap<PreparedStatementHandle, PreparedStatement>()
    private val tickets = ConcurrentHashMap<TicketHandle, IResultCursor>()

    /** Connection for auto-commit (non-transactional) operations. */
    private val defaultConnection: XtdbConnection = node.connect() as XtdbConnection

    override fun close() {
        stmts.closeAll()
        connections.values.forEach { it.close() }
        connections.clear()
        defaultConnection.close()
        allocator.close()
    }

    private fun connectionFor(txHandle: TxHandle?): XtdbConnection =
        if (txHandle != null) {
            requireNotNull(connections[txHandle]) { "unknown tx" }
        } else {
            defaultConnection
        }

    private fun execDml(op: TxOp, txHandle: TxHandle?) {
        try {
            connectionFor(txHandle).executeDml(op)
        } catch (t: Throwable) {
            LOGGER.warn(t, "bang")
            throw t
        }
    }

    private fun handleGetStream(cursor: IResultCursor, listener: ServerStreamListener) {
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
                if (cmd.hasTransactionId()) cmd.transactionId else null
            )

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
        // TODO in tx?
        val psId = cmd.preparedStatementHandle

        stmts
            .computeIfPresent(psId) { _, ps ->
                // TODO we likely needn't take these out and put them back.
                val row = flightStream.toRows(allocator).firstOrNull().orEmpty()

                val newArgs = row.safeMapIndexed { idx, v ->
                    Vector.fromList(allocator, "?_$idx", listOf(v))
                }

                RelationReader.from(newArgs, 1).closeOnCatch { argsRel ->
                    ps.cursor?.close()
                    ps.cursor = ps.prepdQuery?.openQuery(cljMap("args".kw to argsRel))
                    ps
                }
            }
            .also { requireNotNull(it) { "invalid ps-id" } }

        ackStream.onCompleted()
    }

    override fun acceptPutPreparedStatementUpdate(
        cmd: CommandPreparedStatementUpdate,
        ctx: CallContext?,
        flightStream: FlightStream,
        ackStream: StreamListener<PutResult>
    ): Runnable = Runnable {
        // NOTE atm the PSs are either created within a tx and then assumed to be within that tx
        // my mental model would be that you could create a PS outside a tx and then use it inside,
        // but (maybe out of date?) this doesn't seem possible in FSQL.
        val ps = requireNotNull(stmts[cmd.preparedStatementHandle]) { "invalid ps-id" }

        val op = TxOp.SqlBytes(ps.sql, flightStream.toBytes(allocator))
        try {
            execDml(op, ps.txHandle)
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

            // see #5082 — Python ADBC's cursor.execute() routes DML through the query path
            if (isDml(sql)) {
                throw CallStatus.INVALID_ARGUMENT
                    .withDescription("DML statements should be submitted via executeUpdate, not executeQuery (in Python ADBC, use cursor.executescript())")
                    .toRuntimeException()
            }
            val ticketHandle = newHandle()
            val pq = defaultConnection.prepareSql(sql)
            val cursor = pq.openQuery(EMPTY)
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

        val ticket = Ticket(
            ProtoAny.pack(
                CommandPreparedStatementQuery.newBuilder()
                    .setPreparedStatementHandle(psId)
                    .build()
            ).toByteArray()
        )

        val cursor = checkNotNull(ps.cursor ?: ps.prepdQuery?.openQuery(EMPTY)) { "invalid ps-id" }

        ps.cursor = cursor
        return FlightInfo(
            resultTypesToSchema(cursor.resultTypes),
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
        handleGetStream(checkNotNull(ps.cursor) { "cursor not open" }, listener)
    }

    override fun createPreparedStatement(
        req: ActionCreatePreparedStatementRequest,
        ctx: CallContext?,
        listener: StreamListener<Result>
    ) {
        val psId = newHandle()
        val sql = req.queryBytes.toStringUtf8()
        val pq = defaultConnection.prepareSql(sql)
        val ps = PreparedStatement(
            sql,
            if (req.hasTransactionId()) req.transactionId else null,
            pq.takeIf { !isDml(sql) }
        )

        stmts[psId] = ps

        listener.onNext(
            packResult(
                ActionCreatePreparedStatementResult.newBuilder()
                    .setPreparedStatementHandle(psId)
                    .setParameterSchema(
                        ByteString.copyFrom(
                            Schema(
                                (0 until pq.paramCount).map { idx ->
                                    "$$idx" ofType VectorType.fromLegs()
                                }
                            ).serializeAsMessage()
                        )
                    )
                    .build()
            )
        )

        listener.onCompleted()
    }

    override fun closePreparedStatement(
        req: ActionClosePreparedStatementRequest,
        ctx: CallContext?,
        listener: StreamListener<Result>
    ) {
        val ps = stmts.remove(req.preparedStatementHandle)
        ps?.cursor?.close()

        listener.onCompleted()
    }

    override fun beginTransaction(
        req: ActionBeginTransactionRequest,
        ctx: CallContext?,
        listener: StreamListener<ActionBeginTransactionResult>
    ) {
        val txHandle = newHandle()
        val conn = node.connect() as XtdbConnection
        conn.setAutoCommit(false)
        connections[txHandle] = conn

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
        val conn = requireNotNull(connections.remove(txHandle)) { "unknown tx" }

        if (req.action == EndTransaction.END_TRANSACTION_COMMIT) {
            try {
                conn.commit()
                listener.onCompleted()
            } catch (t: Throwable) {
                listener.onError(t)
            } finally {
                conn.close()
            }
        } else {
            conn.close()
            listener.onCompleted()
        }
    }
}
