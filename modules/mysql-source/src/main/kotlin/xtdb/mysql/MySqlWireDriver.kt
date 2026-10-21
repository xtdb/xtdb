package xtdb.mysql

import com.github.shyiko.mysql.binlog.BinaryLogClient
import com.github.shyiko.mysql.binlog.event.*
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream
import com.github.shyiko.mysql.binlog.network.ServerException
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.isActive
import kotlinx.coroutines.runInterruptible
import xtdb.api.error.Fault
import xtdb.api.error.Incorrect
import xtdb.api.error.Unavailable
import xtdb.api.error.Unsupported
import xtdb.util.debug
import xtdb.util.info
import xtdb.util.logger
import java.nio.charset.Charset
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import java.time.Instant
import java.util.BitSet
import java.util.Properties
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import kotlin.time.Duration.Companion.milliseconds

private val LOG = MySqlWireDriver::class.logger

private const val SNAPSHOT_BATCH_SIZE = 1000

// ER_SOURCE_FATAL_ERROR_READING_BINLOG — a purge, but also a server-id clash, among others
private const val ER_SOURCE_FATAL_ERROR_READING_BINLOG = 1236

private val IDLE_POLL = 50.milliseconds

private fun connectionLost(message: String, cause: Throwable? = null) =
    Unavailable(message, "xtdb.mysql/binlog-connection-lost", emptyMap<String, Any>(), cause)

/** @suppress */
class MySqlWireDriver(
    private val dbName: String,
    private val hostname: String,
    private val port: Int,
    private val username: String,
    private val password: String,
    private val serverId: Long,
    private val tables: Set<TableId>,
) : MySqlDriver {

    private fun openConnection(): Connection =
        DriverManager.getConnection(
            "jdbc:mysql://$hostname:$port/",
            Properties().also {
                it["user"] = username
                it["password"] = password
                it["connectTimeout"] = "10000"
                it["characterEncoding"] = "UTF-8"
                it["tinyInt1isBit"] = "false"
                it["yearIsDateType"] = "false"
                it["zeroDateTimeBehavior"] = "CONVERT_TO_NULL"
                it["useServerPrepStmts"] = "true"
            }
        ).also { conn ->
            conn.createStatement().use { s ->
                // TIMESTAMP renders in the session's zone; UTC makes the snapshot read the instant the binlog carries
                s.execute("SET SESSION time_zone = '+00:00'")
                s.execute("SET SESSION net_write_timeout = 3600")
            }
        }

    private inline fun <T> withConnection(f: (Connection) -> T): T = openConnection().use(f)

    private fun Connection.queryString(sql: String): String? =
        createStatement().use { s -> s.executeQuery(sql).use { rs -> if (rs.next()) rs.getString(1) else null } }

    private fun Connection.executedGtids() = GtidSet.parse(queryString("SELECT @@global.gtid_executed"))
    private fun Connection.purgedGtids() = GtidSet.parse(queryString("SELECT @@global.gtid_purged"))

    override fun checkUpstream() = withConnection { conn ->
        fun require(setting: String, vararg acceptable: String) {
            val actual =
                try {
                    conn.queryString("SELECT @@global.$setting")
                } catch (e: SQLException) {
                    throw Incorrect(
                        "can't read '$setting' on the upstream — the MySQL source needs MySQL 8.0.20 or later: ${e.message}",
                        errorCode = "xtdb.mysql/unsupported-upstream-version",
                        data = mapOf("setting" to setting),
                        cause = e,
                    )
                }

            if (acceptable.none { it.equals(actual, ignoreCase = true) })
                throw Incorrect(
                    "the upstream's $setting is '$actual' — it must be '${acceptable.first()}' for the MySQL source to read the binlog",
                    errorCode = "xtdb.mysql/upstream-setting",
                    data = mapOf("setting" to setting, "actual" to actual, "required" to acceptable.first()),
                )
        }

        require("log_bin", "1")
        require("binlog_format", "ROW")
        require("binlog_row_image", "FULL")
        require("binlog_row_metadata", "FULL")
        require("gtid_mode", "ON")
        require("binlog_row_value_options", "")
        require("binlog_transaction_compression", "OFF", "0")

        for (table in tables) {
            val upstreamName = conn.prepareStatement("SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?").use { ps ->
                ps.setString(1, table.database); ps.setString(2, table.table)
                ps.executeQuery().use { rs -> if (rs.next()) TableId(rs.getString(1), rs.getString(2)) else null }
            }

            if (upstreamName == null)
                throw Incorrect(
                    "table $table doesn't exist on the upstream",
                    errorCode = "xtdb.mysql/missing-table",
                    data = mapOf("table" to table.toString()),
                )

            // the binlog names a table as the upstream stores it, whatever case the lookup above matched in
            if (upstreamName != table)
                throw Incorrect(
                    "table $table is named $upstreamName on the upstream — name it exactly so in 'tables'",
                    errorCode = "xtdb.mysql/table-name-case",
                    data = mapOf("table" to table.toString(), "upstream-name" to upstreamName.toString()),
                )
        }
    }

    private fun Connection.checkResumable(from: GtidSet) {
        val executed = executedGtids()
        val purged = purgedGtids()

        if (!from.isSubsetOf(executed))
            throw Incorrect(
                "the upstream hasn't executed everything XTDB has indexed from it — it's a different server, or has lost transactions " +
                    "(XTDB has '$from', the upstream '$executed')",
                errorCode = "xtdb.mysql/upstream-behind",
                data = mapOf("position" to from.toString(), "executed" to executed.toString()),
            )

        if (!purged.isSubsetOf(from))
            throw Incorrect(
                "the upstream has purged binlog XTDB still needs: it has purged '$purged', and XTDB has only '$from' — " +
                    "re-create the database to snapshot afresh, and raise binlog_expire_logs_seconds so it doesn't recur",
                errorCode = "xtdb.mysql/binlog-purged",
                data = mapOf("position" to from.toString(), "purged" to purged.toString()),
            )
    }

    private fun Connection.readTableDef(id: TableId): TableDef {
        val columns = prepareStatement(
            """SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, NUMERIC_PRECISION, CHARACTER_SET_NAME
               FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION"""
        ).use { ps ->
            ps.setString(1, id.database); ps.setString(2, id.table)
            ps.executeQuery().use { rs ->
                buildList {
                    while (rs.next())
                        add(
                            TableDef.Companion.InfoSchemaColumn(
                                rs.getString(1), rs.getString(2), rs.getString(3),
                                rs.getObject(4)?.let { (it as Number).toLong() },
                                rs.getString(5),
                            )
                        )
                }
            }
        }

        return TableDef.fromInformationSchema(id, columns)
    }

    override fun openSnapshot(): MySqlDriver.SnapshotReader {
        val conn = openConnection()
        try {
            val (position, defs) = conn.createStatement().use { s ->
                s.execute("SET SESSION lock_wait_timeout = 60")

                // MySQL has no one statement that opens a snapshot and says where in the binlog it is:
                // hold every commit off for as long as it takes to do the two, then read at leisure.
                // The table shapes are read under the lock too, so no ALTER can land between reading them and the rows.
                LOG.debug { "[$dbName] FLUSH TABLES WITH READ LOCK" }
                s.execute("FLUSH TABLES WITH READ LOCK")
                try {
                    s.execute("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                    s.execute("START TRANSACTION WITH CONSISTENT SNAPSHOT")

                    conn.executedGtids() to tables.sortedBy { it.toString() }.map { conn.readTableDef(it) }
                } finally {
                    s.execute("UNLOCK TABLES")
                }
            }

            LOG.info("[$dbName] Snapshot open at GTID set '$position'")
            return Snapshot(conn, position, defs)
        } catch (e: Throwable) {
            runCatching { conn.close() }
            throw e
        }
    }

    private inner class Snapshot(
        private val conn: Connection,
        override val position: GtidSet,
        private val defs: List<TableDef>,
    ) : MySqlDriver.SnapshotReader {

        override fun batches(): Sequence<List<RowOp.Put>> = sequence {
            for (def in defs) {
                LOG.info("[$dbName] Snapshotting ${def.id} (${def.columns.size} columns: ${def.columns.joinToString { it.name }})")

                var rowCount = 0
                conn.createStatement().use { stmt ->
                    // Connector/J streams a result row by row only at this fetch size
                    stmt.fetchSize = Int.MIN_VALUE

                    stmt.executeQuery("SELECT ${def.columns.joinToString { quote(it.name) }} FROM ${quote(def.id.database)}.${quote(def.id.table)}").use { rs ->
                        generateSequence { if (rs.next()) def.columns.indices.associate { i -> def.columns[i].name to def.columns[i].type.fromSnapshot(rs, i + 1) } else null }
                            .chunked(SNAPSHOT_BATCH_SIZE)
                            .forEach { batch ->
                                rowCount += batch.size
                                yield(batch.map { RowOp.Put(def.id.database, def.id.table, it) })
                            }
                    }
                }

                LOG.info("[$dbName] Finished snapshotting ${def.id} ($rowCount rows)")
            }
        }

        // abort() rather than close() alone: close() waits on a streaming read still parked on the socket
        override fun close() {
            runCatching { conn.abort(Runnable::run) }
            runCatching { conn.close() }
        }
    }

    private fun quote(identifier: String) = "`${identifier.replace("`", "``")}`"

    private fun Connection.collationCharsets(): Map<Int, Charset> =
        createStatement().use { s ->
            s.executeQuery("SELECT ID, CHARACTER_SET_NAME FROM information_schema.COLLATIONS").use { rs ->
                buildMap {
                    while (rs.next())
                        runCatching { MySqlCharsets.javaCharset(rs.getString(2)) }.onSuccess { put(rs.getInt(1), it) }
                }
            }
        }

    override suspend fun openStream(from: GtidSet): MySqlDriver.ChangeStream {
        LOG.info("[$dbName] Opening binlog stream after GTID set '$from'")

        val collations =
            try {
                runInterruptible(Dispatchers.IO) {
                    withConnection { conn ->
                        conn.checkResumable(from)
                        conn.collationCharsets()
                    }
                }
            } catch (e: SQLException) {
                throw connectionLost("couldn't check the upstream before opening the binlog: ${e.message}", e)
            }

        return BinlogStream(from, collations).also { it.connect() }
    }

    private sealed interface Received {
        class Ev(val event: Event) : Received
        class Failure(val cause: Throwable) : Received
    }

    private class OpenTx(val gtid: Gtid, val commitTime: Instant, val alreadyIndexed: Boolean) {
        var began = false
        val ops = mutableListOf<RowOp>()
    }

    private fun binlogRefused(from: GtidSet, e: ServerException) =
        Incorrect(
            "the upstream refused to serve the binlog after GTID set '$from' — " +
                "if it says another replica has the same server id, give this source a serverId of its own: ${e.message}",
            errorCode = "xtdb.mysql/binlog-refused",
            data = mapOf("position" to from.toString()),
            cause = e,
        )

    /**
     * The library's [BinaryLogClient] reads on a thread of its own, handing events over through [received];
     * the rest of the stream's state is the polling coroutine's alone.
     */
    private inner class BinlogStream(from: GtidSet, private val collations: Map<Int, Charset>) : MySqlDriver.ChangeStream {

        private val received = LinkedBlockingQueue<Received>(1024)

        @Volatile private var closed = false

        // written by the deserializer and read by the row deserializers, both on the client's thread
        private val tableMaps = HashMap<Long, TableMapEventData>()

        private val client = BinaryLogClient(hostname, port, username, password).also { client ->
            client.serverId = serverId

            // its keep-alive reconnects from a position the client tracks for itself — the only position is ours
            client.isKeepAlive = false
            client.gtidSet = from.toString()
            client.setThreadFactory { r -> Thread(r, "xtdb-mysql-binlog-$dbName").also { it.isDaemon = true } }
            client.setEventDeserializer(deserializer())

            client.registerEventListener { event -> hand(Received.Ev(event)) }
            client.registerLifecycleListener(object : BinaryLogClient.AbstractLifecycleListener() {
                override fun onCommunicationFailure(c: BinaryLogClient, ex: Exception) {
                    hand(Received.Failure(
                        if (ex is ServerException && ex.errorCode == ER_SOURCE_FATAL_ERROR_READING_BINLOG) binlogRefused(from, ex)
                        else connectionLost("binlog connection failed: ${ex.message}", ex)
                    ))
                }

                override fun onEventDeserializationFailure(c: BinaryLogClient, ex: Exception) {
                    // the client carries on past an event it can't read; queued ahead of whatever follows, this stops us at it
                    hand(Received.Failure(Fault("couldn't decode a binlog event: ${ex.message}", "xtdb.mysql/undecodable-event", emptyMap<String, Any>(), ex)))
                }

                override fun onDisconnect(c: BinaryLogClient) {
                    if (!closed) hand(Received.Failure(connectionLost("binlog connection closed")))
                }
            })
        }

        private fun hand(item: Received) {
            while (!closed && !received.offer(item, 100, TimeUnit.MILLISECONDS)) Unit
        }

        private fun deserializer(): EventDeserializer =
            object : EventDeserializer() {
                override fun nextEvent(input: ByteArrayInputStream): Event? =
                    super.nextEvent(input)?.also { event ->
                        when (event.getHeader<EventHeaderV4>().eventType) {
                            EventType.TABLE_MAP -> event.getData<TableMapEventData>().let { tableMaps[it.tableId] = it }
                            // a ROTATE with no timestamp is the one the server fakes on connect, not a new binlog file
                            EventType.ROTATE -> if (event.getHeader<EventHeaderV4>().timestamp != 0L) tableMaps.clear()
                            else -> Unit
                        }
                    }
            }.also { d ->
                d.setEventDataDeserializer(EventType.WRITE_ROWS, MySqlWriteRows(tableMaps))
                d.setEventDataDeserializer(EventType.EXT_WRITE_ROWS, MySqlWriteRows(tableMaps).setMayContainExtraInformation(true))
                d.setEventDataDeserializer(EventType.UPDATE_ROWS, MySqlUpdateRows(tableMaps))
                d.setEventDataDeserializer(EventType.EXT_UPDATE_ROWS, MySqlUpdateRows(tableMaps).setMayContainExtraInformation(true))
                d.setEventDataDeserializer(EventType.DELETE_ROWS, MySqlDeleteRows(tableMaps))
                d.setEventDataDeserializer(EventType.EXT_DELETE_ROWS, MySqlDeleteRows(tableMaps).setMayContainExtraInformation(true))
            }

        suspend fun connect() {
            try {
                runInterruptible(Dispatchers.IO) { client.connect(10_000) }
            } catch (e: Exception) {
                runCatching { client.disconnect() }
                throw when {
                    e is CancellationException -> e
                    e is ServerException && e.errorCode == ER_SOURCE_FATAL_ERROR_READING_BINLOG -> binlogRefused(position, e)
                    else -> connectionLost("couldn't open the binlog connection to $hostname:$port: ${e.message}", e)
                }
            }
        }

        private var position = from

        private var tx: OpenTx? = null

        /** By table-map id: the included tables' shapes, and null for a table we don't mirror. */
        private val defs = HashMap<Long, TableDef?>()

        override suspend fun poll(): MySqlDriver.Transaction.Committed? {
            while (currentCoroutineContext().isActive) {
                val item = runInterruptible(Dispatchers.IO) { received.poll(IDLE_POLL.inWholeMilliseconds, TimeUnit.MILLISECONDS) }
                    ?: return null

                when (item) {
                    is Received.Failure -> throw item.cause
                    is Received.Ev -> handle(item.event)?.let { return it }
                }
            }

            throw CancellationException("binlog polling cancelled")
        }

        private fun handle(event: Event): MySqlDriver.Transaction.Committed? {
            when (event.getHeader<EventHeaderV4>().eventType) {
                EventType.GTID -> {
                    check(tx == null) { "GTID event inside an unfinished transaction" }
                    val data = event.getData<GtidEventData>()
                    val gtid = data.mySqlGtid.let { Gtid(it.serverId, it.transactionId) }

                    // original: when it committed on the server that first ran it, should this be a replica
                    val commitMicros = data.originalCommitTimestamp.takeIf { it > 0 } ?: data.immediateCommitTimestamp
                    val commitTime = Instant.ofEpochSecond(commitMicros / 1_000_000, (commitMicros % 1_000_000) * 1000)

                    tx = OpenTx(gtid, commitTime, alreadyIndexed = gtid in position)
                }

                EventType.ANONYMOUS_GTID -> throw Incorrect(
                    "the upstream is writing anonymous transactions — gtid_mode must be ON",
                    errorCode = "xtdb.mysql/upstream-setting", data = mapOf("setting" to "gtid_mode"),
                )

                EventType.QUERY -> {
                    val current = tx ?: return null
                    val sql = event.getData<QueryEventData>().sql.trim()

                    when {
                        sql.equals("BEGIN", ignoreCase = true) -> current.began = true
                        sql.equals("COMMIT", ignoreCase = true) -> return commit(current)

                        // logged when a transaction rolls back after changing a non-transactional table
                        sql.equals("ROLLBACK", ignoreCase = true) ->
                            if (current.ops.isEmpty()) return commit(current)
                            else throw Unsupported(
                                "the upstream rolled back ${current.gtid} after logging row changes to included tables",
                                "xtdb.mysql/rolled-back-transaction", mapOf("gtid" to current.gtid.toString()),
                            )

                        sql.startsWith("XA ", ignoreCase = true) -> throw Unsupported(
                            "XA transactions aren't supported ($sql)", "xtdb.mysql/xa-transaction", mapOf("gtid" to current.gtid.toString()),
                        )

                        isStatementDml(sql) -> throw Incorrect(
                            "the binlog carries a statement-format change in ${current.gtid} ($sql) — binlog_format must be ROW for every session",
                            errorCode = "xtdb.mysql/upstream-setting",
                            data = mapOf("setting" to "binlog_format", "gtid" to current.gtid.toString()),
                        )

                        // DDL: a transaction of its own, which commits with no XID
                        !current.began -> return commit(current)
                    }
                }

                EventType.XID -> return tx?.let { commit(it) }

                EventType.TABLE_MAP -> {
                    val data = event.getData<TableMapEventData>()
                    defs[data.tableId] =
                        if (TableId(data.database, data.table) in tables) TableDef.fromTableMap(data) { collations[it] }
                        else null
                }

                EventType.WRITE_ROWS, EventType.EXT_WRITE_ROWS -> event.getData<WriteRowsEventData>().let { d ->
                    addOps(d.tableId, d.includedColumns) { def -> d.rows.map { RowOp.Put(def.id.database, def.id.table, def.fromBinlog(it)) } }
                }

                EventType.DELETE_ROWS, EventType.EXT_DELETE_ROWS -> event.getData<DeleteRowsEventData>().let { d ->
                    addOps(d.tableId, d.includedColumns) { def -> d.rows.map { RowOp.Delete(def.id.database, def.id.table, def.fromBinlog(it)) } }
                }

                EventType.UPDATE_ROWS, EventType.EXT_UPDATE_ROWS -> event.getData<UpdateRowsEventData>().let { d ->
                    addOps(d.tableId, d.includedColumnsBeforeUpdate, d.includedColumns) { def ->
                        d.rows.map { (before, after) -> RowOp.Update(def.id.database, def.id.table, def.fromBinlog(before), def.fromBinlog(after)) }
                    }
                }

                EventType.PARTIAL_UPDATE_ROWS_EVENT -> throw Incorrect(
                    "the binlog carries partial JSON updates — binlog_row_value_options must be empty",
                    errorCode = "xtdb.mysql/upstream-setting", data = mapOf("setting" to "binlog_row_value_options"),
                )

                EventType.TRANSACTION_PAYLOAD -> throw Unsupported(
                    "compressed transactions aren't supported — binlog_transaction_compression must be OFF",
                    "xtdb.mysql/compressed-transaction", mapOf("setting" to "binlog_transaction_compression"),
                )

                EventType.INCIDENT -> throw Fault(
                    "the upstream logged an incident — transactions may be missing from its binlog",
                    "xtdb.mysql/incident", emptyMap<String, Any>(),
                )

                else -> Unit
            }

            return null
        }

        private fun addOps(tableId: Long, vararg images: BitSet, ops: (TableDef) -> List<RowOp>) {
            val def = defs[tableId] ?: return

            // e.g. under a tagged GTID (MySQL 8.3+), whose event this source doesn't read
            val current = tx ?: throw Unsupported(
                "the binlog carries row changes to ${def.id} outside any transaction this source can read",
                "xtdb.mysql/unreadable-transaction", mapOf("table" to def.id.toString()),
            )

            if (current.alreadyIndexed) return

            if (images.any { it.cardinality() != def.columns.size })
                throw Incorrect(
                    "the binlog carries only some of ${def.id}'s columns — binlog_row_image must be FULL for every session",
                    errorCode = "xtdb.mysql/upstream-setting",
                    data = mapOf("setting" to "binlog_row_image", "table" to def.id.toString()),
                )

            current.ops.addAll(ops(def))
        }

        private fun commit(current: OpenTx): MySqlDriver.Transaction.Committed? {
            tx = null
            if (current.alreadyIndexed) return null

            position += current.gtid
            return MySqlDriver.Transaction.Committed(current.gtid, current.commitTime, current.ops, position)
        }

        override fun close() {
            closed = true
            runCatching { client.disconnect() }
            received.clear()
        }
    }

    override fun close() = Unit

    private fun isStatementDml(sql: String): Boolean =
        sql.takeWhile { !it.isWhitespace() }.uppercase() in setOf("INSERT", "UPDATE", "DELETE", "REPLACE", "LOAD")
}
