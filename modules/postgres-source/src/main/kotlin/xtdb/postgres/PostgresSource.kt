package xtdb.postgres

import kotlinx.coroutines.*
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.modules.PolymorphicModuleBuilder
import kotlinx.serialization.modules.subclass
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.transaction.TransactionIsolationLevel.REPEATABLE_READ
import org.postgresql.PGConnection
import org.postgresql.replication.LogSequenceNumber
import org.postgresql.replication.PGReplicationStream
import org.postgresql.util.PSQLException
import xtdb.indexer.TxIndexer
import xtdb.api.Remote
import xtdb.api.RemoteAlias
import xtdb.api.log.Log
import xtdb.api.log.LogClusterAlias
import xtdb.database.ExternalSource
import xtdb.indexer.TxIndexer.OpenTx
import xtdb.indexer.TxIndexer.TxResult
import xtdb.database.ExternalSourceToken
import xtdb.database.proto.DatabaseConfig
import xtdb.error.Fault
import xtdb.error.Incorrect
import xtdb.postgres.PgOutputMessage.ColumnValue
import xtdb.postgres.proto.PostgresSourceConfig
import xtdb.postgres.proto.PostgresSourceToken
import xtdb.postgres.proto.postgresSourceConfig
import xtdb.postgres.proto.postgresSourceToken
import xtdb.table.TableRef
import xtdb.time.InstantUtil.asMicros
import xtdb.util.*
import java.nio.ByteBuffer
import java.time.Instant
import com.google.protobuf.Any as ProtoAny

private val LOG = PostgresSource::class.logger

private const val PROTO_TAG = "proto.xtdb.com"
private const val SNAPSHOT_BATCH_SIZE = 1000

class PostgresSource(
    private val dbName: String,
    private val driver: PgWireDriver,
    private val slotName: String,
) : ExternalSource {

    @Serializable
    @SerialName("!Postgres")
    data class Factory(
        val remote: RemoteAlias,
        val slotName: String,
        val publicationName: String,
        val schemaIncludeList: List<String> = listOf("public"),
    ) : ExternalSource.Factory {

        override fun open(
            dbName: String,
            clusters: Map<LogClusterAlias, Log.Cluster>,
            remotes: Map<RemoteAlias, Remote>,
        ): ExternalSource {
            val raw = remotes[remote]
                ?: throw Incorrect(
                    "no remote configured with alias '$remote' — add a '!Postgres' entry under 'remotes:' in node config",
                    errorCode = "xtdb.postgres/missing-remote",
                    data = mapOf("alias" to remote),
                )

            val actualType = raw::class.simpleName ?: raw::class.qualifiedName ?: "unknown"

            val pg = raw as? PostgresRemote
                ?: throw Incorrect(
                    "remote '$remote' is a $actualType, expected a !Postgres remote",
                    errorCode = "xtdb.postgres/wrong-remote-type",
                    data = mapOf("alias" to remote, "actualType" to actualType),
                )

            val driver = PgWireDriver(
                dbName, pg.hostname, pg.port, pg.database, pg.username, pg.password,
                slotName, publicationName, schemaIncludeList,
            )

            return PostgresSource(dbName, driver, slotName)
        }

        override fun writeTo(dbConfig: DatabaseConfig.Builder) {
            dbConfig.externalSource = ProtoAny.pack(postgresSourceConfig {
                remote = this@Factory.remote
                slotName = this@Factory.slotName
                publicationName = this@Factory.publicationName
                schemaIncludeList += this@Factory.schemaIncludeList
            }, PROTO_TAG)
        }

        class Registration : ExternalSource.Registration {
            override val protoTag: String get() = "$PROTO_TAG/xtdb.postgres.proto.PostgresSourceConfig"

            override fun fromProto(msg: ProtoAny): ExternalSource.Factory {
                val config = msg.unpack(PostgresSourceConfig::class.java)
                return Factory(
                    remote = config.remote,
                    slotName = config.slotName,
                    publicationName = config.publicationName,
                    schemaIncludeList = config.schemaIncludeListList,
                )
            }

            override fun registerSerde(builder: PolymorphicModuleBuilder<ExternalSource.Factory>) {
                builder.subclass(Factory::class)
            }
        }
    }

    override suspend fun onPartitionAssigned(
        partition: Int,
        afterToken: ExternalSourceToken?,
        txIndexer: TxIndexer,
    ) {
        LOG.info("[$dbName] Partition $partition assigned (slot=$slotName)")

        val token = afterToken?.unpack(PostgresSourceToken::class.java)
        LOG.debug { "[$dbName] Recovered token: ${token ?: "none"}" }

        try {
            when {
                token != null && !token.snapshotCompleted ->
                    // > The snapshot is valid until a new command is executed on this connection or the replication connection is closed
                    // https://www.postgresql.org/docs/current/protocol-replication.html#PROTOCOL-REPLICATION-CREATE-REPLICATION-SLOT
                    // Therefore it is impossible to resume a snapshot, meaning if we receive a previous incomplete snapshot we must mark the database inoperable
                    // The only recovery is to clear the topics & object store and try the snapshot again
                    throw Fault(
                        "Incomplete snapshot — database is inoperable",
                        "xtdb.postgres/incomplete-snapshot",
                        mapOf("db-name" to dbName, "slot-name" to slotName),
                    )
                token != null && token.snapshotCompleted -> {
                    LOG.info("[$dbName] Resuming streaming from LSN ${LogSequenceNumber.valueOf(token.latestCommittedLsn)}")
                    streamChanges(txIndexer, token.latestCommittedLsn)
                }
                else -> {
                    LOG.info("[$dbName] Starting initial snapshot")
                    val slotLsn = initialSnapshot(txIndexer)
                    LOG.info("[$dbName] Snapshot complete, switching to streaming from LSN ${LogSequenceNumber.valueOf(slotLsn)}")
                    streamChanges(txIndexer, slotLsn)
                }
            }
        } catch (e: PSQLException) {
            if (e.cause is java.net.SocketException) {
                LOG.warn("[$dbName] Database connection failed when reading from copy (connection closed)")
            } else {
                LOG.error(e, "[$dbName] External source failed")
                throw e
            }
        } catch (e: Exception) {
            LOG.error(e, "[$dbName] External source failed")
            throw e
        }
    }

    /**
     * Runs [block] with a child coroutine that force-closes [closeable] on cancellation.
     *
     * pgjdbc's socket reads don't respond to Thread.interrupt(), so coroutine
     * cancellation alone can't unblock them. The child coroutine watches for
     * cancellation and closes the resource, causing the blocked read to throw.
     */
    private suspend fun <T : AutoCloseable, R> closeOnCancel(closeable: T, block: suspend () -> R): R =
        coroutineScope {
            val watcher = launch {
                try { awaitCancellation() }
                finally { runCatching { closeable.close() } }
            }
            try { block() }
            finally { watcher.cancel() }
        }

    /**
     * Returns the slot LSN for streaming to resume from.
     */
    private suspend fun initialSnapshot(txIndexer: TxIndexer): Long {
        LOG.debug { "[$dbName] Opening replication connection" }

        driver.openReplicationConnection().use { replConn ->
            return closeOnCancel(replConn) {
                val pgReplConn = replConn.unwrap(PGConnection::class.java)

                LOG.debug { "[$dbName] Creating replication slot '$slotName' with pgoutput" }

                // Create replication slot and get exported snapshot
                val slotInfo = pgReplConn.replicationAPI
                    .createReplicationSlot()
                    .logical()
                    .withSlotName(slotName)
                    .withOutputPlugin("pgoutput")
                    .make()

                val snapshotName = slotInfo.snapshotName
                val slotLsn = slotInfo.consistentPoint.asLong()

                LOG.info("[$dbName] Created slot '$slotName' at LSN ${LogSequenceNumber.valueOf(slotLsn)}, snapshot=$snapshotName")

                // Read tables using the exported snapshot for consistency
                driver.jdbi.open().use { handle ->
                    handle.begin()
                    try {
                        handle.transactionIsolationLevel = REPEATABLE_READ

                        LOG.debug { "[$dbName] SET TRANSACTION SNAPSHOT '$snapshotName'" }
                        handle.execute("SET TRANSACTION SNAPSHOT '$snapshotName'")

                        for ((schema, table, batch) in readSnapshotBatches(handle)) {
                            flushSnapshotBatch(txIndexer, slotLsn, schema, table, batch)
                        }

                        // Mark snapshot complete
                        val completeToken = ProtoAny.pack(postgresSourceToken {
                            latestCommittedLsn = slotLsn
                            snapshotCompleted = true
                        }, PROTO_TAG)

                        LOG.debug { "[$dbName] Writing snapshot-complete marker" }
                        txIndexer.indexTx(completeToken) {
                            TxResult.Committed()
                        }

                        slotLsn
                    } finally {
                        handle.rollback()
                    }
                }
            }
        }
    }

    private fun readSnapshotBatches(handle: Handle): Sequence<Triple<String, String, List<Map<String, Any?>>>> = sequence {
        val tables = with(driver) { handle.discoverTables() }
        LOG.info("[$dbName] Discovered ${tables.size} tables: ${tables.joinToString { "${it.first}.${it.second}" }}")

        for ((schema, table) in tables) {
            val columns = with(driver) { handle.discoverColumns(schema, table) }
            val fullTableName = "$schema.$table"

            LOG.info("[$dbName] Snapshotting $fullTableName (${columns.size} columns: ${columns.joinToString { it.name }})")

            var rowCount = 0
            handle.createQuery("SELECT * FROM \"$schema\".\"$table\"")
                .setFetchSize(SNAPSHOT_BATCH_SIZE)
                .map { rs, _ -> columns.associate { col -> col.name to rs.getObject(col.name) } }
                .iterator().use { iter ->
                    iter.asSequence()
                        .chunked(SNAPSHOT_BATCH_SIZE)
                        .forEach { batch ->
                            rowCount += batch.size
                            LOG.debug { "[$dbName] Flushing $fullTableName batch: ${batch.size} rows (total: $rowCount)" }
                            yield(Triple(schema, table, batch))
                        }
                }

            LOG.info("[$dbName] Finished snapshotting $fullTableName ($rowCount rows)")
        }
    }

    private suspend fun streamChanges(txIndexer: TxIndexer, startLsn: Long) {
        LOG.debug { "[$dbName] Opening replication connection for streaming" }

        driver.openReplicationConnection().use { replConn ->
            closeOnCancel(replConn) {
                val pgReplConn = replConn.unwrap(PGConnection::class.java)

                LOG.info("[$dbName] Starting replication stream from LSN ${LogSequenceNumber.valueOf(startLsn)} on slot '$slotName'")

                val stream: PGReplicationStream = driver.startReplicationStream(pgReplConn, startLsn)

                val relations = mutableMapOf<Int, PgOutputMessage.Relation>()

                // Accumulated operations within a single PG transaction
                var currentTxOps = mutableListOf<Pair<PgOutputMessage.Relation, PgOutputMessage>>()

                while (currentCoroutineContext().isActive) {
                    // read() blocks until a message is available; runInterruptible
                    // lets coroutine cancellation interrupt the blocking call.
                    val msg = runInterruptible(Dispatchers.IO) {
                        stream.read()!!
                    }

                    val parsed = PgOutputMessage.parse(msg)
                    LOG.trace { "[$dbName] Received ${parsed::class.simpleName}" }

                    when (parsed) {
                        is PgOutputMessage.Relation -> {
                            relations[parsed.relationId] = parsed
                            LOG.debug { "[$dbName] Relation: ${parsed.schema}.${parsed.table} (id=${parsed.relationId}, ${parsed.columns.size} columns)" }
                        }

                        is PgOutputMessage.Begin -> {
                            LOG.trace { "[$dbName] Begin tx (finalLsn=${LogSequenceNumber.valueOf(parsed.finalLsn)})" }
                            currentTxOps = mutableListOf()
                        }

                        is PgOutputMessage.Insert, is PgOutputMessage.Update, is PgOutputMessage.Delete -> {
                            val relationId = when (parsed) {
                                is PgOutputMessage.Insert -> parsed.relationId
                                is PgOutputMessage.Update -> parsed.relationId
                                is PgOutputMessage.Delete -> parsed.relationId
                            }
                            val relation = relations[relationId]
                                ?: error("Relation $relationId not found — missing Relation message before data message")

                            LOG.trace { "[$dbName] ${parsed::class.simpleName} on ${relation.schema}.${relation.table}" }
                            currentTxOps.add(relation to parsed)
                        }

                        is PgOutputMessage.Commit -> {
                            val commitLsn = LogSequenceNumber.valueOf(parsed.endLsn)

                            if (currentTxOps.isNotEmpty()) {
                                val commitTime = pgTimestampToInstant(parsed.commitTimestamp)
                                LOG.debug { "[$dbName] Commit at $commitLsn: ${currentTxOps.size} ops, commitTime=$commitTime" }

                                val token = ProtoAny.pack(postgresSourceToken {
                                    latestCommittedLsn = parsed.endLsn
                                    snapshotCompleted = true
                                }, PROTO_TAG)

                                val ops = currentTxOps.toList()

                                txIndexer.indexTx(token, systemTime = commitTime) { openTx ->
                                    for ((relation, op) in ops) {
                                        applyStreamingOp(openTx, dbName, relation, op)
                                    }
                                    TxResult.Committed()
                                }
                            } else {
                                LOG.trace { "[$dbName] Empty commit at $commitLsn (keepalive)" }
                            }

                            // Acknowledge up to the commit LSN
                            LOG.trace { "[$dbName] Acknowledging LSN $commitLsn" }
                            runInterruptible(Dispatchers.IO) {
                                stream.setFlushedLSN(commitLsn)
                                stream.setAppliedLSN(commitLsn)
                                stream.forceUpdateStatus()
                            }

                            currentTxOps = mutableListOf()
                        }
                    }
                }

                LOG.info("[$dbName] Streaming loop exiting (coroutine no longer active)")
            }
        }
    }

    private fun applyStreamingOp(
        openTx: OpenTx,
        dbName: String,
        relation: PgOutputMessage.Relation,
        op: PgOutputMessage,
    ) {
        when (op) {
            is PgOutputMessage.Insert -> {
                val row = toRowMap(relation, op.values)
                writeRow(openTx, dbName, relation.schema, relation.table, "c", row, null)
            }

            is PgOutputMessage.Update -> {
                val row = toRowMap(relation, op.newValues)
                writeRow(openTx, dbName, relation.schema, relation.table, "u", row, null)
            }

            is PgOutputMessage.Delete -> {
                val row = toRowMap(relation, op.oldValues)
                writeRow(openTx, dbName, relation.schema, relation.table, "d", null, row)
            }

            is PgOutputMessage.Relation, is PgOutputMessage.Begin, is PgOutputMessage.Commit ->
                error("Unexpected op type in applyStreamingOp: ${op::class.simpleName}")
        }
    }

    private fun toRowMap(relation: PgOutputMessage.Relation, values: List<ColumnValue>): Map<String, Any?> =
        relation.columns
            .mapIndexedNotNull { idx, col ->
                values.getOrNull(idx)?.let { colValue ->
                    col.name to when (colValue) {
                        is ColumnValue.Null -> null
                        is ColumnValue.Unchanged ->
                            throw Incorrect(
                                buildString {
                                    appendLine("Received unchanged TOASTed column '${col.name}' on ${relation.schema}.${relation.table}. ")
                                    appendLine("Set REPLICA IDENTITY FULL on the source table: ")
                                    appendLine("ALTER TABLE \"${relation.schema}\".\"${relation.table}\" REPLICA IDENTITY FULL")
                                }
                            )

                        is ColumnValue.Text -> PgTypeCoercion.coerce(colValue.value, col.typeOid)
                    }
                }
            }
            .toMap()

    override fun close() {
        LOG.info("[$dbName] Closing external source")
    }

    private suspend fun flushSnapshotBatch(
        txIndexer: TxIndexer,
        slotLsn: Long,
        schema: String,
        table: String,
        rows: List<Map<String, Any?>>,
    ) {
        val token = ProtoAny.pack(postgresSourceToken {
            latestCommittedLsn = slotLsn
            snapshotCompleted = false
        }, PROTO_TAG)

        txIndexer.indexTx(token) { openTx ->
            for (row in rows) {
                writeRow(openTx, dbName, schema, table, "r", row, null)
            }
            TxResult.Committed()
        }
    }

    companion object {
        // PG epoch is 2000-01-01T00:00:00Z, timestamps are in microseconds
        private val PG_EPOCH = Instant.parse("2000-01-01T00:00:00Z")

        fun pgTimestampToInstant(pgMicros: Long): Instant =
            PG_EPOCH.plusNanos(pgMicros * 1000)
    }
}

private fun writeRow(
    openTx: OpenTx,
    dbName: String,
    schema: String,
    table: String,
    op: String,
    after: Map<String, Any?>?,
    before: Map<String, Any?>?,
) {
    val openTxTable = openTx.table(TableRef(dbName, schema, table))

    when (op) {
        "c", "r", "u" -> {
            requireNotNull(after) { "Missing row data for $op operation" }
            val docMap = after.toMutableMap()

            val id = docMap["_id"] ?: throw Incorrect("Missing '_id' in row from $schema.$table")

            val explicitValidFrom = (docMap.remove("_valid_from") as? Instant)?.asMicros
            val explicitValidTo = (docMap.remove("_valid_to") as? Instant)?.asMicros

            if (explicitValidTo != null && explicitValidFrom == null)
                throw Incorrect("'_valid_to' requires '_valid_from'")

            openTxTable.logPut(
                ByteBuffer.wrap(id.asIid),
                explicitValidFrom ?: openTx.systemFrom,
                explicitValidTo ?: Long.MAX_VALUE,
            ) { openTxTable.docWriter.writeObject(docMap) }
        }

        "d" -> {
            requireNotNull(before) { "Missing 'before' data for delete — check REPLICA IDENTITY on source table" }
            val id = before["_id"] ?: throw Incorrect("Missing '_id' in 'before' for delete on $schema.$table")

            openTxTable.logDelete(
                ByteBuffer.wrap(id.asIid),
                openTx.systemFrom,
                Long.MAX_VALUE,
            )
        }

        else -> throw Incorrect("Unknown CDC op: '$op'")
    }
}
