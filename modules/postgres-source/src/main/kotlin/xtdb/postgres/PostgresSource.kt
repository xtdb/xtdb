package xtdb.postgres

import kotlinx.coroutines.*
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.modules.PolymorphicModuleBuilder
import kotlinx.serialization.modules.subclass
import org.postgresql.replication.LogSequenceNumber
import org.postgresql.util.PSQLException
import xtdb.indexer.TxIndexer
import xtdb.api.Remote
import xtdb.api.RemoteAlias
import xtdb.api.log.Log
import xtdb.api.log.LogClusterAlias
import xtdb.database.ExternalSource
import xtdb.indexer.OpenTx
import xtdb.indexer.TxIndexer.TxResult
import xtdb.database.ExternalSourceToken
import xtdb.database.proto.DatabaseConfig
import xtdb.error.Fault
import xtdb.error.Incorrect
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

class PostgresSource(
    private val dbName: String,
    private val driver: PostgresDriver,
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
                finally {
                    runCatching { closeable.close() }
                        .onFailure { LOG.warn(it, "[$dbName] Failed to force-close $closeable on cancellation") }
                }
            }
            try { block() }
            finally { watcher.cancel() }
        }

    private suspend fun initialSnapshot(txIndexer: TxIndexer): Long {
        driver.openSnapshot().use { snapshot ->
            closeOnCancel(snapshot) {
                for (batch in snapshot.batches()) {
                    val token = ProtoAny.pack(postgresSourceToken {
                        latestCommittedLsn = snapshot.slotLsn
                        snapshotCompleted = false
                    }, PROTO_TAG)

                    txIndexer.indexTx(token) { openTx ->
                        for (op in batch) {
                            writeOp(openTx, dbName, op)
                        }
                        TxResult.Committed()
                    }
                }

                val completeToken = ProtoAny.pack(postgresSourceToken {
                    latestCommittedLsn = snapshot.slotLsn
                    snapshotCompleted = true
                }, PROTO_TAG)

                LOG.debug { "[$dbName] Writing snapshot-complete marker" }
                txIndexer.indexTx(completeToken) {
                    TxResult.Committed()
                }
            }

            return snapshot.slotLsn
        }
    }

    private suspend fun streamChanges(txIndexer: TxIndexer, startLsn: Long) {
        driver.openStream(startLsn).use { stream ->
            closeOnCancel(stream) {
                while (currentCoroutineContext().isActive) {
                    stream.nextTransaction { tx ->
                        val token = ProtoAny.pack(postgresSourceToken {
                            latestCommittedLsn = tx.lsn
                            snapshotCompleted = true
                        }, PROTO_TAG)

                        txIndexer.indexTx(token, systemTime = tx.commitTime) { openTx ->
                            for (op in tx.ops) {
                                writeOp(openTx, dbName, op)
                            }
                            TxResult.Committed()
                        }
                    }
                }
            }
        }
    }

    override fun close() {
        LOG.info("[$dbName] Closing external source")
        driver.close()
    }
}

private fun writeOp(
    openTx: OpenTx,
    dbName: String,
    op: RowOp,
) {
    val openTxTable = openTx.table(TableRef(dbName, op.schema, op.table))

    when (op) {
        is RowOp.Put -> {
            val docMap = op.row.toMutableMap()

            val id = docMap["_id"]
                ?: throw Incorrect("Missing '_id' in row from ${op.schema}.${op.table}")

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

        is RowOp.Delete -> {
            val id = op.row["_id"]
                ?: throw Incorrect("Missing '_id' in delete on ${op.schema}.${op.table}")

            openTxTable.logDelete(
                ByteBuffer.wrap(id.asIid),
                openTx.systemFrom,
                Long.MAX_VALUE,
            )
        }
    }
}
