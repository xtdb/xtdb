package xtdb.mysql

import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.*
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.modules.PolymorphicModuleBuilder
import kotlinx.serialization.modules.subclass
import xtdb.api.Remote
import xtdb.api.RemoteAlias
import xtdb.api.TransactionResult
import xtdb.api.error.Fault
import xtdb.api.error.Incorrect
import xtdb.api.tx.ExternalSource
import xtdb.api.tx.ExternalSourceToken
import xtdb.api.tx.TxIndexer
import xtdb.api.tx.TxIndexer.TxResult
import xtdb.mysql.proto.MySqlSourceConfig
import xtdb.mysql.proto.MySqlSourceToken
import xtdb.mysql.proto.mySqlSourceConfig
import xtdb.mysql.proto.mySqlSourceToken
import xtdb.util.*
import com.google.protobuf.Any as ProtoAny

private val LOG = MySqlSource::class.logger

private const val PROTO_TAG_PREFIX = "proto.xtdb.com"

private const val MAX_SERVER_ID = 4_294_967_295L

class MySqlSource(
    private val dbName: String,
    private val driver: MySqlDriver,
) : ExternalSource {

    @Serializable
    @SerialName("!MySQL")
    data class Factory(
        val remote: RemoteAlias,
        val serverId: Long,
        val tables: List<String>,
    ) : ExternalSource.Factory {

        override fun open(
            dbName: String,
            remotes: Map<RemoteAlias, Remote>,
            meterRegistry: MeterRegistry?,
        ): ExternalSource {
            val raw = remotes[remote]
                ?: throw Incorrect(
                    "no remote configured with alias '$remote' — add a '!MySQL' entry under 'remotes:' in node config",
                    errorCode = "xtdb.mysql/missing-remote",
                    data = mapOf("alias" to remote),
                )

            val actualType = raw::class.simpleName ?: raw::class.qualifiedName ?: "unknown"

            val mysql = raw as? MySqlRemote
                ?: throw Incorrect(
                    "remote '$remote' is a $actualType, expected a !MySQL remote",
                    errorCode = "xtdb.mysql/wrong-remote-type",
                    data = mapOf("alias" to remote, "actualType" to actualType),
                )

            if (serverId !in 1..MAX_SERVER_ID)
                throw Incorrect(
                    "serverId must be between 1 and $MAX_SERVER_ID, and unique among everything replicating from the upstream — got $serverId",
                    errorCode = "xtdb.mysql/invalid-server-id",
                    data = mapOf("server-id" to serverId),
                )

            if (tables.isEmpty())
                throw Incorrect(
                    "'tables' is empty — list the upstream tables to mirror, as 'database.table'",
                    errorCode = "xtdb.mysql/no-tables",
                )

            val driver = MySqlWireDriver(
                dbName, mysql.hostname, mysql.port, mysql.username, mysql.password,
                serverId, tables.map(TableId::parse).toSet(),
            )

            return MySqlSource(dbName, driver)
        }

        class Registration : ExternalSource.Registration<Factory> {
            override val protoTag: String get() = "$PROTO_TAG_PREFIX/xtdb.mysql.proto.MySqlSourceConfig"

            override val factoryClass get() = Factory::class.java

            override fun toProto(factory: Factory): ProtoAny =
                ProtoAny.pack(mySqlSourceConfig {
                    remote = factory.remote
                    serverId = factory.serverId
                    tables.addAll(factory.tables)
                }, PROTO_TAG_PREFIX)

            override fun fromProto(msg: ProtoAny): Factory {
                val config = msg.unpack(MySqlSourceConfig::class.java)
                return Factory(remote = config.remote, serverId = config.serverId, tables = config.tablesList.toList())
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
        LOG.info("[$dbName] Partition $partition assigned")

        val token = afterToken?.let { MySqlSourceToken.parseFrom(it) }
        LOG.debug { "[$dbName] Recovered token: ${token ?: "none"}" }

        try {
            runInterruptible(Dispatchers.IO) { driver.checkUpstream() }

            val resumeFrom = when {
                token != null && !token.snapshotCompleted ->
                    // The snapshot was a transaction on a connection that went with the term, and a retry couldn't tell
                    // which rows were deleted since: the only recovery is to clear the topics & object store and snapshot again.
                    throw Fault(
                        "Incomplete snapshot — database is inoperable",
                        "xtdb.mysql/incomplete-snapshot",
                        mapOf("db-name" to dbName),
                    )

                token != null -> GtidSet.parse(token.gtidSet)
                    .also { LOG.info("[$dbName] Resuming streaming after GTID set '$it'") }

                else -> {
                    LOG.info("[$dbName] Starting initial snapshot")
                    initialSnapshot(txIndexer)
                        .also { LOG.info("[$dbName] Snapshot complete, switching to streaming after GTID set '$it'") }
                }
            }

            streamChanges(txIndexer, resumeFrom)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            LOG.error(e, "[$dbName] External source failed")
            throw e
        }
    }

    // a JDBC read parked on the socket ignores interruption; closing what it reads is the only way to unblock it
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

    private fun token(position: GtidSet, snapshotCompleted: Boolean) =
        mySqlSourceToken {
            gtidSet = position.toString()
            this.snapshotCompleted = snapshotCompleted
        }.toByteArray()

    private suspend fun initialSnapshot(txIndexer: TxIndexer): GtidSet {
        runInterruptible(Dispatchers.IO) { driver.openSnapshot() }.use { snapshot ->
            closeOnCancel(snapshot) {
                val batchToken = token(snapshot.position, snapshotCompleted = false)

                for (batch in snapshot.batches()) {
                    // Fire-and-forget: batches pipeline through the indexer, and the snapshot-complete marker
                    // below is an in-order durability barrier for all of them. Discarding the handle is safe
                    // because a batch ingest failure fails the leader term, which stands this source down —
                    // so the snapshot aborts whether or not anything was awaiting that batch.
                    txIndexer.submitTx(batchToken) { openTx ->
                        DirectMirror.indexTx(MySqlDriver.Transaction.Snapshot(batch, snapshot.position), openTx)
                        TxResult.Committed()
                    }
                }

                // `executeTx`, not `submitTx`: awaiting the marker's durability guarantees every batch before it
                // is durable too (the indexer settles in submission order), so the snapshot-complete token is
                // never durable ahead of the rows it marks complete.
                LOG.debug { "[$dbName] Writing snapshot-complete marker" }
                txIndexer.executeTx(token(snapshot.position, snapshotCompleted = true)) { TxResult.Committed() }
            }

            return snapshot.position
        }
    }

    private suspend fun streamChanges(txIndexer: TxIndexer, from: GtidSet) {
        // Submitted to the indexer but not yet applied, in submission (= commit) order. We submit ahead
        // so back-to-back CDC txs pipeline through the double-buffered indexer; `submitTx`'s bounded
        // hand-off buffer suspends us under backpressure, keeping this bounded.
        val awaitingApply = ArrayDeque<Deferred<TransactionResult>>()

        driver.openStream(from).use { stream ->
            while (currentCoroutineContext().isActive) {
                // Every upstream transaction is an XT one, those with no included rows too: its token is what keeps a
                // restart clear of binlog the upstream has since purged.
                stream.poll()?.let { tx ->
                    awaitingApply.addLast(
                        txIndexer.submitTx(token(tx.position, snapshotCompleted = true), systemTime = tx.commitTime) { openTx ->
                            DirectMirror.indexTx(tx, openTx)
                            TxResult.Committed()
                        }
                    )
                }

                // Drains the completed prefix rather than awaiting the head, so a slow tx can't stall the poll loop.
                // `await()` rethrows an ingest failure, which closes the stream without having moved past anything.
                while (awaitingApply.firstOrNull()?.isCompleted == true) awaitingApply.removeFirst().await()
            }

            throw CancellationException("[$dbName] Streaming stood down")
        }
    }

    override fun close() {
        LOG.info("[$dbName] Closing external source")
        driver.close()
    }
}
