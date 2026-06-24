package xtdb.postgres

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import kotlinx.coroutines.*
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.modules.PolymorphicModuleBuilder
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.subclass
import org.postgresql.replication.LogSequenceNumber
import org.postgresql.util.PSQLException
import xtdb.indexer.TxIndexer
import xtdb.api.Remote
import xtdb.api.RemoteAlias
import xtdb.database.ExternalSource
import xtdb.indexer.TxIndexer.TxResult
import xtdb.database.ExternalSourceToken
import xtdb.error.Fault
import xtdb.error.Incorrect
import xtdb.postgres.proto.PostgresSourceConfig
import xtdb.postgres.proto.PostgresSourceToken
import xtdb.postgres.proto.postgresSourceConfig
import xtdb.postgres.proto.postgresSourceToken
import xtdb.util.*
import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import com.google.protobuf.Any as ProtoAny

private val LOG = PostgresSource::class.logger

private const val PROTO_TAG_PREFIX = "proto.xtdb.com"

class PostgresSource(
    private val dbName: String,
    private val driver: PostgresDriver,
    private val slotName: String,
    private val indexer: PgIndexer,
    meterRegistry: MeterRegistry? = null,
) : ExternalSource {

    private val tags = listOf(
        Tag.of("db", dbName),
        Tag.of("source", slotName),
        Tag.of("source_type", "postgres"),
    )

    private val eventsCounter: Counter? = meterRegistry?.let {
        Counter.builder("xtdb.postgres_source.events.total")
            .description("pgoutput insert/update/delete events ingested")
            .tags(tags)
            .register(it)
    }

    private val commitsCounter: Counter? = meterRegistry?.let {
        Counter.builder("xtdb.postgres_source.commits.total")
            .description("source transactions committed")
            .tags(tags)
            .register(it)
    }

    private val commitLag: DistributionSummary? = meterRegistry?.let {
        DistributionSummary.builder("xtdb.postgres_source.commit_lag_seconds")
            .description("wall-clock seconds between source commit and apply")
            .baseUnit("seconds")
            .publishPercentiles(0.5, 0.95, 0.99)
            .tags(tags)
            .register(it)
    }

    // epoch seconds of the latest applied commit; 0 until the first event
    private val lastEventEpochSeconds = AtomicLong(0)

    // 1 while a replication stream is open, 0 otherwise
    private val connectionState = AtomicInteger(0)

    // Last successful WAL-lag read; null until first query.
    // Retained on failure so a transient query blip doesn't reset the gauge to 0.
    @Volatile private var lastKnownWalLagBytes: Long? = null

    private fun walLagBytes(): Long? {
        try {
            driver.queryWalLagBytes()?.let { lastKnownWalLagBytes = it }
        } catch (e: Exception) {
            LOG.debug(e) { "[$dbName] Failed to query WAL lag" }
        }
        return lastKnownWalLagBytes
    }

    init {
        meterRegistry?.let { reg ->
            Gauge.builder("xtdb.postgres_source.last_event_time", lastEventEpochSeconds) { it.get().toDouble() }
                .description("epoch seconds of the most recently applied source commit")
                .baseUnit("seconds")
                .tags(tags)
                .register(reg)

            Gauge.builder("xtdb.postgres_source.connection_state", connectionState) { it.get().toDouble() }
                .description("1 if a replication stream is currently open, 0 otherwise")
                .tags(tags)
                .register(reg)

            Gauge.builder("xtdb.postgres_source.wal_lag_bytes", this) { walLagBytes()?.toDouble() ?: 0.0 }
                .description("WAL bytes between pg_current_wal_lsn and our slot's confirmed_flush_lsn")
                .baseUnit("bytes")
                .tags(tags)
                .register(reg)
        }
    }

    @Serializable
    @SerialName("!Postgres")
    data class Factory(
        val remote: RemoteAlias,
        val slotName: String,
        val publicationName: String,
        val indexer: PgIndexer.Factory = DirectMirror.Factory(),
    ) : ExternalSource.Factory {

        override fun open(
            dbName: String,
            remotes: Map<RemoteAlias, Remote>,
            meterRegistry: MeterRegistry?,
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
                slotName, publicationName,
            )

            return PostgresSource(dbName, driver, slotName, indexer.open(), meterRegistry)
        }

        class Registration : ExternalSource.Registration<Factory> {
            override val protoTag: String get() = "$PROTO_TAG_PREFIX/xtdb.postgres.proto.PostgresSourceConfig"

            override val factoryClass get() = Factory::class.java

            override fun toProto(factory: Factory): ProtoAny =
                ProtoAny.pack(postgresSourceConfig {
                    remote = factory.remote
                    slotName = factory.slotName
                    publicationName = factory.publicationName
                    indexer = PgIndexer.Factory.toProto(factory.indexer)
                }, PROTO_TAG_PREFIX)

            override fun fromProto(msg: ProtoAny): Factory {
                val config = msg.unpack(PostgresSourceConfig::class.java)
                return Factory(
                    remote = config.remote,
                    slotName = config.slotName,
                    publicationName = config.publicationName,
                    // absent on configs persisted before pluggable indexers landed
                    indexer = if (config.hasIndexer()) PgIndexer.Factory.fromProto(config.indexer)
                              else DirectMirror.Factory(),
                )
            }

            override fun registerSerde(builder: PolymorphicModuleBuilder<ExternalSource.Factory>) {
                builder.subclass(Factory::class)
            }

            override val serializersModule: SerializersModule = PgIndexer.Factory.serializersModule
        }
    }

    override suspend fun onPartitionAssigned(
        partition: Int,
        afterToken: ExternalSourceToken?,
        txIndexer: TxIndexer,
    ) {
        LOG.info("[$dbName] Partition $partition assigned (slot=$slotName)")

        val token = afterToken?.let { PostgresSourceToken.parseFrom(it) }
        LOG.debug { "[$dbName] Recovered token: ${token ?: "none"}" }

        if (!driver.publicationExists()) {
            throw Incorrect(
                "Publication does not exist on the upstream — create it before attaching the source",
                errorCode = "xtdb.postgres/missing-publication",
                data = mapOf("db-name" to dbName, "slot-name" to slotName),
            )
        }

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
            if (e.cause is java.net.SocketException && !currentCoroutineContext().isActive) {
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

    // pgjdbc reads ignore Thread.interrupt(); force-closing the resource is the
    // only way to unblock a parked socket read on coroutine cancellation.
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
                    val token = postgresSourceToken {
                        latestCommittedLsn = snapshot.slotLsn
                        snapshotCompleted = false
                    }.toByteArray()

                    txIndexer.indexTx(token) { openTx ->
                        // snapshot has no upstream commit time — use the tx's system-time
                        val snapshotTx = PostgresDriver.Transaction(snapshot.slotLsn, openTx.txKey.systemTime, batch)
                        indexer.indexTx(snapshotTx, openTx)
                        TxResult.Committed()
                    }
                }

                val completeToken = postgresSourceToken {
                    latestCommittedLsn = snapshot.slotLsn
                    snapshotCompleted = true
                }.toByteArray()

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
            connectionState.set(1)
            try {
                while (currentCoroutineContext().isActive) {
                    stream.nextTransaction { tx ->
                        val token = postgresSourceToken {
                            latestCommittedLsn = tx.lsn
                            snapshotCompleted = true
                        }.toByteArray()

                        txIndexer.indexTx(token, systemTime = tx.commitTime) { openTx ->
                            indexer.indexTx(tx, openTx)
                            TxResult.Committed()
                        }

                        // Record only on successful apply — failures will replay this tx.
                        eventsCounter?.increment(tx.ops.size.toDouble())
                        commitsCounter?.increment()
                        lastEventEpochSeconds.set(tx.commitTime.epochSecond)
                        commitLag?.record(
                            (Instant.now().toEpochMilli() - tx.commitTime.toEpochMilli()) / 1000.0,
                        )
                    }
                }
            } finally {
                connectionState.set(0)
            }
        }
    }

    override fun close() {
        LOG.info("[$dbName] Closing external source")
        runCatching { indexer.close() }
        driver.close()
    }
}
