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
import xtdb.api.tx.TxIndexer
import xtdb.api.Remote
import xtdb.api.RemoteAlias
import xtdb.api.TransactionResult
import xtdb.api.tx.ExternalSource
import xtdb.api.tx.TxIndexer.TxResult
import xtdb.api.tx.ExternalSourceToken
import xtdb.api.error.Fault
import xtdb.api.error.Incorrect
import xtdb.postgres.proto.PostgresSourceConfig
import xtdb.postgres.proto.PostgresSourceToken
import xtdb.postgres.proto.postgresSourceConfig
import xtdb.postgres.proto.postgresSourceToken
import xtdb.postgres.PostgresSource.Assignment.Assigned
import xtdb.postgres.PostgresSource.Assignment.Unassigned
import xtdb.util.*
import java.time.Instant
import java.util.concurrent.atomic.AtomicReference
import com.google.protobuf.Any as ProtoAny

private val LOG = PostgresSource::class.logger

private const val PROTO_TAG_PREFIX = "proto.xtdb.com"

class PostgresSource(
    private val dbName: String,
    private val driver: PostgresDriver,
    private val slotName: String,
    private val indexer: PgIndexer,
    private val meterRegistry: MeterRegistry? = null,
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

    /**
     * Whether a partition is assigned to this node, and if so everything that assignment accumulates.
     *
     * The whole of the per-assignment state lives on [Assigned], so a new assignment is one swap that
     * starts all of it from scratch — nothing can carry the previous term's readings into this one.
     * [Assigned] is mutated in place by the poll loop that owns it, which is the sole writer.
     */
    private sealed interface Assignment {

        /** Another node holds the partition, so this one has no business querying the slot. */
        data object Unassigned : Assignment

        data class Assigned(
            /** True for the length of the replication stream; false while snapshotting. */
            @Volatile var streaming: Boolean = false,
            /** Epoch seconds of the latest applied commit; 0 until this assignment's first event. */
            @Volatile var lastEventEpochSeconds: Long = 0,
        ) : Assignment
    }

    private val assignment = AtomicReference<Assignment>(Unassigned)

    // Assigned-but-not-streaming still reports the lag: the slot stops advancing during the initial
    // snapshot, so a long snapshot is exactly when WAL piling up upstream matters.
    private fun walLagBytes(): Double =
        when (assignment.get()) {
            Unassigned -> Double.NaN
            is Assigned ->
                try {
                    driver.queryWalLagBytes()?.toDouble() ?: Double.NaN
                } catch (e: Exception) {
                    LOG.debug(e) { "[$dbName] Failed to query WAL lag" }
                    Double.NaN
                }
        }

    private val gauges: List<Gauge> = meterRegistry?.let { reg ->
        listOf(
            Gauge.builder("xtdb.postgres_source.last_event_time", assignment) {
                (it.get() as? Assigned)?.lastEventEpochSeconds?.toDouble() ?: 0.0
            }
                .description("epoch seconds of the most recently applied source commit")
                .baseUnit("seconds")
                .tags(tags)
                .register(reg),

            Gauge.builder("xtdb.postgres_source.connection_state", assignment) {
                if ((it.get() as? Assigned)?.streaming == true) 1.0 else 0.0
            }
                .description("1 if a replication stream is currently open, 0 otherwise")
                .tags(tags)
                .register(reg),

            Gauge.builder("xtdb.postgres_source.wal_lag_bytes", this) { it.walLagBytes() }
                .description("WAL bytes between pg_current_wal_lsn and our slot's confirmed_flush_lsn; NaN if we can't read it")
                .baseUnit("bytes")
                .tags(tags)
                .register(reg),
        )
    }.orEmpty()

    @Serializable
    @SerialName("!Postgres")
    data class Factory(
        val remote: RemoteAlias,
        val slotName: String,
        val publicationName: String,
        val indexer: PgIndexer.Factory,
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
                    // Configs persisted before `indexer` was a required field were all effectively using
                    // DirectMirror, so we default to it here — new databases must say which indexer they want.
                    indexer =
                        if (config.hasIndexer()) PgIndexer.Factory.fromProto(config.indexer)
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

        val assigned = Assigned().also { assignment.set(it) }
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
                    streamChanges(txIndexer, token.latestCommittedLsn, assigned)
                }
                else -> {
                    LOG.info("[$dbName] Starting initial snapshot")
                    val slotLsn = initialSnapshot(txIndexer)
                    LOG.info("[$dbName] Snapshot complete, switching to streaming from LSN ${LogSequenceNumber.valueOf(slotLsn)}")
                    streamChanges(txIndexer, slotLsn, assigned)
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
        } finally {
            assignment.set(Unassigned)
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

                    // Fire-and-forget: batches pipeline through the indexer, and the snapshot-complete marker
                    // below is an in-order durability barrier for all of them. A batch ingest failure surfaces
                    // on a later submit or on the marker's `executeTx`, aborting the snapshot.
                    txIndexer.submitTx(token) { openTx ->
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

                // `executeTx`, not `submitTx`: awaiting the marker's durability guarantees every batch before it
                // is durable too (the indexer settles in submission order), so the snapshot-complete token is
                // never durable ahead of the rows it marks complete.
                LOG.debug { "[$dbName] Writing snapshot-complete marker" }
                txIndexer.executeTx(completeToken) {
                    TxResult.Committed()
                }
            }

            return snapshot.slotLsn
        }
    }

    private suspend fun streamChanges(txIndexer: TxIndexer, startLsn: Long, assigned: Assigned) {
        driver.openStream(startLsn).use { stream ->
            assigned.streaming = true

            // Transactions submitted to the indexer but not yet known durable, in submission (= LSN) order. We
            // read + submit ahead of durability so back-to-back CDC txs pipeline through the double-buffered
            // indexer; `submitTx`'s bounded hand-off buffer suspends us under backpressure, keeping this bounded.
            val awaitingDurability = ArrayDeque<Pair<PostgresDriver.Transaction, Deferred<TransactionResult>>>()

            // Confirmation is specified in dev/doc/pgsrc.allium; the names below are its names.

            // held_lsn — opens at startLsn per SourceOpensStream, not at nothing.
            var heldLsn = startLsn

            // A lower bound on slot.confirmed_lsn, per SourceConfirmsPosition's @guidance.
            var confirmedLsn = 0L

            fun durableLsn(): Long? =
                txIndexer.latestBlock.value?.externalSourceToken
                    ?.let { PostgresSourceToken.parseFrom(it).latestCommittedLsn }

            fun confirmableLsn(): Long {
                val durable = durableLsn()
                return if (durable == null || durable >= heldLsn) stream.walEnd else durable
            }

            suspend fun confirm() {
                val lsn = confirmableLsn()
                if (lsn > confirmedLsn) {
                    stream.acknowledge(lsn)
                    confirmedLsn = lsn
                }
            }

            // Drains the completed prefix rather than awaiting the head, so a slow tx can't stall the poll
            // loop. `await()` rethrows an ingest failure, which unwinds past `use` — ImportFailureTearsDownStream.
            // The metrics land here so a re-delivered tx isn't counted twice.
            suspend fun drainApplied() {
                while (awaitingDurability.firstOrNull()?.second?.isCompleted == true) {
                    val (tx, handle) = awaitingDurability.removeFirst()
                    handle.await()
                    eventsCounter?.increment(tx.ops.size.toDouble())
                    commitsCounter?.increment()
                    assigned.lastEventEpochSeconds = tx.commitTime.epochSecond
                    commitLag?.record(
                        (Instant.now().toEpochMilli() - tx.commitTime.toEpochMilli()) / 1000.0,
                    )
                }
            }

            try {
                while (currentCoroutineContext().isActive) {
                    // SourceConfirmsPosition. Ahead of the first poll too: per ResendsFromConfirmed the
                    // re-delivery window is measured from the confirmed position, so the sooner the better.
                    confirm()

                    // SourceReceivesTransaction.
                    stream.poll()?.let { tx ->
                        if (tx.lsn <= startLsn) {
                            LOG.debug { "[$dbName] Skipping re-delivered tx at LSN ${LogSequenceNumber.valueOf(tx.lsn)} (<= resume LSN)" }
                            return@let
                        }

                        val token = postgresSourceToken {
                            latestCommittedLsn = tx.lsn
                            snapshotCompleted = true
                        }.toByteArray()

                        val handle = txIndexer.submitTx(token, systemTime = tx.commitTime) { openTx ->
                            indexer.indexTx(tx, openTx)
                            TxResult.Committed()
                        }
                        awaitingDurability.addLast(tx to handle)
                        heldLsn = tx.lsn
                    }

                    drainApplied()
                }
            } finally {
                assigned.streaming = false
            }
        }
    }

    override fun close() {
        LOG.info("[$dbName] Closing external source")
        // Before the driver goes: wal_lag_bytes queries it on scrape.
        meterRegistry?.let { reg ->
            (gauges + listOfNotNull(eventsCounter, commitsCounter, commitLag)).forEach { reg.remove(it) }
        }
        runCatching { indexer.close() }
        driver.close()
    }
}
