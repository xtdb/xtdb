package xtdb.database

import clojure.lang.*
import kotlinx.coroutines.*
import kotlinx.coroutines.time.withTimeout
import xtdb.util.logger
import xtdb.util.warn
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.decodeFromString
import org.apache.arrow.memory.BufferAllocator
import xtdb.NodeBase
import xtdb.api.TransactionResult
import xtdb.api.Xtdb
import xtdb.api.YAML_SERDE
import xtdb.api.log.IngestionStoppedException
import xtdb.api.log.Log
import xtdb.api.log.LogOffset
import xtdb.api.log.MessageId
import xtdb.api.log.ReplicaMessage
import xtdb.api.log.SourceMessage
import xtdb.api.log.Watchers
import xtdb.api.storage.Storage
import xtdb.api.storage.Storage.applyStorage
import xtdb.arrow.VectorType
import xtdb.catalog.BlockCatalog
import xtdb.catalog.TableCatalog
import xtdb.compactor.Compactor
import xtdb.database.proto.DatabaseConfig
import xtdb.database.proto.DatabaseMode
import xtdb.error.Incorrect
import xtdb.indexer.*
import xtdb.metadata.PageMetadata
import xtdb.query.IQuerySource
import xtdb.storage.BufferPool
import xtdb.table.DatabaseName
import xtdb.table.TableRef
import xtdb.trie.ColumnName
import xtdb.trie.TrieCatalog
import xtdb.tx.TxOp
import xtdb.tx.TxOpts
import xtdb.tx.serializeUserMetadata
import xtdb.tx.toArrowBytes
import xtdb.util.MsgIdUtil.msgIdToEpoch
import xtdb.util.MsgIdUtil.msgIdToOffset
import xtdb.util.MsgIdUtil.offsetToMsgId
import xtdb.util.closeAll
import xtdb.util.info
import xtdb.util.safelyOpening
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import java.time.Duration
import java.util.*
import kotlin.concurrent.Volatile

private val LOG = Database::class.logger

class Database(
    val allocator: BufferAllocator,
    val config: Config,
    override val storage: DatabaseStorage,
    val isIndexing: Boolean,
    val partitions: Map<Int, DatabasePartition>,
    private val meterRegistry: MeterRegistry?,
    private val job: Job? = null,
    private val registeredGauges: List<Gauge> = emptyList(),
) : IQuerySource.QueryDatabase, AutoCloseable {

    init {
        require(partitions.isNotEmpty()) { "Database must have at least one partition" }
    }

    // Single-partition delegate. Per the stage-4 design, this becomes a per-partition
    // lookup once `partitions > 1` is enabled; for now every Database has exactly one.
    private val partition0: DatabasePartition get() = partitions.getValue(0)

    val name: DatabaseName get() = partition0.state.name
    override val queryState: DatabaseState get() = partition0.state
    val watchers: Watchers get() = partition0.watchers
    val compactorOrNull: Compactor.ForDatabase? get() = partition0.compactorOrNull

    override fun openSnapshot(): DatabaseSnapshot =
        // List position = partition index, so basis-vector slot N matches `partitions[N]`.
        DatabaseSnapshot(partitions.entries.sortedBy { it.key }.map { it.value.openSnapshot() })

    val blockCatalog: BlockCatalog get() = partition0.blockCatalog
    val tableCatalog: TableCatalog get() = partition0.tableCatalog

    fun getColumnTypes(table: TableRef, snap: DatabaseSnapshot): Map<ColumnName, VectorType>? {
        val historical = tableCatalog.getTypes(table)
        // TODO (#5557 unit 7) iterate `snap.partitions` and merge per-partition live types;
        // for now single-partition is the only allowed shape, so pick the only Snapshot.
        val live = snap.partitions.first().allColumnTypes[table]
        return if (historical == null && live == null) null
        else (historical ?: emptyMap()) + (live ?: emptyMap())
    }
    val trieCatalog: TrieCatalog get() = partition0.trieCatalog
    val liveIndex: LiveIndex get() = partition0.liveIndex

    val sourceLog: Log<SourceMessage> get() = storage.sourceLog
    val replicaLog: Log<ReplicaMessage> get() = storage.replicaLog
    val bufferPool: BufferPool get() = storage.bufferPool
    val metadataManager: PageMetadata.Factory get() = storage.metadataManager

    val compactor: Compactor.ForDatabase get() = partition0.compactor

    val latestProcessedMsgId: MessageId get() = watchers.latestSourceMsgId
    val ingestionError: IngestionStoppedException? get() = watchers.exception

    @Volatile
    var isClosing: Boolean = false
        internal set

    fun awaitTxBlocking(txId: Long, timeout: Duration? = null): TransactionResult? =
        runBlocking {
            check(isIndexing) { "log processor not initialised" }
            if (timeout == null) watchers.awaitTx(txId) else withTimeout(timeout) { watchers.awaitTx(txId) }
        }

    fun awaitSourceBlocking(sourceMsgId: MessageId, timeout: Duration? = null) =
        runBlocking {
            check(isIndexing) { "log processor not initialised" }
            if (timeout == null) watchers.awaitSource(sourceMsgId)
            else withTimeout(timeout) { watchers.awaitSource(sourceMsgId) }
        }

    override fun equals(other: Any?): Boolean =
        this === other || (other is Database && name == other.name)

    override fun hashCode() = Objects.hash(name)

    // Phase 1: cancel-join this database's job tree. A suspend (not `runBlocking`) so DETACH's closer
    // coroutine needn't park a thread. `close` MUST follow a returned `cancelAndJoin` — see the
    // SubSystem precedent in LogProcessor. Node shutdown cancels the whole catalog tree at once
    // instead, so its owner cancel-joins on its behalf and only `close` runs per database.
    suspend fun cancelAndJoin() = job?.cancelAndJoin()

    override fun close() {
        // Phase 2: the job tree has already been cancel-joined (by `cancelAndJoin` above, or by the
        // owner cancelling the catalog root). Free state, children before the database allocator.
        meterRegistry?.let { reg -> registeredGauges.forEach { reg.remove(it) } }
        (partitions.values + listOf(storage, allocator)).closeAll()
    }

    fun submitTxBlocking(ops: List<TxOp>, opts: TxOpts): Xtdb.SubmittedTx {
        if (config.externalSource != null)
            throw Incorrect(
                "Cannot submit transactions to database '$name': it has an external source configured. " +
                        "External-source databases use a separate txId sequence — submit through the source instead.",
                "xtdb/submit-tx-to-external-source-db",
                mapOf("db-name" to name)
            )

        val defaultTz = checkNotNull(opts.defaultTz) { "missing defaultTz" }
        val txMsg = SourceMessage.Tx(
            txOps = ops.toArrowBytes(allocator),
            systemTime = opts.systemTime,
            defaultTz = defaultTz,
            user = opts.user,
            userMetadata = opts.userMetadata?.let { serializeUserMetadata(allocator, it) }
        )
        val meta = sourceLog.appendMessageBlocking(txMsg)
        return Xtdb.SubmittedTx(meta.msgId)
    }

    fun sendFlushBlockMessage(): Log.MessageMetadata = runBlocking {
        sourceLog.appendMessage(SourceMessage.FlushBlock(blockCatalog.currentBlockIndex ?: -1))
    }

    fun sendAttachDbMessage(dbName: DatabaseName, config: Config): Log.MessageMetadata = runBlocking {
        sourceLog.appendMessage(SourceMessage.AttachDatabase(dbName, config))
    }

    fun sendDetachDbMessage(dbName: DatabaseName): Log.MessageMetadata = runBlocking {
        sourceLog.appendMessage(SourceMessage.DetachDatabase(dbName))
    }

    /**
     * Run one cycle of every garbage collector on the leader (block + trie), waiting for both.
     * No-op on non-leader nodes — GC only runs on the leader for a database.
     * Intended for tests and manual admin pokes; bypasses the `enabled` flag.
     */
    fun gcAll() {
        partitions.values.forEach { it.logProcessor?.gcAll() }
    }

    companion object {
        private fun logNewEpoch() {
            LOG.info(
                "Starting node with a log that has a different epoch than the latest processed message " +
                        "(this is expected if you are starting a new epoch) " +
                        "- skipping offset validation."
            )
        }

        private fun throwIllegalLogState(
            dbName: DatabaseName,
            latestSubmittedOffset: LogOffset, logEpoch: Int, processedEpoch: Int, processedOffset: MessageId
        ): Nothing {
            val logState =
                if (latestSubmittedOffset == -1L) "the log is empty"
                else "epoch=$logEpoch, offset=$latestSubmittedOffset"

            error(
                buildString {
                    appendLine("Database '$dbName' failed to start due to an invalid transaction log state ($logState) " +
                            "that does not correspond with the latest processed message " +
                            "(epoch=$processedEpoch and offset=$processedOffset).")
                    appendLine()
                    append("Please see https://docs.xtdb.com/ops/backup-and-restore/out-of-sync-log.html " +
                            "for more information and next steps.")
                }
            )
        }

        private fun validateOffsets(dbName: DatabaseName, log: Log<SourceMessage>, latestProcessedMsgId: MessageId?) {
            if (latestProcessedMsgId == null) return

            val processedOffset = msgIdToOffset(latestProcessedMsgId)
            val processedEpoch = msgIdToEpoch(latestProcessedMsgId)
            val logEpoch = log.epoch
            val latestSubmittedOffset = log.latestSubmittedOffset

            when {
                processedEpoch != logEpoch -> logNewEpoch()

                latestSubmittedOffset < processedOffset ->
                    throwIllegalLogState(dbName, latestSubmittedOffset, logEpoch, processedEpoch, processedOffset)
            }
        }

        @JvmStatic
        fun open(
            base: NodeBase,
            dbName: DatabaseName,
            dbConfig: Config,
            compactor: Compactor,
            parentScope: CoroutineScope,
            dbCatalog: Catalog? = null,
        ): Database = safelyOpening {
            val indexerConfig = base.config.indexer
            val readOnly = dbConfig.isReadOnly

            val allocator = open { base.allocator.newChildAllocator("database/$dbName", 0, Long.MAX_VALUE) }
            val storage = open { DatabaseStorage.open(allocator, base, dbName, dbConfig) }
            val state = open { DatabaseState.open(allocator, storage, dbName, indexerConfig) }
            val blockCatalog = state.blockCatalog
            val sourceMsgId = maxOf(
                blockCatalog.latestProcessedMsgId ?: -1,
                offsetToMsgId(storage.sourceLog.epoch, -1)
            )
            // tx-id and source-msg-id can diverge under ext-source — seed them independently:
            // tx-id from the live-index's last committed tx, source-msg-id from the persisted
            // block-catalog watermark (or the source-log epoch floor on a fresh epoch).
            val txId = state.liveIndex.latestCompletedTx?.txId ?: -1L

            // Catch log/storage divergence (rotated/truncated/wrong topic) before we wire up
            // the indexer — see /ops/backup-and-restore/out-of-sync-log.
            validateOffsets(dbName, storage.sourceLog, blockCatalog.latestProcessedMsgId)

            val watchers = Watchers(
                latestTxId = txId,
                latestSourceMsgId = sourceMsgId,
                externalSourceToken = blockCatalog.externalSourceToken,
            )

            val crashLogger = CrashLogger(allocator, storage.bufferPool, base.config.nodeId)

            // SupervisorJob child of the catalog's root job: the owner's single cancel still cascades
            // down to this database's whole tree (term, compactor, source-log subscription), but a
            // failure *within* the database (e.g. the source-log subscription) surfaces through this
            // scope's CoroutineExceptionHandler — `watchers.notifyError` — rather than propagating
            // up. A CoroutineExceptionHandler only fires for a root coroutine or a direct child of a
            // SupervisorJob, so this must be a SupervisorJob (cf. LogProcessor.openTerm). Sibling-
            // database isolation is a separate concern, provided by the parent dbJob being a Supervisor.
            val job = SupervisorJob(parentScope.coroutineContext.job)

            // Child of the database `job`; the owner's cancel stops and joins it along with the rest
            // of the tree.
            val compactorScope = CoroutineScope(Job(job))

            // For open-failure unwinding, each coroutine-owning resource registers a closeable that
            // cancel-joins its scope then frees it. Per-resource (rather than one global cancel
            // registered last) so a failure *between* resources can't leave safelyOpening freeing a
            // child allocator while its loop is still live. safelyOpening unwinds last-registered-
            // first, so these run before the plain allocator/storage/state closes — children first.
            val compactorForDb =
                (if (readOnly) Compactor.NOOP.openForDatabase(compactorScope, allocator, storage, state, watchers)
                else compactor.openForDatabase(compactorScope, allocator, storage, state, watchers))
                    .also { forDb ->
                        open {
                            AutoCloseable {
                                runBlocking { compactorScope.coroutineContext.job.cancelAndJoin() }
                                forDb.close()
                            }
                        }
                    }

            val scope = CoroutineScope(job + CoroutineExceptionHandler { _, e ->
                watchers.notifyError(e)
            })

            val logProcessor = if (indexerConfig.enabled) {
                val blockUploader = BlockUploader(storage, state, compactorForDb, dbCatalog, base.meterRegistry)
                val hasExternalSource = dbConfig.externalSource != null

                val procFactory = object : LogProcessor.ProcessorFactory {
                    override fun openFollower(
                        termScope: CoroutineScope,
                        pendingBlock: PendingBlock?,
                        afterReplicaMsgId: MessageId,
                    ) = FollowerLogProcessor(
                        termScope, allocator, storage.replicaLog, storage.bufferPool, state, compactorForDb,
                        watchers, dbCatalog, pendingBlock, afterReplicaMsgId,
                        hasExternalSource = hasExternalSource,
                        meterRegistry = base.meterRegistry,
                    )

                    // The leader term owns (and frees) its replica producer and ext source.
                    override fun openLeader(
                        termScope: CoroutineScope,
                        replicaProducer: Log.AtomicProducer<ReplicaMessage>,
                        afterReplicaMsgId: MessageId,
                    ) = LeaderLogProcessor(
                        allocator, base, storage, crashLogger,
                        state, blockUploader, watchers,
                        extSource = dbConfig.externalSource?.open(dbName, base.remotes, base.meterRegistry),
                        replicaProducer = replicaProducer,
                        skipTxs = indexerConfig.skipTxs.toSet(),
                        dbCatalog = dbCatalog,
                        partition = 0,
                        afterReplicaMsgId = afterReplicaMsgId,
                        flushTimeout = indexerConfig.flushDuration,
                        scope = termScope,
                    )

                    override fun openTransition(
                        replicaProducer: Log.AtomicProducer<ReplicaMessage>,
                        afterReplicaMsgId: MessageId,
                    ) = TransitionLogProcessor(
                        allocator, storage.bufferPool, state, state.liveIndex,
                        blockUploader, replicaProducer,
                        watchers, dbCatalog,
                        afterReplicaMsgId,
                        hasExternalSource = hasExternalSource,
                    )
                }

                LogProcessor(procFactory, storage, state, watchers, blockUploader, scope, base.meterRegistry)
                    .also { lp ->
                        // job.cancelAndJoin joins the term *and* the source-log subscription below.
                        open { AutoCloseable { runBlocking { job.cancelAndJoin() }; lp.close() } }
                    }
            } else null

            val meterRegistry = base.meterRegistry
            val gauges = meterRegistry?.let { reg ->
                fun gauge(name: String, f: () -> Double) =
                    Gauge.builder(name) { f() }
                        .tag("db", dbName)
                        .register(reg)

                listOf(
                    gauge("node.tx.latestCompletedTxId") {
                        (state.liveIndex.latestCompletedTx?.txId ?: -1).toDouble()
                    },
                    gauge("node.tx.latestSubmittedMsgId") {
                        storage.sourceLog.latestSubmittedMsgId.toDouble()
                    },
                    gauge("node.tx.latestProcessedMsgId") {
                        watchers.latestSourceMsgId.toDouble()
                    },
                    gauge("node.tx.lag.MsgId") {
                        maxOf(storage.sourceLog.latestSubmittedMsgId - watchers.latestSourceMsgId, 0).toDouble()
                    },
                )
            } ?: emptyList()

            val partition = DatabasePartition(
                partition = 0,
                state = state,
                watchers = watchers,
                compactorOrNull = compactorForDb,
                logProcessor = logProcessor,
            )

            val db = Database(
                allocator = allocator,
                config = dbConfig,
                storage = storage,
                isIndexing = indexerConfig.enabled,
                partitions = mapOf(0 to partition),
                meterRegistry = meterRegistry,
                job = job,
                registeredGauges = gauges,
            )

            if (indexerConfig.enabled && !readOnly) {
                val listener = object : Log.SubscriptionListener<SourceMessage> {
                    override suspend fun onPartitionsAssigned(partitions: Collection<Int>) =
                        partitions.singleOrNull()
                            ?.let { db.partitions[it]?.logProcessor?.onPartitionsAssigned(listOf(it)) }

                    override suspend fun onPartitionsRevoked(partitions: Collection<Int>) {
                        for (idx in partitions) db.partitions[idx]?.logProcessor?.onPartitionsRevoked(listOf(idx))
                    }
                }
                scope.launch { storage.sourceLog.openGroupSubscription(listener) }
            }

            db
        }
    }

    @Serializable
    enum class Mode {
        @SerialName("read-write")
        READ_WRITE,

        @SerialName("read-only")
        READ_ONLY;

        fun toProto(): DatabaseMode = when (this) {
            READ_WRITE -> DatabaseMode.READ_WRITE
            READ_ONLY -> DatabaseMode.READ_ONLY
        }

        companion object {
            @JvmStatic
            fun fromProto(mode: DatabaseMode): Mode = when (mode) {
                DatabaseMode.READ_WRITE, DatabaseMode.UNRECOGNIZED -> READ_WRITE
                DatabaseMode.READ_ONLY -> READ_ONLY
            }
        }
    }

    @Serializable
    data class Config(
        val log: Log.Factory = Log.inMemoryLog,
        val storage: Storage.Factory = Storage.inMemory(),
        val mode: Mode = Mode.READ_WRITE,
        val externalSource: ExternalSource.Factory? = null,
        val critical: Boolean = false,
    ) {
        fun log(log: Log.Factory) = copy(log = log)
        fun storage(storage: Storage.Factory) = copy(storage = storage)
        fun mode(mode: Mode) = copy(mode = mode)
        fun externalSource(externalSource: ExternalSource.Factory?) = copy(externalSource = externalSource)
        fun critical(critical: Boolean) = copy(critical = critical)

        val isReadOnly: Boolean get() = mode == Mode.READ_ONLY

        val serializedConfig: DatabaseConfig
            get() = DatabaseConfig.newBuilder()
                .also { dbConfig ->
                    log.writeTo(dbConfig)
                    dbConfig.applyStorage(storage)
                    dbConfig.mode = mode.toProto()
                    externalSource?.let { dbConfig.externalSource = ExternalSource.Factory.toProto(it) }
                    dbConfig.critical = critical
                }.build()

        companion object {
            @JvmStatic
            fun fromYaml(yaml: String): Config = YAML_SERDE.decodeFromString(yaml.trimIndent())

            @JvmStatic
            fun fromProto(dbConfig: DatabaseConfig) =
                Config()
                    .log(Log.Factory.fromProto(dbConfig))
                    .storage(Storage.Factory.fromProto(dbConfig))
                    .mode(Mode.fromProto(dbConfig.mode))
                    .externalSource(dbConfig.externalSource.takeIf { dbConfig.hasExternalSource() }
                        ?.let { ExternalSource.Factory.fromProto(it) })
                    .critical(dbConfig.critical)
        }
    }

    interface Catalog : ILookup, Seqable, Iterable<Database>, IQuerySource.QueryCatalog {
        companion object {
            private suspend fun Database.sync() = watchers.awaitSource(sourceLog.latestSubmittedMsgId)

            private suspend fun Catalog.awaitAll0(token: String) = coroutineScope {
                val basis = token.decodeTxBasisToken()

                databaseNames
                    .mapNotNull { databaseOrNull(it) }
                    .filter { it.isIndexing }
                    .map { db ->
                        launch {
                            val basisVec = basis[db.name] ?: return@launch
                            db.partitions.entries.sortedBy { it.key }
                                .forEach { (partIdx, part) ->
                                    basisVec.getOrNull(partIdx)?.let { part.watchers.awaitSource(it) }
                                }
                        }
                    }
                    .joinAll()
            }

            private suspend fun Catalog.syncAll0() = coroutineScope {
                databaseNames
                    .mapNotNull { databaseOrNull(it) }
                    .filter { it.isIndexing }
                    .map { db -> launch { db.sync() } }
                    .joinAll()
            }

            @JvmField
            val EMPTY = object : Catalog {
                override val databaseNames: Collection<DatabaseName> = emptySet()

                override fun databaseOrNull(dbName: DatabaseName) = null

                override fun attach(dbName: DatabaseName, config: Config?): Unit =
                    error("can't attach database to empty db-cat")

                override fun detach(dbName: DatabaseName) =
                    error("can't detach database from empty db-cat")
            }
        }

        override val databaseNames: Collection<DatabaseName>
        override fun databaseOrNull(dbName: DatabaseName): Database?

        operator fun get(dbName: DatabaseName) = databaseOrNull(dbName)

        val primary: Database get() = databaseOrNull("xtdb")!!

        // a read snapshot pinned to now: each database's latest-completed system-time per partition,
        // encoded as a time-basis token. The Kotlin twin of the node's PStatus/snapshot-token, so a
        // connection can pin a begin-time read basis without reaching into Clojure.
        fun snapshotToken(): String =
            databaseNames.mapNotNull { dbName ->
                // databaseNames and databaseOrNull are read separately, so a concurrent detach can drop a db
                // between them — skip it, as the sibling awaitAll0/syncAll0 do, rather than NPE.
                databaseOrNull(dbName)?.let { db ->
                    dbName to db.partitions.entries.sortedBy { it.key }
                        .map { it.value.liveIndex.latestCompletedTx?.systemTime }
                }
            }.toMap().encodeTimeBasisToken()

        fun attach(dbName: DatabaseName, config: Config?)
        fun detach(dbName: DatabaseName)

        override fun valAt(key: Any?) = valAt(key, null)
        override fun valAt(key: Any?, notFound: Any?) = databaseOrNull(key as DatabaseName) ?: notFound

        override fun iterator() = databaseNames.mapNotNull { databaseOrNull(it) }.iterator()

        override fun seq(): ISeq? =
            databaseNames.takeIf { it.isNotEmpty() }
                ?.map { MapEntry(it, databaseOrNull(it)) }
                ?.let { RT.seq(it) }

        fun awaitAll(token: String?, timeout: Duration?) = runBlocking {
            if (token != null)
                try {
                    if (timeout == null) awaitAll0(token) else withTimeout(timeout) { awaitAll0(token) }
                } catch (e: TimeoutCancellationException) {
                    val basis = token.decodeTxBasisToken()
                    val dbStatus = databaseNames
                        .mapNotNull { databaseOrNull(it) }
                        .filter { it.isIndexing }
                        .joinToString("\n") { db ->
                            val awaiting = basis[db.name]?.first()
                            val current = db.latestProcessedMsgId
                            val error = db.ingestionError != null
                            "  db=${db.name}: awaiting=$awaiting, current=$current, ingestionError=$error"
                        }
                    LOG.warn("awaitAll timed out after $timeout:\n$dbStatus")
                    throw e
                }
        }

        fun syncAll(timeout: Duration?) = runBlocking {
            if (timeout == null) syncAll0() else withTimeout(timeout) { syncAll0() }
        }

        val serialisedSecondaryDatabases: Map<DatabaseName, DatabaseConfig>
            get() = this.filterNot { it.name == "xtdb" }
                .associate { db -> db.name to db.config.serializedConfig }
    }
}
