package xtdb.indexer

import io.micrometer.core.instrument.Counter
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.apache.arrow.memory.BufferAllocator
import xtdb.NodeBase
import xtdb.api.TransactionKey
import xtdb.api.TransactionResult
import xtdb.api.log.*
import xtdb.api.log.Log.AtomicProducer.Companion.withTx
import xtdb.api.log.ReplicaMessage.BlockBoundary
import xtdb.api.log.ReplicaMessage.TriesAdded
import xtdb.api.storage.Storage
import xtdb.arrow.Relation
import xtdb.arrow.asChannel
import xtdb.database.Database
import xtdb.database.DatabaseState
import xtdb.database.DatabaseStorage
import xtdb.database.ExternalSource
import xtdb.database.ExternalSourceToken
import xtdb.error.Anomaly
import xtdb.error.Interrupted
import xtdb.garbage_collector.BlockGarbageCollector
import xtdb.garbage_collector.TrieGarbageCollector
import xtdb.indexer.Indexer.Companion.addTxRow
import xtdb.indexer.TxIndexer.TxResult
import xtdb.table.TableRef
import xtdb.time.InstantUtil.asMicros
import xtdb.time.InstantUtil.fromMicros
import xtdb.tx.deserializeUserMetadata
import xtdb.util.StringUtil.asLexDec
import xtdb.util.StringUtil.asLexHex
import xtdb.util.asPath
import xtdb.util.debug
import xtdb.util.error
import xtdb.util.logger
import xtdb.util.trace
import xtdb.util.warn
import java.nio.ByteBuffer
import java.time.Duration
import java.time.Instant
import java.time.InstantSource
import java.time.ZoneId
import java.time.ZonedDateTime
import kotlin.coroutines.CoroutineContext

private val LOG = LeaderLogProcessor::class.logger

class LeaderLogProcessor(
    allocator: BufferAllocator,
    private val nodeBase: NodeBase,
    private val dbStorage: DatabaseStorage,
    indexer: Indexer,
    crashLogger: CrashLogger,
    private val dbState: DatabaseState,
    private val blockUploader: BlockUploader,
    private val watchers: Watchers,
    private val extSource: ExternalSource?,
    private val replicaProducer: Log.AtomicProducer<ReplicaMessage>,
    private val skipTxs: Set<MessageId>,
    private val dbCatalog: Database.Catalog?,
    partition: Int,
    afterSourceMsgId: MessageId,
    afterReplicaMsgId: MessageId,
    afterToken: ExternalSourceToken?,
    private val instantSource: InstantSource = InstantSource.system(),
    flushTimeout: Duration = Duration.ofMinutes(5),
    ctx: CoroutineContext = Dispatchers.Default,
) : LogProcessor.LeaderProcessor, TxIndexer {

    init {
        require((dbCatalog != null) == (dbState.name == "xtdb")) {
            "dbCatalog must be provided iff database is 'xtdb'"
        }
    }

    private val dbName = dbState.name
    private val sourceLog = dbStorage.sourceLog
    private val bufferPool = dbStorage.bufferPool
    private val liveIndex = dbState.liveIndex

    private val blockCatalog = dbState.blockCatalog
    private val trieCatalog = dbState.trieCatalog

    private val allocator = allocator.newChildAllocator("leader-log-processor", 0, Long.MAX_VALUE)

    private val tracer = nodeBase.tracer?.takeIf { nodeBase.config.tracer.transactionTracing }

    private val indexer: Indexer.ForDatabase =
        indexer.openForDatabase(this.allocator, dbState, liveIndex, crashLogger, this)

    internal val blockGc = nodeBase.config.garbageCollector.let { cfg ->
        BlockGarbageCollector(
            bufferPool, blockCatalog,
            blocksToKeep = cfg.blocksToKeep,
            enabled = cfg.enabled,
            meterRegistry = nodeBase.meterRegistry,
        )
    }

    override var pendingBlock: PendingBlock? = null
        private set

    override var latestSourceMsgId: MessageId = afterSourceMsgId
        private set

    override var latestReplicaMsgId: MessageId = afterReplicaMsgId
        private set

    private val blockFlusher = BlockFlusher(flushTimeout, blockCatalog)

    // Serialises every writer to the replica log: source-log records (via [processRecords]),
    // external-source txs (via [indexTx]), and the trie-GC `TriesDeleted` publishes (via
    // [trieGc]'s commit callback). The transactional Kafka producer is not safe for concurrent
    // `withTx`, so every path holds this lock before touching it.
    private val mutex = Mutex()

    internal val trieGc = nodeBase.config.garbageCollector.let { cfg ->
        TrieGarbageCollector(
            bufferPool, dbState,
            // The replica-log append and the local catalog mutation are one atom under the
            // replica-log mutex. If they were split, this interleaving would corrupt persistent
            // state:
            //
            //   1. Trie GC takes the mutex, appends `TriesDeleted(G)` at replica position N,
            //      releases the mutex.
            //   2. Before Trie GC re-takes a (separate) lock to mutate the catalog, another
            //      coroutine — say an ext-source `commit` whose `liveIndex.isFull()` — grabs
            //      the mutex.
            //   3. That coroutine runs `finishBlock`, which uploads table-block files snapshotting
            //      the current catalog. The catalog still has G in it (Trie GC's mutation hasn't
            //      happened yet), so the table-block file at replica position M > N records
            //      "catalog includes G" — even though the replica log already has `TriesDeleted`
            //      for G at N.
            //   4. Trie GC finally mutates the catalog and removes G.
            //
            // The table-block file uploaded at (3) is now a persistent snapshot of state that
            // disagrees with the replica log it claims to be a snapshot of.
            commitTriesDeleted = { tableName, trieKeys ->
                mutex.withLock {
                    appendToReplica(ReplicaMessage.TriesDeleted(tableName.schemaAndTable, trieKeys))
                    trieCatalog.deleteTries(tableName, trieKeys)
                }
            },
            blocksToKeep = cfg.blocksToKeep,
            garbageLifetime = cfg.garbageLifetime,
            enabled = cfg.enabled,
            meterRegistry = nodeBase.meterRegistry,
        )
    }

    private val txErrorCounter: Counter? =
        nodeBase.meterRegistry?.let { Counter.builder("tx.error").register(it) }

    private val extJob = extSource?.let { source ->
        CoroutineScope(ctx).launch {
            try {
                source.onPartitionAssigned(partition, afterToken, this@LeaderLogProcessor)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Throwable) {
                watchers.notifyError(e)
            }
        }
    }

    private fun smoothSystemTime(systemTime: Instant): Instant {
        val lct = liveIndex.latestCompletedTx?.systemTime ?: return systemTime
        val floor = fromMicros(lct.asMicros + 1)
        return if (systemTime.isBefore(floor)) floor else systemTime
    }

    private fun openTx(txKey: TransactionKey, externalSourceToken: ExternalSourceToken?) =
        OpenTx(allocator, nodeBase, dbStorage, dbState, txKey, externalSourceToken, tracer)

    override fun startTx(txKey: TransactionKey): OpenTx = openTx(txKey, null)

    override suspend fun indexTx(
        externalSourceToken: ExternalSourceToken?,
        txId: Long?,
        systemTime: Instant?,
        writer: suspend (OpenTx) -> TxResult,
    ): TxResult {
        val resolvedTxId = txId ?: ((liveIndex.latestCompletedTx?.txId ?: -1) + 1)
        val resolvedSystemTime = systemTime ?: instantSource.instant()
        val txKey = TransactionKey(resolvedTxId, smoothSystemTime(resolvedSystemTime))

        try {
            val openTx = openTx(txKey, externalSourceToken)
            val result = try {
                writer(openTx)
            } catch (e: Throwable) {
                openTx.close()
                throw e
            }

            when (result) {
                is TxResult.Committed -> openTx.use {
                    it.addTxRow(dbName, txKey, null, result.userMetadata)
                    commit(it, result)
                }

                is TxResult.Aborted -> {
                    txErrorCounter?.increment()
                    openTx.close()
                    openTx(txKey, externalSourceToken).use { abortTx ->
                        abortTx.addTxRow(dbName, txKey, result.error, result.userMetadata)
                        commit(abortTx, result)
                    }
                }
            }

            return result
        } catch (e: CancellationException) {
            throw e
        } catch (e: Throwable) {
            watchers.notifyError(e)
            throw e
        }
    }

    private suspend fun maybeFlushBlock() {
        if (blockFlusher.checkBlockTimeout(blockCatalog, liveIndex)) {
            val flushMessage = SourceMessage.FlushBlock(blockCatalog.currentBlockIndex ?: -1)
            blockFlusher.flushedTxId = sourceLog.appendMessage(flushMessage).msgId
        }
    }

    private suspend fun appendToReplica(message: ReplicaMessage): Log.MessageMetadata =
        replicaProducer.withTx { tx -> tx.appendMessage(message) }.await()
            .also { latestReplicaMsgId = it.msgId }

    private suspend fun finishBlock(latestProcessedMsgId: MessageId, externalSourceToken: ExternalSourceToken?) {
        val boundaryMsg =
            BlockBoundary((blockCatalog.currentBlockIndex ?: -1) + 1, latestProcessedMsgId, externalSourceToken)

        val boundaryMsgId = appendToReplica(boundaryMsg).msgId
        LOG.debug("[$dbName] block boundary b${boundaryMsg.blockIndex.asLexHex}: source=$latestProcessedMsgId, replica=$boundaryMsgId")

        pendingBlock = PendingBlock(boundaryMsgId, boundaryMsg)

        latestReplicaMsgId = blockUploader.uploadBlock(replicaProducer, boundaryMsgId, boundaryMsg)
        pendingBlock = null

        // Fire-and-forget: a non-suspending [signal] just enqueues a cycle on the GC's own
        // coroutine. We can call this from inside the replica-log mutex without deadlock — the
        // GC's `commitTriesDeleted` callback only takes the mutex when its loop actually runs,
        // by which time we've long released it. Tx processing carries on without waiting for GC.
        blockGc.signal()
        trieGc.signal()
    }

    private fun resolveTx(
        msgId: MessageId, record: Log.Record<SourceMessage>, msg: SourceMessage
    ): ReplicaMessage.ResolvedTx {
        if (skipTxs.isNotEmpty() && skipTxs.contains(msgId)) {
            LOG.warn("[$dbName] Skipping transaction id $msgId - within XTDB_SKIP_TXS")

            val payload = when (msg) {
                is SourceMessage.Tx -> msg.encode()
                is SourceMessage.LegacyTx -> msg.payload
                else -> error("unexpected message type: ${msg::class}")
            }
            bufferPool.putObject("skipped-txs/${msgId.asLexDec}".asPath, ByteBuffer.wrap(payload))

            return indexer.indexTx(msgId, record.logTimestamp, null, null, null, null, null)
        }

        return when (msg) {
            is SourceMessage.Tx -> {
                msg.txOps.asChannel.use { ch ->
                    Relation.StreamLoader(allocator, ch).use { loader ->
                        Relation(allocator, loader.schema).use { rel ->
                            loader.loadNextPage(rel)

                            val userMetadata = msg.userMetadata?.let { deserializeUserMetadata(allocator, it) }

                            indexer.indexTx(
                                msgId, record.logTimestamp,
                                rel["tx-ops"],
                                msg.systemTime, msg.defaultTz, msg.user, userMetadata
                            )
                        }
                    }
                }
            }

            is SourceMessage.LegacyTx -> {
                msg.payload.asChannel.use { txOpsCh ->
                    Relation.StreamLoader(allocator, txOpsCh).use { loader ->
                        Relation(allocator, loader.schema).use { rel ->
                            loader.loadNextPage(rel)

                            val systemTime =
                                (rel["system-time"].getObject(0) as ZonedDateTime?)?.toInstant()

                            val defaultTz =
                                (rel["default-tz"].getObject(0) as String?).let { ZoneId.of(it) }

                            val user = rel["user"].getObject(0) as String?

                            val userMetadata = rel.vectorForOrNull("user-metadata")?.getObject(0)

                            indexer.indexTx(
                                msgId, record.logTimestamp,
                                rel["tx-ops"].listElements,
                                systemTime, defaultTz, user, userMetadata
                            )
                        }
                    }
                }
            }

            else -> error("unexpected message type: ${msg::class}")
        }
    }

    private fun notifyTx(resolvedTx: ReplicaMessage.ResolvedTx) {
        val txKey = TransactionKey(resolvedTx.txId, resolvedTx.systemTime)

        val result =
            if (resolvedTx.committed) TransactionResult.Committed(txKey)
            else TransactionResult.Aborted(txKey, resolvedTx.error)

        watchers.notifyTx(result, resolvedTx.txId, resolvedTx.externalSourceToken)
    }

    private suspend fun handleResolvedTx(resolvedTx: ReplicaMessage.ResolvedTx) {
        val txId = resolvedTx.txId

        appendToReplica(resolvedTx)
        latestSourceMsgId = txId
        notifyTx(resolvedTx)

        if (liveIndex.isFull())
            finishBlock(txId, resolvedTx.externalSourceToken)
    }

    private suspend fun commit(openTx: OpenTx, result: TxResult) {
        val txKey = openTx.txKey
        val externalSourceToken = openTx.externalSourceToken
        val tableData = openTx.serializeTableData()

        mutex.withLock {
            liveIndex.commitTx(openTx)

            val resolvedTx = ReplicaMessage.ResolvedTx(
                txKey.txId, txKey.systemTime,
                committed = when (result) {
                    is TxResult.Committed -> true
                    is TxResult.Aborted -> false
                },
                error = (result as? TxResult.Aborted)?.error,
                tableData, dbOp = null,
                externalSourceToken = externalSourceToken,
            )

            appendToReplica(resolvedTx)

            val txResult = when (result) {
                is TxResult.Committed -> TransactionResult.Committed(txKey)
                is TxResult.Aborted -> TransactionResult.Aborted(txKey, result.error)
            }
            watchers.notifyTx(txResult, latestSourceMsgId, resolvedTx.externalSourceToken)

            if (liveIndex.isFull())
                finishBlock(latestSourceMsgId, resolvedTx.externalSourceToken)
        }
    }

    override suspend fun processRecords(records: List<Log.Record<SourceMessage>>) {
        maybeFlushBlock()

        for (record in records) {
            mutex.withLock {
                val msgId = record.msgId
                LOG.trace { "[$dbName] leader: message $msgId (${record.message::class.simpleName})" }

                try {
                    when (val msg = record.message) {
                        is SourceMessage.Tx -> {
                            val resolved = resolveTx(msgId, record, msg)
                            handleResolvedTx(msg.externalSourceToken?.let { resolved.copy(externalSourceToken = it) }
                                ?: resolved)
                        }

                        is SourceMessage.LegacyTx -> handleResolvedTx(resolveTx(msgId, record, msg))

                        is SourceMessage.FlushBlock -> {
                            val expectedBlockIdx = msg.expectedBlockIdx
                            if (expectedBlockIdx != null && expectedBlockIdx == (blockCatalog.currentBlockIndex ?: -1L)) {
                                finishBlock(msgId, watchers.externalSourceToken)
                            }
                            latestSourceMsgId = msgId
                            watchers.notifyMsg(msgId)
                        }

                        is SourceMessage.AttachDatabase -> {
                            val txKey = TransactionKey(msgId, record.logTimestamp)
                            val error = if (dbCatalog != null) {
                                try {
                                    dbCatalog.attach(msg.dbName, msg.config)
                                    null
                                } catch (e: Anomaly.Caller) {
                                    LOG.debug(e) { "[$dbName] leader: attach database '${msg.dbName}' failed at $msgId" }
                                    e
                                }
                            } else null

                            val resolvedTx = indexer.addTxRow(txKey, error)
                                .let { if (error == null) it.copy(dbOp = DbOp.Attach(msg.dbName, msg.config)) else it }

                            appendToReplica(resolvedTx)

                            val result =
                                if (error == null) TransactionResult.Committed(txKey)
                                else TransactionResult.Aborted(txKey, error)
                            latestSourceMsgId = msgId
                            watchers.notifyTx(result, msgId, null)
                        }

                        is SourceMessage.DetachDatabase -> {
                            val txKey = TransactionKey(msgId, record.logTimestamp)
                            val error = if (dbCatalog != null) {
                                try {
                                    dbCatalog.detach(msg.dbName)
                                    null
                                } catch (e: Anomaly.Caller) {
                                    LOG.debug(e) { "[$dbName] leader: detach database '${msg.dbName}' failed at $msgId" }
                                    e
                                }
                            } else null

                            val resolvedTx = indexer.addTxRow(txKey, error)
                                .let { if (error == null) it.copy(dbOp = DbOp.Detach(msg.dbName)) else it }

                            appendToReplica(resolvedTx)

                            val result = if (error == null) TransactionResult.Committed(txKey)
                            else TransactionResult.Aborted(txKey, error)
                            latestSourceMsgId = msgId
                            watchers.notifyTx(result, msgId, null)
                        }

                        is SourceMessage.TriesAdded -> {
                            if (msg.storageVersion == Storage.VERSION && msg.storageEpoch == bufferPool.epoch) {
                                msg.tries.groupBy { it.tableName }.forEach { (tableName, tries) ->
                                    trieCatalog.addTries(
                                        TableRef.parse(dbState.name, tableName),
                                        tries,
                                        record.logTimestamp
                                    )
                                }
                            }

                            appendToReplica(
                                TriesAdded(
                                    msg.storageVersion, msg.storageEpoch, msg.tries,
                                    sourceMsgId = msgId
                                )
                            )

                            latestSourceMsgId = msgId
                            watchers.notifyMsg(msgId)
                        }

                        // TODO this one's going before release
                        is SourceMessage.BlockUploaded -> {
                            latestSourceMsgId = msgId
                            watchers.notifyMsg(msgId)
                        }
                    }
                } catch (e: InterruptedException) {
                    throw e
                } catch (e: Interrupted) {
                    throw e
                } catch (e: Throwable) {
                    LOG.error(
                        e,
                        "[$dbName] leader: failed to process log record with msgId $msgId (${record.message::class.simpleName})"
                    )
                    watchers.notifyError(e)
                    throw e
                }
            }
        }
    }

    override fun close() {
        // HACK: we cancel without joining because a blocking join deadlocks under runTest's virtual time.
        extJob?.cancel()
        extSource?.close()
        blockGc.close()
        trieGc.close()
        indexer.close()
        allocator.close()
    }
}
