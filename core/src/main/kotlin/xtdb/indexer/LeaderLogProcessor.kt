package xtdb.indexer

import io.micrometer.core.instrument.Counter
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.selects.selectUnbiased
import org.apache.arrow.memory.BufferAllocator
import xtdb.Metrics.withSpan
import xtdb.NodeBase
import xtdb.api.TransactionKey
import xtdb.api.TransactionResult.Aborted
import xtdb.api.TransactionResult.Committed
import xtdb.api.log.*
import xtdb.api.log.Log.AtomicProducer.Companion.withTx
import xtdb.api.log.ReplicaMessage.BlockBoundary
import xtdb.api.log.ReplicaMessage.TriesAdded
import xtdb.api.storage.Storage
import xtdb.arrow.Relation
import xtdb.arrow.VectorReader
import xtdb.arrow.asChannel
import xtdb.database.*
import xtdb.error.Anomaly
import xtdb.error.Fault
import xtdb.error.Incorrect
import xtdb.error.Interrupted
import xtdb.garbage_collector.BlockGarbageCollector
import xtdb.garbage_collector.TrieGarbageCollector
import xtdb.indexer.TxIndexer.TxResult
import xtdb.table.TableRef
import xtdb.time.InstantUtil.asMicros
import xtdb.time.InstantUtil.fromMicros
import xtdb.trie.TrieKey
import xtdb.tx.deserializeUserMetadata
import xtdb.util.*
import xtdb.util.StringUtil.asLexDec
import xtdb.util.StringUtil.asLexHex
import java.nio.ByteBuffer
import java.time.*

private val SKIPPED_EXN: Throwable = Fault("Transaction was skipped", "xtdb/skipped-tx")

private val LOG = LeaderLogProcessor::class.logger

class LeaderLogProcessor(
    allocator: BufferAllocator,
    private val nodeBase: NodeBase,
    private val dbStorage: DatabaseStorage,
    crashLogger: CrashLogger,
    private val dbState: DatabaseState,
    private val blockUploader: BlockUploader,
    private val watchers: Watchers,
    private val extSource: ExternalSource?,
    private val replicaProducer: Log.AtomicProducer<ReplicaMessage>,
    private val skipTxs: Set<MessageId>,
    private val dbCatalog: Database.Catalog?,
    partition: Int,
    afterReplicaMsgId: MessageId,
    private val instantSource: InstantSource = InstantSource.system(),
    flushTimeout: Duration = Duration.ofMinutes(5),
    scope: CoroutineScope,
    // Base for the GCs' delete fan-out; defaults to IO in prod, sims inject the seeded dispatcher.
    gcDispatcher: CoroutineDispatcher = Dispatchers.IO,
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

    private val sourceLogTxIndexer = SourceLogTxIndexer(this.allocator, nodeBase, dbState, crashLogger)

    // The GCs run under a SupervisorJob child of the leader scope, so one GC's failure cancels
    // neither its sibling nor the persister; cancelling the leader scope reaps them all.
    private val gcScope = CoroutineScope(scope.coroutineContext + SupervisorJob(scope.coroutineContext.job))

    internal val blockGc = nodeBase.config.garbageCollector.let { cfg ->
        BlockGarbageCollector(
            gcScope,
            bufferPool, blockCatalog,
            blocksToKeep = cfg.blocksToKeep,
            enabled = cfg.enabled,
            meterRegistry = nodeBase.meterRegistry,
            dispatcher = gcDispatcher,
            dbName = dbName
        )
    }

    override var pendingBlock: PendingBlock? = null
        private set

    override var latestReplicaMsgId: MessageId = afterReplicaMsgId
        private set

    private val blockFlusher = BlockFlusher(flushTimeout, blockCatalog)

    private sealed interface PersisterTask {
        val onComplete: CompletableDeferred<Unit>
    }

    private sealed interface SourceLogTask : PersisterTask {
        // One task per poll batch; the persister resolves + imports the records in order.
        // onComplete is required by PersisterTask but unused here — processRecords fires and returns.
        data class Batch(val records: List<Log.Record<SourceMessage>>) : SourceLogTask {
            override val onComplete = CompletableDeferred<Unit>()
        }
    }

    private sealed interface ExtSourceTask : PersisterTask {
        class IndexTx(
            val externalSourceToken: ExternalSourceToken?,
            val systemTime: Instant?,
            val writer: suspend (OpenTx) -> TxResult,
        ) : ExtSourceTask {
            val result = CompletableDeferred<TxResult>()

            override val onComplete = CompletableDeferred<Unit>()
        }
    }

    private sealed interface GcTask : PersisterTask {
        data class TriesDeleted(val tableName: TableRef, val trieKeys: Set<TrieKey>) : GcTask {
            override val onComplete = CompletableDeferred<Unit>()
        }
    }

    // capacity 1: the poll thread can deposit one batch ahead and read the next while the persister
    // works, bounding lookahead to ~2 batches. Backpressure falls out of a full channel suspending the send.
    private val sourceLogCh =
        Channel<SourceLogTask>(capacity = 1, onUndeliveredElement = { it.onComplete.cancel() })
    private val extSourceCh =
        Channel<ExtSourceTask>(onUndeliveredElement = { it.onComplete.cancel() })
    private val gcCh =
        Channel<GcTask>(onUndeliveredElement = { it.onComplete.cancel() })

    private suspend fun handleSourceLogBatch(records: List<Log.Record<SourceMessage>>) {
        for (record in records) {
            LOG.trace { "[$dbName] leader: message ${record.msgId} (${record.message::class.simpleName})" }
            handleSourceLogRecord(record)
        }
    }

    private suspend fun handleSourceLogRecord(record: Log.Record<SourceMessage>) {
        val msgId = record.msgId

        when (val msg = record.message) {
            is SourceMessage.Tx, is SourceMessage.LegacyTx ->
                handleResolvedTx(resolveTx(msgId, record, msg), msgId)

            is SourceMessage.FlushBlock -> {
                val expectedBlockIdx = msg.expectedBlockIdx
                if (expectedBlockIdx != null && expectedBlockIdx == (blockCatalog.currentBlockIndex ?: -1L)) {
                    finishBlock(msgId, watchers.externalSourceToken)
                } else {
                    // see #5680
                    appendToReplica(ReplicaMessage.NoOp(srcMsgId = msgId))
                }
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

                val resolvedTx = openTx(txKey, null).use { it.commitTx(error).copy(srcMsgId = msgId) }
                    .let { if (error == null) it.copy(dbOp = DbOp.Attach(msg.dbName, msg.config)) else it }

                appendToReplica(resolvedTx)
                liveIndex.importTx(resolvedTx)

                val result = if (error == null) Committed(txKey) else Aborted(txKey, error)
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

                val resolvedTx = openTx(txKey, null).use { it.commitTx(error).copy(srcMsgId = msgId) }
                    .let { if (error == null) it.copy(dbOp = DbOp.Detach(msg.dbName)) else it }

                appendToReplica(resolvedTx)
                liveIndex.importTx(resolvedTx)

                val result = if (error == null) Committed(txKey) else Aborted(txKey, error)
                watchers.notifyTx(result, msgId, null)
            }

            is SourceMessage.TriesAdded -> {
                if (msg.storageVersion == Storage.VERSION && msg.storageEpoch == bufferPool.epoch) {
                    msg.tries.groupBy { it.tableName }.forEach { (tableName, tries) ->
                        trieCatalog.addTries(TableRef.parse(tableName), tries, record.logTimestamp)
                    }
                }

                appendToReplica(TriesAdded(msg.storageVersion, msg.storageEpoch, msg.tries, sourceMsgId = msgId))

                watchers.notifyMsg(msgId)
            }

            // TODO this one's going after 2.2
            is SourceMessage.BlockUploaded -> {
                watchers.notifyMsg(msgId)
            }
        }
    }

    private suspend fun handleResolvedTx(resolvedTx: ReplicaMessage.ResolvedTx, srcMsgId: MessageId?) {
        val txKey = TransactionKey(resolvedTx.txId, resolvedTx.systemTime)
        val txResult = if (resolvedTx.committed) Committed(txKey) else Aborted(txKey, resolvedTx.error)

        // Ext-source txs carry no source-log position of their own (`srcMsgId == null` on the way in)
        // and track progress via `externalSourceToken`, so they don't advance the leader's
        // `latestSourceMsgId` (which is driven by the source log). We do stamp the current source-log
        // watermark onto the replicated record, though: without it a follower's `latestSourceMsgId`
        // lags between block boundaries, and on promotion it resumes the source log from a stale point
        // and replays an already-covered block boundary.
        val effectiveSrcMsgId = srcMsgId ?: watchers.latestSourceMsgId

        appendToReplica(resolvedTx.copy(srcMsgId = effectiveSrcMsgId))
        liveIndex.importTx(resolvedTx)

        watchers.notifyTx(txResult, effectiveSrcMsgId, resolvedTx.externalSourceToken)

        if (liveIndex.isFull())
            finishBlock(effectiveSrcMsgId, resolvedTx.externalSourceToken)
    }

    private suspend fun handleIndexTx(task: ExtSourceTask.IndexTx) {
        val txKey = TransactionKey(
            (liveIndex.latestCompletedTx?.txId ?: -1) + 1,
            smoothSystemTime(task.systemTime ?: instantSource.instant())
        )

        var openTx = openTx(txKey, task.externalSourceToken)

        @Suppress("ConvertTryFinallyToUseCall") // because openTx is a var
        try {
            val writerResult = task.writer(openTx)
            val resolvedTx = when (writerResult) {
                is TxResult.Committed ->
                    openTx.commitTx(error = null, writerResult.userMetadata)

                is TxResult.Aborted -> {
                    txErrorCounter?.increment()
                    openTx.close()
                    // fresh tx for the abort row — the original openTx may hold partial writes
                    openTx = openTx(txKey, task.externalSourceToken)
                    openTx.commitTx(writerResult.error, writerResult.userMetadata)
                }
            }

            handleResolvedTx(resolvedTx, srcMsgId = null)
            task.result.complete(writerResult)
        } finally {
            openTx.close()
        }
    }

    private suspend fun handleTriesDeleted(task: GcTask.TriesDeleted) {
        appendToReplica(ReplicaMessage.TriesDeleted(task.tableName.schemaAndTable, task.trieKeys))
        trieCatalog.deleteTries(task.tableName, task.trieKeys)
    }

    private suspend fun submit(task: PersisterTask) {
        when (task) {
            is SourceLogTask -> sourceLogCh.send(task)
            is ExtSourceTask -> extSourceCh.send(task)
            is GcTask -> gcCh.send(task)
        }
        task.onComplete.await()
    }

    internal val trieGc = nodeBase.config.garbageCollector.let { cfg ->
        // The replica-log append and the local catalog mutation are one atom — both run inside
        // a single Persister task. If they were split, this interleaving would corrupt
        // persistent state:
        //
        //   1. Trie GC submits `TriesDeleted(G)` at replica position N, then (separately)
        //      submits the catalog mutation.
        //   2. Between the two, another Persister task — say an ext-source `commit` whose
        //      `liveIndex.isFull()` — runs `finishBlock`, which uploads table-block files
        //      snapshotting the current catalog. The catalog still has G in it (Trie GC's
        //      mutation hasn't happened yet), so the table-block file at replica position
        //      M > N records "catalog includes G" — even though the replica log already has
        //      `TriesDeleted` for G at N.
        //   3. Trie GC's catalog mutation finally runs and removes G.
        //
        // The table-block file uploaded at (2) is now a persistent snapshot of state that
        // disagrees with the replica log it claims to be a snapshot of.
        val commitTriesDeleted: suspend (TableRef, Set<TrieKey>) -> Unit = { tableName, trieKeys ->
            submit(GcTask.TriesDeleted(tableName, trieKeys))
        }

        TrieGarbageCollector(
            gcScope,
            bufferPool, dbState,
            commitTriesDeleted, cfg.blocksToKeep, cfg.garbageLifetime,
            cfg.enabled,
            nodeBase.meterRegistry,
            dispatcher = gcDispatcher,
        )
    }

    private val txErrorCounter: Counter? = nodeBase.meterRegistry?.let { Counter.builder("tx.error").register(it) }

    private val termJob: Job = scope.launch {
        // supervisorScope so an ext-source crash doesn't kill the persister.
        supervisorScope {
            launch {
                // Close the channels with the failure cause so a subsequent `processRecords` send
                // (no longer awaited per-record) throws it rather than a bare ClosedSendChannelException.
                var cause: Throwable? = null
                try {
                    while (true) {
                        val task: PersisterTask = selectUnbiased {
                            sourceLogCh.onReceive { it }
                            extSourceCh.onReceive { it }
                            gcCh.onReceive { it }
                        }
                        try {
                            when (task) {
                                is SourceLogTask.Batch -> handleSourceLogBatch(task.records)
                                is ExtSourceTask.IndexTx -> handleIndexTx(task)
                                is GcTask.TriesDeleted -> handleTriesDeleted(task)
                            }
                            task.onComplete.complete(Unit)
                        } catch (e: CancellationException) {
                            task.onComplete.cancel(e)
                            throw e
                        } catch (e: InterruptedException) {
                            task.onComplete.completeExceptionally(e)
                            throw e
                        } catch (e: Interrupted) {
                            task.onComplete.completeExceptionally(e)
                            throw e
                        } catch (e: Throwable) {
                            watchers.notifyError(e)
                            task.onComplete.completeExceptionally(e)
                            throw e
                        }
                    }
                } catch (e: CancellationException) {
                    // term cancellation: close the channels without an error cause
                } catch (t: Throwable) {
                    cause = t
                } finally {
                    sourceLogCh.close(cause)
                    extSourceCh.close(cause)
                    gcCh.close(cause)
                }
            }

            extSource?.let { source ->
                launch {
                    try {
                        source.onPartitionAssigned(partition, watchers.externalSourceToken, this@LeaderLogProcessor)
                    } catch (e: CancellationException) {
                        throw e
                    } catch (e: Throwable) {
                        watchers.notifyError(e)
                    }
                }
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

    override suspend fun indexTx(
        externalSourceToken: ExternalSourceToken?, systemTime: Instant?,
        writer: suspend (OpenTx) -> TxResult,
    ): TxResult =
        ExtSourceTask.IndexTx(externalSourceToken, systemTime, writer)
            .also { submit(it) }
            .result.await()

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

        // Safe to call from inside a Persister task: signal() just enqueues a cycle on the GC's
        // own coroutine; its `commitTriesDeleted` callback submits a fresh task that won't run
        // until this one returns.
        blockGc.signal()
        trieGc.signal()
    }

    private fun indexSourceLogTx(
        msgId: MessageId,
        msgTimestamp: Instant,
        txOps: VectorReader?,
        systemTime: Instant?,
        defaultTz: ZoneId?,
        user: String?,
        userMetadata: Any?,
    ): ReplicaMessage.ResolvedTx = tracer.withSpan(
        "xtdb.transaction",
        attributes = mapOf("operations.count" to (txOps?.valueCount ?: 0).toString()),
    ) {
        val userMetadataMap = userMetadata as? Map<*, *>
        val lcTx = liveIndex.latestCompletedTx

        // If lc-tx's systemTime >= msgTimestamp, bump past it by 1µs; otherwise use msgTimestamp.
        // (`+1000ns` is `+1µs`.)
        val defaultSystemTime: Instant = lcTx?.systemTime?.let { lcSysTime ->
            if (lcSysTime >= msgTimestamp) lcSysTime.plusNanos(1_000) else null
        } ?: msgTimestamp

        // Specified system-time before lc-tx → invalid; abort with that error.
        // The aborted tx-key uses the *default* (smoothed) systemTime, not the rejected one,
        // so the tx-key still satisfies the monotonicity invariant.
        if (systemTime != null && lcTx != null && systemTime < lcTx.systemTime) {
            val txKey = TransactionKey(msgId, defaultSystemTime)
            val err = Incorrect(
                "specified system-time older than current tx",
                "invalid-system-time",
                mapOf(
                    "tx-key" to TransactionKey(msgId, systemTime),
                    "latest-completed-tx" to lcTx,
                ),
            )
            LOG.warn { "specified system-time '$systemTime' older than current tx '$lcTx'" }

            return@withSpan openTx(txKey, null).use { openTx ->
                txErrorCounter?.increment()
                openTx.commitTx(err, userMetadataMap)
            }
        }

        val effectiveSystemTime = systemTime ?: defaultSystemTime
        val txKey = TransactionKey(msgId, effectiveSystemTime)

        openTx(txKey, null).use { openTx ->
            if (txOps == null) {
                return@withSpan openTx.commitTx(SKIPPED_EXN, userMetadataMap)
            }

            val opts = SourceLogTxIndexer.TxOpts(
                txKey = txKey,
                currentTime = effectiveSystemTime,
                systemTime = effectiveSystemTime.asMicros,
                defaultTz = defaultTz,
                user = user,
            )

            when (val result = sourceLogTxIndexer.ForTx(txOps, opts).indexTx(openTx)) {
                is TxResult.Committed -> openTx.commitTx(null, userMetadataMap)

                is TxResult.Aborted -> {
                    LOG.debug(result.error) { "aborted tx" }
                    // Open a fresh tx for the abort row — the original openTx may have partial writes.
                    return@withSpan openTx(txKey, null).use { abortTx ->
                        txErrorCounter?.increment()
                        abortTx.commitTx(result.error, userMetadataMap)
                    }
                }
            }
        }
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

            return indexSourceLogTx(msgId, record.logTimestamp, null, null, null, null, null)
        }

        return when (msg) {
            is SourceMessage.Tx -> {
                msg.txOps.asChannel.use { ch ->
                    Relation.StreamLoader(allocator, ch).use { loader ->
                        Relation(allocator, loader.schema).use { rel ->
                            loader.loadNextPage(rel)

                            val userMetadata = msg.userMetadata?.let { deserializeUserMetadata(allocator, it) }

                            indexSourceLogTx(
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

                            val userMetadata = rel.vectorForOrNull("user-metadata")?.getObject(0)
                            val user = rel.vectorForOrNull("user")?.getObject(0) as String?

                            indexSourceLogTx(
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

    override suspend fun processRecords(records: List<Log.Record<SourceMessage>>) {
        maybeFlushBlock()

        // Await the batch through the persister rather than firing and returning. The persister still
        // resolves + imports on its own thread (the heavy work is off the poll thread), but blocking
        // here until it's done keeps the shared consumer's poll loop and the persister in lock-step:
        // whenever the poll thread is back in `poll()` — where Kafka runs rebalance callbacks, which
        // run a leader/follower transition under `runBlocking` — the persister is quiescent, so a
        // concurrent DETACH/shutdown that must cancel-join the term doesn't wedge against in-flight
        // import work on a starved dispatcher (#5741).
        if (records.isNotEmpty()) submit(SourceLogTask.Batch(records))
    }

    override fun close() {
        extSource?.close()
        replicaProducer.close()
        allocator.close() // last: Arrow won't close it while a child buffer is live
    }
}
