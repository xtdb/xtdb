package xtdb.indexer

import io.micrometer.core.instrument.Counter
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.selects.selectUnbiased
import org.apache.arrow.memory.BufferAllocator
import xtdb.Metrics.withSpan
import xtdb.NodeBase
import xtdb.api.TransactionKey
import xtdb.api.TransactionResult
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
) : LogProcessor.Processor<SourceMessage>, TxIndexer {

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

    var pendingBlock: PendingBlock? = null
        private set

    override var latestReplicaMsgId: MessageId = afterReplicaMsgId
        private set

    private val blockFlusher = BlockFlusher(flushTimeout, blockCatalog)

    // Resolver-owned staging area: resolved-but-not-yet-durable txs, seeded at the durable head. The
    // resolver is its sole accessor (no lock). Freed in close() (phase 2), once the resolver job is
    // joined so nothing live still touches it; see StagingIndex.
    private val stagingIndex = StagingIndex(allocator, liveIndex.latestCompletedTx)

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

    // capacity 1 so a fire-and-forget `submitTx` caller can queue one tx ahead while the persister works the
    // current one (bounding lookahead to ~2). `executeTx` still blocks on the result regardless of capacity.
    private val extSourceCh =
        Channel<ExtSourceTask>(capacity = 1, onUndeliveredElement = { it.onComplete.cancel() })
    private val gcCh =
        Channel<GcTask>(onUndeliveredElement = { it.onComplete.cancel() })

    private suspend fun handleSourceLogBatch(records: List<Log.Record<SourceMessage>>) {
        for (record in records) {
            LOG.trace { "[$dbName] leader: message ${record.msgId} (${record.message::class.simpleName})" }
            handleSourceLogRecord(record)
        }

        // Poll-boundary drain: flush any accumulated tail so the batch task returns with the resolver
        // quiescent — nothing in flight when the poll loop re-enters poll() and Kafka may run a
        // rebalance/transition under runBlocking (#5741).
        drainStaging()
    }

    private suspend fun handleSourceLogRecord(record: Log.Record<SourceMessage>) {
        val msgId = record.msgId
        val msg = record.message

        // Data txs accumulate into the staging batch and are committed together at the next drain. Every
        // other message is a boundary that must first drain the accumulated txs: a control message's
        // replica-log append has to land after the appends of the txs that preceded it in source order,
        // and a block cut needs those txs durable before the block is written.
        if (msg !is SourceMessage.Tx && msg !is SourceMessage.LegacyTx) drainStaging()

        when (msg) {
            is SourceMessage.Tx, is SourceMessage.LegacyTx -> {
                val (txResult, openTx) = resolveTx(msgId, record, msg)
                openTx.use {
                    stagingIndex.stage(it, msgId, txResult, dbOp = null)
                }
            }

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

                openTx(txKey, null).use { openTx ->
                    openTx.writeTxRow(error, null)
                    val txResult = if (error == null) Committed(txKey) else Aborted(txKey, error)
                    val dbOp = if (error == null) DbOp.Attach(msg.dbName, msg.config) else null
                    stagingIndex.stage(openTx, msgId, txResult, dbOp)
                }
                drainStaging()
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

                openTx(txKey, null).use { openTx ->
                    openTx.writeTxRow(error, null)
                    val txResult = if (error == null) Committed(txKey) else Aborted(txKey, error)
                    val dbOp = if (error == null) DbOp.Detach(msg.dbName) else null
                    stagingIndex.stage(openTx, msgId, txResult, dbOp)
                }
                drainStaging()
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

    // Seal the accumulated batch, make it durable, and promote it into the durable live index. Each tx
    // serializes its own table data into a replica message here (ResolvedTx.toReplicaMessage) — at seal,
    // off the resolver's resolve path — rather than eagerly when it was resolved.
    //
    // Seal + commit is synchronous for now (one producer transaction per batch, committed here on the
    // resolver); the committer-coroutine offload + cross-batch accumulation come with the pipeline step.
    // Promotion is per-tx in send order, post-durable: a mid-batch import fault leaves the cursor at the
    // last tx that actually imported (partial-failure safety), with the un-imported tail freed below.
    private suspend fun drainStaging() {
        if (stagingIndex.isEmpty) return

        val batch = stagingIndex.drainAccumulated()
        try {
            val messages = batch.map { it.toReplicaMessage() }
            val handles = replicaProducer.withTx { tx -> messages.map { tx.appendMessage(it) } }

            for ((resolvedTx, handle) in batch.zip(handles)) {
                liveIndex.commitTx(resolvedTx.txKey, resolvedTx.allTables.associate { it.ref to it.relation })
                latestReplicaMsgId = handle.await().msgId
                watchers.notifyTx(resolvedTx.txResult, resolvedTx.srcMsgId, resolvedTx.externalSourceToken)
            }

            if (liveIndex.isFull()) {
                val last = batch.last()
                finishBlock(last.srcMsgId, last.externalSourceToken)
            }
        } finally {
            batch.closeAll()
        }
    }

    private suspend fun handleIndexTx(task: ExtSourceTask.IndexTx) {
        val txKey = TransactionKey(
            (stagingIndex.latestCompletedTx?.txId ?: -1) + 1,
            smoothSystemTime(task.systemTime ?: instantSource.instant())
        )

        var openTx = openTx(txKey, task.externalSourceToken)

        @Suppress("ConvertTryFinallyToUseCall") // because openTx is a var
        try {
            val writerResult = task.writer(openTx)
            val txResult: TransactionResult = when (writerResult) {
                is TxResult.Committed -> {
                    openTx.writeTxRow(null, writerResult.userMetadata)
                    Committed(txKey)
                }

                is TxResult.Aborted -> {
                    txErrorCounter?.increment()
                    openTx.close()
                    // fresh tx for the abort row — the original openTx may hold partial writes
                    openTx = openTx(txKey, task.externalSourceToken)
                    openTx.writeTxRow(writerResult.error, writerResult.userMetadata)
                    Aborted(txKey, writerResult.error)
                }
            }

            // Ext-source txs carry no source-log position of their own and track progress via
            // `externalSourceToken`, so they don't advance `latestSourceMsgId` (driven by the source log).
            // We do stamp the current source-log watermark onto the replicated record: without it a
            // follower's `latestSourceMsgId` lags between block boundaries, and on promotion it resumes
            // the source log from a stale point and replays an already-covered block boundary.
            val effectiveSrcMsgId = watchers.latestSourceMsgId
            stagingIndex.stage(openTx, effectiveSrcMsgId, txResult, dbOp = null)
            drainStaging()
            task.result.complete(writerResult)
        } finally {
            openTx.close()
        }
    }

    private suspend fun handleTriesDeleted(task: GcTask.TriesDeleted) {
        appendToReplica(ReplicaMessage.TriesDeleted(task.tableName.schemaAndTable, task.trieKeys))
        trieCatalog.deleteTries(task.tableName, task.trieKeys)
    }

    // Hand the task to the persister and return its completion handle. The caller decides whether to
    // await it: `executeTx`, GC and `processRecords` await (they need the work done before returning);
    // `submitTx` doesn't (fire-and-forget). Suspends only on the channel send (backpressure).
    private suspend fun enqueue(task: PersisterTask): Deferred<Unit> {
        when (task) {
            is SourceLogTask -> sourceLogCh.send(task)
            is ExtSourceTask -> extSourceCh.send(task)
            is GcTask -> gcCh.send(task)
        }
        return task.onComplete
    }

    private val txErrorCounter: Counter? = nodeBase.meterRegistry?.let { Counter.builder("tx.error").register(it) }

    // The term handle: a supervisor child of the Database scope, owning the persister body (launched
    // last, in the `init` below) and the GCs. A term-internal failure surfaces via `notifyError`
    // rather than cancelling the source-log subscription — its sibling under the Database scope's own
    // SupervisorJob — and `cancelAndJoin` reaps the whole term. See dev/doc/coroutines.adoc.
    private val termJob = SupervisorJob(scope.coroutineContext.job)

    // The GCs run under a SupervisorJob child of `termJob`, so one GC's failure cancels neither its
    // sibling nor the persister; cancelling `termJob` reaps them all.
    private val gcScope = scope + SupervisorJob(termJob)

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
            enqueue(GcTask.TriesDeleted(tableName, trieKeys)).await()
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

    // Launched last so every field the body reaches — e.g. blockGc/trieGc via finishBlock — is
    // initialised before the first record. Runs under `termJob`, so `cancelAndJoin` reaps it.
    init {
        CoroutineScope(scope.coroutineContext + termJob).launch {
            launch {
                // Close the channels with the failure cause so a subsequent `enqueue` send throws it rather
                // than a bare ClosedSendChannelException. An awaiting caller (`executeTx`, GC, `processRecords`)
                // sees the cause through its `await`; this close-with-cause is the safety net for fire-and-forget
                // `submitTx`, and for any caller's next send once the persister loop has exited.
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
                } catch (_: CancellationException) {
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
                supervisorScope {
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
    }

    private fun smoothSystemTime(systemTime: Instant): Instant {
        val lct = stagingIndex.latestCompletedTx?.systemTime ?: return systemTime
        val floor = fromMicros(lct.asMicros + 1)
        return if (systemTime.isBefore(floor)) floor else systemTime
    }

    private fun openTx(txKey: TransactionKey, externalSourceToken: ExternalSourceToken?) =
        OpenTx(allocator, nodeBase, dbStorage, dbState, txKey, externalSourceToken, tracer, stagingIndex.resolvedTxs)

    override suspend fun executeTx(
        externalSourceToken: ExternalSourceToken?, systemTime: Instant?,
        writer: suspend (OpenTx) -> TxResult,
    ): TxResult =
        ExtSourceTask.IndexTx(externalSourceToken, systemTime, writer)
            .also { enqueue(it).await() }
            .result.await()

    override suspend fun submitTx(
        externalSourceToken: ExternalSourceToken?, systemTime: Instant?,
        writer: suspend (OpenTx) -> TxResult,
    ) {
        // Fire-and-forget: enqueue and return without awaiting the completion handle. The task's
        // `result`/`onComplete` are completed by the persister but go unawaited here — an unrecoverable
        // failure closes the channel with its cause, so the next `enqueue` (this or `executeTx`) throws it.
        enqueue(ExtSourceTask.IndexTx(externalSourceToken, systemTime, writer))
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

        // Safe to call from inside a Persister task: signal() just enqueues a cycle on the GC's
        // own coroutine; its `commitTriesDeleted` callback submits a fresh task that won't run
        // until this one returns.
        blockGc.signal()
        trieGc.signal()
    }

    // A fresh tx committing a single skip / abort / invalid-system-time row, returned LIVE — the caller
    // stages it (which slices its writes) then closes it. On any failure the tx is closed here so nothing
    // leaks. `countError` mirrors the pre-staging behaviour: skipped txs don't count as errors.
    private fun commitStandaloneTx(
        txKey: TransactionKey, error: Throwable, userMetadata: Map<*, *>?, countError: Boolean,
    ): Pair<TransactionResult, OpenTx> {
        if (countError) txErrorCounter?.increment()
        val openTx = openTx(txKey, null)
        return try {
            openTx.writeTxRow(error, userMetadata)
            Aborted(txKey, error) to openTx
        } catch (e: Throwable) {
            openTx.close(); throw e
        }
    }

    // Resolves a source-log tx and returns its result alongside the LIVE OpenTx that produced it — the
    // resolver stages that OpenTx (taking independent slices of its writes) and closes it afterwards.
    // Any OpenTx not returned (an aborted tx's partial-write attempt, or one whose commit throws) is closed
    // here.
    private fun indexSourceLogTx(
        msgId: MessageId,
        msgTimestamp: Instant,
        txOps: VectorReader?,
        systemTime: Instant?,
        defaultTz: ZoneId?,
        user: String?,
        userMetadata: Any?,
    ): Pair<TransactionResult, OpenTx> = tracer.withSpan(
        "xtdb.transaction",
        attributes = mapOf("operations.count" to (txOps?.valueCount ?: 0).toString()),
    ) {
        val userMetadataMap = userMetadata as? Map<*, *>
        // The APPLIED head (staging), not the durable head: a tx must system-time-smooth against the
        // staged predecessors it resolves behind, which lead the durable index.
        val lcTx = stagingIndex.latestCompletedTx

        // If lc-tx's systemTime >= msgTimestamp, bump past it by 1µs; otherwise use msgTimestamp.
        // (`+1000ns` is `+1µs`.)
        val defaultSystemTime: Instant = lcTx?.systemTime?.let { lcSysTime ->
            if (lcSysTime >= msgTimestamp) lcSysTime.plusNanos(1_000) else null
        } ?: msgTimestamp

        // Specified system-time before lc-tx → invalid; abort with that error.
        // The aborted tx-key uses the *default* (smoothed) systemTime, not the rejected one,
        // so the tx-key still satisfies the monotonicity invariant.
        if (systemTime != null && lcTx != null && systemTime < lcTx.systemTime) {
            val err = Incorrect(
                "specified system-time older than current tx",
                "invalid-system-time",
                mapOf(
                    "tx-key" to TransactionKey(msgId, systemTime),
                    "latest-completed-tx" to lcTx,
                ),
            )
            LOG.warn { "specified system-time '$systemTime' older than current tx '$lcTx'" }

            return@withSpan commitStandaloneTx(
                TransactionKey(msgId, defaultSystemTime), err, userMetadataMap, countError = true
            )
        }

        val effectiveSystemTime = systemTime ?: defaultSystemTime
        val txKey = TransactionKey(msgId, effectiveSystemTime)

        if (txOps == null)
            return@withSpan commitStandaloneTx(txKey, SKIPPED_EXN, userMetadataMap, countError = false)

        val openTx = openTx(txKey, null)
        val result = try {
            val opts = SourceLogTxIndexer.TxOpts(
                txKey = txKey,
                currentTime = effectiveSystemTime,
                systemTime = effectiveSystemTime.asMicros,
                defaultTz = defaultTz,
                user = user,
            )
            sourceLogTxIndexer.ForTx(txOps, opts).indexTx(openTx)
        } catch (e: Throwable) {
            openTx.close(); throw e
        }

        when (result) {
            is TxResult.Committed ->
                try {
                    openTx.writeTxRow(null, userMetadataMap)
                    Committed(txKey) to openTx
                } catch (e: Throwable) {
                    openTx.close(); throw e
                }

            is TxResult.Aborted -> {
                LOG.debug(result.error) { "aborted tx" }
                // fresh tx for the abort row — the original openTx may hold partial writes
                openTx.close()
                commitStandaloneTx(txKey, result.error, userMetadataMap, countError = true)
            }
        }
    }

    private fun resolveTx(
        msgId: MessageId, record: Log.Record<SourceMessage>, msg: SourceMessage
    ): Pair<TransactionResult, OpenTx> {
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
        if (records.isNotEmpty()) enqueue(SourceLogTask.Batch(records)).await()
    }

    suspend fun cancelAndJoin() = termJob.cancelAndJoin()

    override fun close() {
        extSource?.close()
        replicaProducer.close()
        stagingIndex.close() // free any un-promoted staged slices before the allocator
        allocator.close() // last: Arrow won't close it while a child buffer is live
    }
}
