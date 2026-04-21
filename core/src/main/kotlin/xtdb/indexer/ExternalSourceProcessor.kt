package xtdb.indexer

import clojure.lang.Keyword
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.apache.arrow.memory.BufferAllocator
import xtdb.ResultCursor
import xtdb.api.TransactionKey
import xtdb.api.TransactionResult
import xtdb.api.log.*
import xtdb.api.log.Log.AtomicProducer.Companion.withTx
import xtdb.api.log.ReplicaMessage.BlockBoundary
import xtdb.api.log.ReplicaMessage.TriesAdded
import xtdb.api.storage.Storage
import xtdb.arrow.RelationReader
import xtdb.database.DatabaseState
import xtdb.database.DatabaseStorage
import xtdb.database.ExternalSource
import xtdb.database.ExternalSource.TxResult
import xtdb.database.ExternalSourceToken
import xtdb.error.Interrupted
import xtdb.indexer.Indexer.Companion.addTxRow
import xtdb.query.IQuerySource
import xtdb.query.QueryOpts
import xtdb.table.TableRef
import xtdb.time.InstantUtil.asMicros
import xtdb.time.InstantUtil.fromMicros
import xtdb.util.StringUtil.asLexHex
import xtdb.util.debug
import xtdb.util.error
import xtdb.util.logger
import xtdb.util.trace
import java.time.Duration
import java.time.Instant
import java.time.InstantSource
import kotlin.coroutines.CoroutineContext

private val LOG = ExternalSourceProcessor::class.logger

class ExternalSourceProcessor(
    private val allocator: BufferAllocator,
    private val dbStorage: DatabaseStorage,
    private val replicaProducer: Log.AtomicProducer<ReplicaMessage>,
    private val dbState: DatabaseState,
    private val watchers: Watchers,
    private val blockUploader: BlockUploader,
    private val querySource: IQuerySource,
    partition: Int,
    afterSourceMsgId: MessageId,
    afterReplicaMsgId: MessageId,
    private val extSource: ExternalSource,
    afterToken: ExternalSourceToken?,
    private val instantSource: InstantSource = InstantSource.system(),
    flushTimeout: Duration = Duration.ofMinutes(5),
    ctx: CoroutineContext = Dispatchers.Default,
) : LogProcessor.LeaderProcessor {

    init {
        require(dbState.name != "xtdb") {
            "ExternalSourceProcessor cannot run on the primary 'xtdb' database — primary handles AttachDatabase / DetachDatabase which external-source DBs never see."
        }
    }

    private val dbName = dbState.name
    private val sourceLog = dbStorage.sourceLog
    private val bufferPool = dbStorage.bufferPool
    private val liveIndex = dbState.liveIndex

    private val blockCatalog = dbState.blockCatalog
    private val trieCatalog = dbState.trieCatalog

    override var pendingBlock: PendingBlock? = null
        private set

    override var latestSourceMsgId: MessageId = afterSourceMsgId
        private set

    override var latestReplicaMsgId: MessageId = afterReplicaMsgId
        private set

    private val blockFlusher = BlockFlusher(flushTimeout, blockCatalog)

    // Serialises the two input streams — source-log records (via [processRecords]) and external-source txs
    // (via the subscription coroutine below).
    // The transactional Kafka producer (replicaProducer) is not safe for concurrent `withTx`, so both paths
    // must hold this lock before touching it.
    private val mutex = Mutex()

    private fun smoothSystemTime(systemTime: Instant): Instant {
        val lct = liveIndex.latestCompletedTx?.systemTime ?: return systemTime
        val floor = fromMicros(lct.asMicros + 1)
        return if (systemTime.isBefore(floor)) floor else systemTime
    }

    // Wraps the inner [OpenTx] and exposes the narrow surface source authors see:
    // writes via [table], reads via [openQuery]. The wrapper forwards query execution
    // to the node's [IQuerySource], scoped to this database with a snapshot that includes
    // the tx's in-flight writes.
    private inner class WriterOpenTx(val inner: OpenTx) : ExternalSource.OpenTx {
        override val systemFrom get() = inner.systemFrom

        override fun table(ref: TableRef) = inner.table(ref)
        override fun table(schemaName: String, tableName: String) =
            inner.table(TableRef(dbName, schemaName, tableName))

        override fun openQuery(
            sql: String,
            args: RelationReader?,
            opts: ExternalSource.QueryOpts,
        ): ResultCursor {
            val currentTime = opts.currentTime ?: inner.txKey.systemTime

            val snapSource = object : Snapshot.Source {
                override fun openSnapshot() = liveIndex.openSnapshot(inner)
            }
            val queryCat = Indexer.queryCatalog(dbStorage, dbState, snapSource)

            val prepareOpts = mapOf(
                Keyword.intern("current-time") to currentTime,
                Keyword.intern("default-tz") to opts.defaultTz,
                Keyword.intern("default-db") to dbName,
                Keyword.intern("query-text") to sql,
            )

            val pq = querySource.prepareQuery(sql, queryCat, prepareOpts)
            return pq.openQuery(args, QueryOpts(currentTime = currentTime, defaultTz = opts.defaultTz))
        }
    }

    private val txIndexer = object : ExternalSource.TxIndexer {
        override suspend fun indexTx(
            externalSourceToken: ExternalSourceToken?,
            systemTime: Instant?,
            writer: suspend (ExternalSource.OpenTx) -> TxResult,
        ): TxResult {
            val txId = (liveIndex.latestCompletedTx?.txId ?: -1) + 1
            val txKey = TransactionKey(txId, smoothSystemTime(systemTime ?: instantSource.instant()))

            try {
                val openTx = OpenTx(allocator, txKey)
                val result = try {
                    writer(WriterOpenTx(openTx))
                } catch (e: Throwable) {
                    openTx.close()
                    throw e
                }

                when (result) {
                    is TxResult.Committed -> openTx.use {
                        it.addTxRow(dbName, txKey, null, result.userMetadata)
                        commitAndPublish(it, txKey, result, externalSourceToken)
                    }

                    is TxResult.Aborted -> {
                        openTx.close()
                        OpenTx(allocator, txKey).use { abortTx ->
                            abortTx.addTxRow(dbName, txKey, result.error, result.userMetadata)
                            commitAndPublish(abortTx, txKey, result, externalSourceToken)
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
    }

    private val extJob = CoroutineScope(ctx).launch {
        try {
            extSource.onPartitionAssigned(partition, afterToken, txIndexer)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Throwable) {
            watchers.notifyError(e)
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
    }

    private suspend fun commitAndPublish(
        openTx: OpenTx,
        txKey: TransactionKey,
        result: TxResult,
        externalSourceToken: ExternalSourceToken?,
    ) {
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
                LOG.trace { "[${dbName}] external: message $msgId (${record.message::class.simpleName})" }

                try {
                    when (val msg = record.message) {
                        is SourceMessage.FlushBlock -> {
                            val expectedBlockIdx = msg.expectedBlockIdx
                            if (expectedBlockIdx != null && expectedBlockIdx == (blockCatalog.currentBlockIndex
                                    ?: -1L)
                            ) {
                                finishBlock(msgId, watchers.externalSourceToken)
                            }
                            latestSourceMsgId = msgId
                            watchers.notifyMsg(msgId)
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

                        // User txs and catalog mutations can't appear on an external-source DB's source log:
                        // - Tx / LegacyTx are blocked at Database.submitTxBlocking (see #5443)
                        // - AttachDatabase / DetachDatabase only target the primary DB, never external-source secondaries
                        is SourceMessage.Tx, is SourceMessage.LegacyTx,
                        is SourceMessage.AttachDatabase, is SourceMessage.DetachDatabase ->
                            error("[${dbName}] external-source DB received unexpected source message: ${msg::class.simpleName}")
                    }
                } catch (e: InterruptedException) {
                    throw e
                } catch (e: Interrupted) {
                    throw e
                } catch (e: Throwable) {
                    LOG.error(
                        e,
                        "[${dbName}] external: failed to process log record with msgId $msgId (${record.message::class.simpleName})"
                    )
                    watchers.notifyError(e)
                    throw e
                }
            }
        }
    }

    override fun close() {
        // HACK: we cancel without joining because a blocking join deadlocks under runTest's virtual time.
        extJob.cancel()
        extSource.close()
    }
}
