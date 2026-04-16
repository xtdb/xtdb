package xtdb.indexer

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.apache.arrow.memory.BufferAllocator
import xtdb.api.TransactionKey
import xtdb.api.TransactionResult
import xtdb.api.log.*
import xtdb.api.log.Log.AtomicProducer.Companion.withTx
import xtdb.api.log.ReplicaMessage.BlockBoundary
import xtdb.api.log.ReplicaMessage.TriesAdded
import xtdb.api.storage.Storage
import xtdb.database.DatabaseState
import xtdb.database.DatabaseStorage
import xtdb.database.ExternalSource
import xtdb.database.ExternalSource.TxResult
import xtdb.database.ExternalSourceToken
import xtdb.error.Interrupted
import xtdb.indexer.Indexer.Companion.addTxRow
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
import kotlin.coroutines.CoroutineContext

private val LOG = ExternalSourceProcessor::class.logger

class ExternalSourceProcessor(
    private val allocator: BufferAllocator,
    dbStorage: DatabaseStorage,
    private val replicaProducer: Log.AtomicProducer<ReplicaMessage>,
    private val dbState: DatabaseState,
    private val watchers: Watchers,
    private val blockUploader: BlockUploader,
    afterSourceMsgId: MessageId,
    afterReplicaMsgId: MessageId,
    private val extSource: ExternalSource,
    afterToken: ExternalSourceToken?,
    partition: Int = 0,
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

    private val txIndexer = ExternalSource.TxIndexer { txId, systemTime, externalSourceToken, writer ->
        val txKey = TransactionKey(txId, smoothSystemTime(systemTime))

        try {
            val openTx = OpenTx(allocator, txKey)
            val result = try {
                writer(openTx)
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

            result
        } catch (e: CancellationException) {
            throw e
        } catch (e: Throwable) {
            watchers.notifyError(e)
            throw e
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
