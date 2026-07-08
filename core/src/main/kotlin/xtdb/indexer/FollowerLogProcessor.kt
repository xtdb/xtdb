package xtdb.indexer

import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.first
import org.apache.arrow.memory.BufferAllocator
import xtdb.api.TransactionKey
import xtdb.api.TransactionResult
import xtdb.api.log.*
import xtdb.api.storage.Storage
import xtdb.block.proto.Block.parseFrom
import xtdb.catalog.BlockCatalog.Companion.blockFilePath
import xtdb.compactor.Compactor
import xtdb.database.Database
import xtdb.database.DatabaseState
import xtdb.error.Anomaly
import xtdb.error.Interrupted
import xtdb.log.proto.TrieDetails
import xtdb.storage.BufferPool
import xtdb.table.TableRef
import xtdb.util.StringUtil.asLexHex
import xtdb.util.debug
import xtdb.util.error
import xtdb.util.logger
import xtdb.util.trace

private val LOG = FollowerLogProcessor::class.logger

class FollowerLogProcessor @JvmOverloads constructor(
    scope: CoroutineScope,
    allocator: BufferAllocator,
    replicaLog: Log<ReplicaMessage>,
    private val bufferPool: BufferPool,
    private val dbState: DatabaseState,
    private val compactor: Compactor.ForDatabase,
    private val watchers: Watchers,
    private val dbCatalog: Database.Catalog?,
    pendingBlock: PendingBlock?,
    afterReplicaMsgId: MessageId,
    private val hasExternalSource: Boolean,
    private val meterRegistry: MeterRegistry? = null,
    private val maxBufferedRecords: Int = 1024,
) : LogProcessor.Processor<ReplicaMessage> {

    private val dbName = dbState.name

    private fun processTimer(msgType: String): Timer? = meterRegistry?.let {
        Timer.builder("xtdb.replica.process.timer")
            .description("Time spent processing replica log records, by message type")
            .tag("db", dbName)
            .tag("msg.type", msgType)
            .publishPercentiles(0.75, 0.85, 0.95, 0.98, 0.99, 0.999)
            .register(it)
    }

    private val resolvedTxTimer = processTimer("ResolvedTx")
    private val triesAddedTimer = processTimer("TriesAdded")
    private val blockBoundaryTimer = processTimer("BlockBoundary")
    private val blockUploadedTimer = processTimer("BlockUploaded")
    private val triesDeletedTimer = processTimer("TriesDeleted")

    private val blockBufferTimer: Timer? = meterRegistry?.let {
        Timer.builder("xtdb.replica.block.buffer.timer")
            .description("Time the follower spends buffering records between BlockBoundary and BlockUploaded")
            .tag("db", dbName)
            .publishPercentiles(0.75, 0.85, 0.95, 0.98, 0.99, 0.999)
            .register(it)
    }

    private val bufferedRecordsSummary: DistributionSummary? = meterRegistry?.let {
        DistributionSummary.builder("xtdb.replica.block.buffered.records")
            .description("Number of records buffered while a follower waits for BlockUploaded")
            .tag("db", dbName)
            .register(it)
    }

    private var blockBufferStartSample: Timer.Sample? = null

    private inline fun <R> Timer?.timed(block: () -> R): R {
        if (this == null) return block()
        val sample = Timer.start(meterRegistry!!)
        try {
            return block()
        } finally {
            sample.stop(this)
        }
    }

    var pendingBlock: PendingBlock? = pendingBlock
        private set

    private sealed interface ReplicaState {
        data class Active(val msgId: MessageId) : ReplicaState
        data class Failed(val msgId: MessageId, val exception: Throwable) : ReplicaState
    }

    private val replicaState = MutableStateFlow<ReplicaState>(ReplicaState.Active(afterReplicaMsgId))

    private fun ReplicaState.activeOrThrow(): ReplicaState.Active = when (this) {
        is ReplicaState.Active -> this
        is ReplicaState.Failed -> throw exception
    }

    override val latestReplicaMsgId: MessageId get() = when (val s = replicaState.value) {
        is ReplicaState.Active -> s.msgId
        is ReplicaState.Failed -> s.msgId
    }

    private val blockCatalog = dbState.blockCatalog
    private val tableCatalog = dbState.tableCatalog
    private val trieCatalog = dbState.trieCatalog
    private val liveIndex = dbState.liveIndex

    private val allocator = allocator.newChildAllocator("follower-log-processor", 0, Long.MAX_VALUE)

    private fun addTries(tries: List<TrieDetails>, logTimestamp: LogTimestamp) {
        tries.groupBy { it.tableName }.forEach { (tableName, tries) ->
            trieCatalog.addTries(TableRef.parse(tableName), tries, logTimestamp)
        }
    }

    private val ReplicaMessage.stale get() =
        when (this) {
            is ReplicaMessage.ResolvedTx -> txId <= watchers.latestTxId
            is ReplicaMessage.TriesAdded -> sourceMsgId <= watchers.latestSourceMsgId
            is ReplicaMessage.BlockBoundary -> blockIndex <= (blockCatalog.currentBlockIndex ?: -1)
            is ReplicaMessage.BlockUploaded -> blockIndex <= (blockCatalog.currentBlockIndex ?: -1)
            is ReplicaMessage.NoOp -> srcMsgId != null && srcMsgId <= watchers.latestSourceMsgId
            // `trieCatalog.deleteTries` is set-removal — idempotent — so replay is always safe.
            is ReplicaMessage.TriesDeleted -> false
        }

    private suspend fun processRecord(record: Log.Record<ReplicaMessage>) {
        when (val msg = record.message) {
            is ReplicaMessage.ResolvedTx -> resolvedTxTimer.timed {
                liveIndex.importTx(msg)

                val systemTime = msg.systemTime
                val txKey = TransactionKey(msg.txId, systemTime)
                if (msg.committed) {
                    when (val dbOp = msg.dbOp) {
                        is DbOp.Attach -> if (dbCatalog != null) {
                            try {
                                dbCatalog.attach(dbOp.dbName, dbOp.config)
                            } catch (e: Anomaly.Caller) {
                                LOG.debug(e) { "[$dbName] follower: attach database '${dbOp.dbName}' failed" }
                            }
                        }
                        is DbOp.Detach -> if (dbCatalog != null) {
                            try {
                                dbCatalog.detach(dbOp.dbName)
                            } catch (e: Anomaly.Caller) {
                                LOG.debug(e) { "[$dbName] follower: detach database '${dbOp.dbName}' failed" }
                            }
                        }
                        null -> {}
                    }
                }

                val result =
                    if (msg.committed) TransactionResult.Committed(txKey)
                    else TransactionResult.Aborted(txKey, msg.error)

                // Handling for pre-`f3eb8d7d9` ResolvedTx records — see #5586.
                val effectiveSrcMsgId = msg.srcMsgId
                    ?: if (hasExternalSource) watchers.latestSourceMsgId else msg.txId
                watchers.notifyTx(result, effectiveSrcMsgId, msg.externalSourceToken)
            }

            is ReplicaMessage.TriesAdded -> triesAddedTimer.timed {
                if (msg.storageVersion == Storage.VERSION && msg.storageEpoch == bufferPool.epoch)
                    addTries(msg.tries, record.logTimestamp)

                watchers.notifyMsg(msg.sourceMsgId)
            }

            is ReplicaMessage.BlockBoundary -> blockBoundaryTimer.timed {
                pendingBlock = PendingBlock(record.msgId, msg, maxBufferedRecords)
                LOG.debug("[$dbName] block boundary b${msg.blockIndex.asLexHex}: source=${msg.latestProcessedMsgId}, replica=${record.msgId} — waiting for BlockUploaded...")
                watchers.notifyMsg(msg.latestProcessedMsgId)
                blockBufferStartSample = meterRegistry?.let { Timer.start(it) }
            }

            is ReplicaMessage.BlockUploaded -> error(
                "BlockUploaded should be handled by handleRecord, never reaching processRecord directly. msgId=${record.msgId}, blockIndex=${msg.blockIndex.asLexHex}, latestProcessedMsgId=${msg.latestProcessedMsgId}"
            )

            is ReplicaMessage.NoOp -> msg.srcMsgId?.let { watchers.notifyMsg(it) }

            is ReplicaMessage.TriesDeleted -> triesDeletedTimer.timed {
                trieCatalog.deleteTries(TableRef.parse(msg.tableName), msg.trieKeys)
            }
        }

    }

    private suspend fun handleRecord(record: Log.Record<ReplicaMessage>) {
        val msg = record.message
        LOG.trace { "[$dbName] follower: message ${record.msgId} (${msg::class.simpleName})" }

        pendingBlock?.let { pendingBlock ->
            val pendingBlockIdx = pendingBlock.blockIdx
            if (msg is ReplicaMessage.BlockUploaded
                && msg.blockIndex == pendingBlockIdx
                && msg.storageVersion == Storage.VERSION
                && msg.storageEpoch == bufferPool.epoch
            ) {
                LOG.debug("[$dbName] block uploaded b${msg.blockIndex.asLexHex}: source=${msg.latestProcessedMsgId}, replica=${record.msgId} (${pendingBlock.bufferedRecords.size} buffered)")
                val bufferedRecords = blockUploadedTimer.timed {
                    val block = parseFrom(bufferPool.getByteArray(blockFilePath(pendingBlockIdx)))

                    addTries(msg.tries, record.logTimestamp)
                    blockCatalog.refresh(block)
                    tableCatalog.updateFromBlockMetadata(blockCatalog.currentBlockIndex, liveIndex.blockMetadata())
                    liveIndex.nextBlock()
                    compactor.signalBlock()

                    val bufferedRecords = pendingBlock.bufferedRecords
                    bufferedRecordsSummary?.record(bufferedRecords.size.toDouble())
                    blockBufferTimer?.let { blockBufferStartSample?.stop(it) }
                    blockBufferStartSample = null
                    this.pendingBlock = null
                    bufferedRecords
                }

                // replay buffered records — their typed notifications advance the watermarks
                bufferedRecords.forEach { handleRecord(it) }
            } else {
                LOG.trace { "[$dbName] follower: buffering message ${record.msgId} (${msg::class.simpleName}) during pending block b${pendingBlockIdx} (${pendingBlock.bufferedRecords.size + 1} buffered)" }
                pendingBlock += record
            }

            return
        }

        if (!msg.stale) processRecord(record)
    }

    override suspend fun processRecords(records: List<Log.Record<ReplicaMessage>>) {
        for (record in records) {
            try {
                handleRecord(record)
                replicaState.value = ReplicaState.Active(record.msgId)
            } catch (e: CancellationException) {
                // The owner cancelled the term — not a processing failure, so don't poison the
                // shared watchers via notifyError.
                throw e
            } catch (e: InterruptedException) {
                throw e
            } catch (e: Interrupted) {
                throw e
            } catch (e: Throwable) {
                LOG.error(
                    e,
                    "[$dbName] follower: failed to process log record with msgId ${record.msgId} (${record.message::class.simpleName})"
                )
                replicaState.value = ReplicaState.Failed(record.msgId, e)
                watchers.notifyError(e)
                throw e
            }
        }
    }

    suspend fun awaitReplicaMsgId(target: MessageId) {
        LOG.debug("[$dbName] transition: awaiting replica watcher catch-up to $target")
        replicaState.first { it.activeOrThrow().msgId >= target }
        LOG.debug("[$dbName] transition: replica watchers caught up to $target")
    }

    private fun notifyError(e: Throwable) {
        replicaState.value = ReplicaState.Failed(latestReplicaMsgId, e)
    }

    // Launched last so every field the tail touches is initialised before the first record.
    init {
        scope.launch {
            try {
                replicaLog.tailAll(afterReplicaMsgId, this@FollowerLogProcessor)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Throwable) {
                notifyError(e); throw e
            }
        }
    }

    override fun close() {
        allocator.close()
    }
}
