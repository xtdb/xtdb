package xtdb.indexer

import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.apache.arrow.memory.BufferAllocator
import xtdb.api.DatabaseName
import xtdb.api.TransactionKey
import xtdb.api.TransactionResult
import xtdb.api.log.*
import xtdb.api.storage.Storage
import xtdb.block.proto.Block.parseFrom
import xtdb.catalog.TableCatalog.Companion.blockFilePath
import xtdb.compactor.Compactor
import xtdb.database.Database
import xtdb.database.PartitionState
import xtdb.api.error.Anomaly
import xtdb.types.LogTimestamp
import xtdb.types.MessageId
import xtdb.log.proto.TrieDetails
import xtdb.storage.BufferPool
import xtdb.table.fromSchemaAndTable
import xtdb.util.StringUtil.asLexHex
import xtdb.util.closeAll
import xtdb.util.debug
import xtdb.util.error
import xtdb.util.logger
import xtdb.util.trace

private val LOG = FollowerLogProcessor::class.logger

class FollowerLogProcessor @JvmOverloads constructor(
    allocator: BufferAllocator,
    private val bufferPool: BufferPool,
    private val partitionState: PartitionState,
    private val dbName: DatabaseName,
    private val compactor: Compactor.ForDatabase,
    private val watchers: Watchers,
    private val dbCatalog: Database.Catalog?,
    pendingBlock: PendingBlock?,
    private val termFence: TermFence,
    private val hasExternalSource: Boolean,
    private val meterRegistry: MeterRegistry? = null,
    private val maxBufferedRecords: Int = 1024,
) : AutoCloseable {

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

    private val tableCatalog = partitionState.tableCatalog
    private val trieCatalog = partitionState.trieCatalog
    private val liveIndex = partitionState.liveIndex

    private val allocator = allocator.newChildAllocator("follower-log-processor", 0, Long.MAX_VALUE)

    private fun addTries(tries: List<TrieDetails>, logTimestamp: LogTimestamp) {
        tries.groupBy { it.tableName }.forEach { (tableName, tries) ->
            trieCatalog.addTries(fromSchemaAndTable(tableName), tries, logTimestamp)
        }
    }

    private val ReplicaMessage.stale
        get() =
            when (this) {
                is ReplicaMessage.ResolvedTx -> txId <= watchers.latestTxId
                is ReplicaMessage.TriesAdded -> sourceMsgId <= watchers.latestSourceMsgId
                is ReplicaMessage.BlockBoundary -> blockIndex <= (tableCatalog.currentBlockIndex ?: -1)
                is ReplicaMessage.BlockUploaded -> blockIndex <= (tableCatalog.currentBlockIndex ?: -1)
                is ReplicaMessage.NoOp -> srcMsgId != null && srcMsgId <= watchers.latestSourceMsgId
                // `trieCatalog.deleteTries` is set-removal — idempotent — so replay is always safe.
                is ReplicaMessage.TriesDeleted -> false
            }

    private suspend fun processRecord(record: Log.Record<ReplicaMessage>, replicaMsgId: MessageId?) {
        when (val msg = record.message) {
            is ReplicaMessage.ResolvedTx -> resolvedTxTimer.timed {
                val systemTime = msg.systemTime
                val txKey = TransactionKey(msg.txId, systemTime)

                val tables = msg.loadTableData(allocator)
                try {
                    liveIndex.commitTx(txKey, tables)
                } finally {
                    tables.closeAll()
                }

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
                watchers.notifyApplied(replicaMsgId, effectiveSrcMsgId, result, msg.externalSourceToken)
            }

            is ReplicaMessage.TriesAdded -> triesAddedTimer.timed {
                if (msg.storageVersion == Storage.VERSION && msg.storageEpoch == bufferPool.epoch)
                    addTries(msg.tries, record.logTimestamp)

                watchers.notifyApplied(replicaMsgId, msg.sourceMsgId)
            }

            is ReplicaMessage.BlockBoundary -> blockBoundaryTimer.timed {
                pendingBlock = PendingBlock(record.msgId, msg, maxBufferedRecords)
                LOG.debug("[$dbName] block boundary b${msg.blockIndex.asLexHex}: source=${msg.latestProcessedMsgId}, replica=${record.msgId} — waiting for BlockUploaded...")
                watchers.notifyApplied(replicaMsgId, msg.latestProcessedMsgId)
                blockBufferStartSample = meterRegistry?.let { Timer.start(it) }
            }

            is ReplicaMessage.BlockUploaded -> error(
                "BlockUploaded should be handled by handleRecord, never reaching processRecord directly. msgId=${record.msgId}, blockIndex=${msg.blockIndex.asLexHex}, latestProcessedMsgId=${msg.latestProcessedMsgId}"
            )

            is ReplicaMessage.NoOp -> watchers.notifyApplied(replicaMsgId, msg.srcMsgId)

            is ReplicaMessage.TriesDeleted -> triesDeletedTimer.timed {
                trieCatalog.deleteTries(fromSchemaAndTable(msg.tableName), msg.trieKeys)
                watchers.notifyApplied(replicaMsgId)
            }
        }

    }

    private suspend fun handleRecord(record: Log.Record<ReplicaMessage>, replicaMsgId: MessageId?) {
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
                    tableCatalog.refresh(block, liveIndex.blockMetadata())
                    liveIndex.nextBlock()
                    compactor.signalBlock()

                    val bufferedRecords = pendingBlock.bufferedRecords
                    bufferedRecordsSummary?.record(bufferedRecords.size.toDouble())
                    blockBufferTimer?.let { blockBufferStartSample?.stop(it) }
                    blockBufferStartSample = null
                    this.pendingBlock = null
                    bufferedRecords
                }

                // Replayed with no consume position: the position counted these when they were first
                // read and held, so re-notifying it here would walk it backwards.
                bufferedRecords.forEach { held -> handleRecord(held, null) }
            } else {
                LOG.trace { "[$dbName] follower: buffering message ${record.msgId} (${msg::class.simpleName}) during pending block b${pendingBlockIdx} (${pendingBlock.bufferedRecords.size + 1} buffered)" }
                pendingBlock += record
            }

            watchers.notifyApplied(replicaMsgId)
            return
        }

        if (msg.stale) watchers.notifyApplied(replicaMsgId) else processRecord(record, replicaMsgId)
    }

    suspend fun processRecords(records: List<Log.Record<ReplicaMessage>>) {
        for (record in records) {
            try {
                val term = record.message.termId
                if (termFence.admit(term)) handleRecord(record, record.msgId)
                else {
                    // Fenced: a higher-term leader has superseded this message's writer. Discard it, but
                    // still advance the consume position (discard suppresses application, not consumption)
                    // so a transition catch-up can't hang on a fenced no-op.
                    LOG.debug {
                        "[$dbName] follower: discarding fenced record ${record.msgId} " +
                                "(term ${LeaderTerm.format(term)} < ${LeaderTerm.format(termFence.highestSeen)})"
                    }
                    watchers.notifyApplied(record.msgId)
                }
            } catch (e: Throwable) {
                if (e.isShutdownSignal) throw e

                LOG.error(
                    e,
                    "[$dbName] follower: failed to process log record with msgId ${record.msgId} (${record.message::class.simpleName})"
                )
                watchers.notifyError(e)
                throw e
            }
        }
    }

    override fun close() {
        allocator.close()
    }
}
