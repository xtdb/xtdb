package xtdb.indexer

import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.apache.arrow.memory.BufferAllocator
import xtdb.api.DatabaseName
import xtdb.api.TransactionKey
import xtdb.api.TransactionResult
import xtdb.api.error.Anomaly
import xtdb.api.log.*
import xtdb.api.storage.Storage
import xtdb.block.proto.Block.parseFrom
import xtdb.catalog.BlockCatalog.Companion.blockFilePath
import xtdb.compactor.Compactor
import xtdb.database.Database
import xtdb.database.PartitionState
import xtdb.log.proto.TrieDetails
import xtdb.storage.BufferPool
import xtdb.table.fromSchemaAndTable
import xtdb.types.LogTimestamp
import xtdb.types.MessageId
import xtdb.util.StringUtil.asLexHex
import xtdb.util.closeAll
import xtdb.util.debug
import xtdb.util.logger
import xtdb.util.trace

private val LOG = ReplicaApplier::class.logger

/**
 * Applying one partition's replica log to local state: the term fence, the block hold, and the effects
 * each kind of record has.
 *
 * One of these per partition, for the whole life of the participant — so the read position and the block
 * hold below are the partition's, and a role change hands them over rather than copying them. Leading is
 * passed in per record, and reaches this class through the two hooks on [Leadership] and nowhere else.
 * See `allium/log-processor-lifecycle.allium`.
 */
internal class ReplicaApplier(
    allocator: BufferAllocator,
    private val bufferPool: BufferPool,
    partitionState: PartitionState,
    private val dbName: DatabaseName,
    private val compactor: Compactor.ForDatabase,
    private val watchers: Watchers,
    private val dbCatalog: Database.Catalog?,
    afterReplicaMsgId: MessageId,
    private val hasExternalSource: Boolean,
    private val meterRegistry: MeterRegistry? = null,
    private val maxBufferedRecords: Int = 1024,
) : AutoCloseable {

    private val allocator = allocator.newChildAllocator("replica-applier", 0, Long.MAX_VALUE)

    private val blockCatalog = partitionState.blockCatalog
    private val tableCatalog = partitionState.tableCatalog
    private val trieCatalog = partitionState.trieCatalog
    private val liveIndex = partitionState.liveIndex
    private val termFence = partitionState.termFence

    var latestReplicaMsgId: MessageId = afterReplicaMsgId
        private set

    var pendingBlock: PendingBlock? = null
        private set

    /**
     * Give up the hold on a block this participant has uploaded itself.
     *
     * The ordinary release ([releaseHeldBlock]) is driven by reading somebody's `BlockUploaded` back; an
     * author has no upload to wait for. The buffering timer is dropped rather than recorded for the same
     * reason — nothing was waited for, and leaving it running would date the next hold's measurement.
     */
    fun releaseHeldBlockAsAuthor() {
        pendingBlock = null
        blockBufferStartSample = null
    }

    // ---- metrics ----

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
            .description("Time spent buffering records between BlockBoundary and BlockUploaded")
            .tag("db", dbName)
            .publishPercentiles(0.75, 0.85, 0.95, 0.98, 0.99, 0.999)
            .register(it)
    }

    private val bufferedRecordsSummary: DistributionSummary? = meterRegistry?.let {
        DistributionSummary.builder("xtdb.replica.block.buffered.records")
            .description("Number of records buffered while waiting for BlockUploaded")
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

    // ---- staleness ----

    // Records at or below a watermark we already hold: replayed from a catch-up that started behind
    // where local state had already reached. Distinct from a fenced record, which is one a superseded
    // leader wrote and which no reader may ever apply.
    private val ReplicaMessage.stale
        get() = when (this) {
            is ReplicaMessage.ResolvedTx -> txId <= watchers.latestTxId
            is ReplicaMessage.TriesAdded -> sourceMsgId <= watchers.latestSourceMsgId
            is ReplicaMessage.BlockBoundary -> blockIndex <= (blockCatalog.currentBlockIndex ?: -1)
            is ReplicaMessage.BlockUploaded -> blockIndex <= (blockCatalog.currentBlockIndex ?: -1)
            is ReplicaMessage.NoOp -> srcMsgId != null && srcMsgId <= watchers.latestSourceMsgId
            // `trieCatalog.deleteTries` is set-removal — idempotent — so replay is always safe.
            is ReplicaMessage.TriesDeleted -> false
        }

    private fun addTries(tries: List<TrieDetails>, logTimestamp: LogTimestamp) {
        tries.groupBy { it.tableName }.forEach { (tableName, tries) ->
            trieCatalog.addTries(fromSchemaAndTable(tableName), tries, logTimestamp)
        }
    }

    // ---- applying a record ----

    private suspend fun applyFromRecord(record: Log.Record<ReplicaMessage>, leadership: Leadership?) {
        when (val msg = record.message) {
            is ReplicaMessage.ResolvedTx -> resolvedTxTimer.timed {
                val txKey = TransactionKey(msg.txId, msg.systemTime)

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
                                LOG.debug(e) { "[$dbName] attach database '${dbOp.dbName}' failed" }
                            }
                        }

                        is DbOp.Detach -> if (dbCatalog != null) {
                            try {
                                dbCatalog.detach(dbOp.dbName)
                            } catch (e: Anomaly.Caller) {
                                LOG.debug(e) { "[$dbName] detach database '${dbOp.dbName}' failed" }
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
                // Held first, then offered: whoever ends up taking the cut, the boundary is a block
                // being held from the instant it is read, which is what a demote mid-upload sees.
                pendingBlock = PendingBlock(record.msgId, msg, maxBufferedRecords)

                if (leadership?.takeCut(record, msg) == true) {
                    pendingBlock = null
                    watchers.notifyMsg(msg.latestProcessedMsgId)
                } else {
                    LOG.debug("[$dbName] block boundary b${msg.blockIndex.asLexHex}: source=${msg.latestProcessedMsgId}, replica=${record.msgId} — waiting for BlockUploaded...")
                    watchers.notifyMsg(msg.latestProcessedMsgId)
                    blockBufferStartSample = meterRegistry?.let { Timer.start(it) }
                }
            }

            // Reached only for an upload whose boundary we never held — a catch-up that started
            // between the two. The watermark still has to advance.
            is ReplicaMessage.BlockUploaded -> watchers.notifyMsg(msg.latestProcessedMsgId)

            is ReplicaMessage.NoOp -> msg.srcMsgId?.let { watchers.notifyMsg(it) }

            is ReplicaMessage.TriesDeleted -> triesDeletedTimer.timed {
                trieCatalog.deleteTries(fromSchemaAndTable(msg.tableName), msg.trieKeys)
            }
        }
    }

    // The cut we were waiting on has landed — so it was taken by whoever wrote the boundary, which was
    // not us.
    private suspend fun releaseHeldBlock(
        held: PendingBlock, record: Log.Record<ReplicaMessage>, msg: ReplicaMessage.BlockUploaded,
        leadership: Leadership?,
    ) {
        LOG.debug("[$dbName] block uploaded b${msg.blockIndex.asLexHex}: source=${msg.latestProcessedMsgId}, replica=${record.msgId} (${held.bufferedRecords.size} buffered)")

        val heldRecords = blockUploadedTimer.timed {
            val block = parseFrom(bufferPool.getByteArray(blockFilePath(held.blockIdx)))

            addTries(msg.tries, record.logTimestamp)
            blockCatalog.refresh(block)
            tableCatalog.updateFromBlockMetadata(blockCatalog.currentBlockIndex, liveIndex.blockMetadata())
            liveIndex.nextBlock()
            compactor.signalBlock()

            val heldRecords = held.bufferedRecords
            bufferedRecordsSummary?.record(heldRecords.size.toDouble())
            blockBufferTimer?.let { blockBufferStartSample?.stop(it) }
            blockBufferStartSample = null
            pendingBlock = null
            heldRecords
        }

        for (heldRecord in heldRecords) applyRecord(heldRecord, leadership)
    }

    private suspend fun applyRecord(record: Log.Record<ReplicaMessage>, leadership: Leadership?) {
        val msg = record.message
        LOG.trace { "[$dbName] replica: message ${record.msgId} (${msg::class.simpleName})" }

        pendingBlock?.let { held ->
            if (msg is ReplicaMessage.BlockUploaded
                && msg.blockIndex == held.blockIdx
                && msg.storageVersion == Storage.VERSION
                && msg.storageEpoch == bufferPool.epoch
            ) releaseHeldBlock(held, record, msg, leadership)
            else {
                LOG.trace { "[$dbName] replica: holding message ${record.msgId} (${msg::class.simpleName}) during block b${held.blockIdx} (${held.bufferedRecords.size + 1} held)" }
                held += record
            }

            return
        }

        // The first hook, asked before staleness and before the record's own contents: what leadership
        // holds is the record, and a record we have only just written is never a replay of something we
        // already had, whatever a watermark comparison would say of it.
        if (leadership?.applyAuthored(record) == true) return

        if (!msg.stale) applyFromRecord(record, leadership)
    }

    /**
     * Apply one record read off the replica log, and advance the consume position over it.
     *
     * A record below the fence is discarded rather than applied — it was written by a leader the log has
     * since moved past — but the position still advances, so a catch-up can't hang on a fenced record.
     */
    suspend fun apply(record: Log.Record<ReplicaMessage>, leadership: Leadership?) {
        val term = record.message.termId

        if (termFence.admit(term)) {
            // Above our own term, so a newer leader exists and we are finished. Raised before the record
            // is applied, because by then it is the new leader's to apply and we are not leading.
            leadership?.let {
                if (term > it.term)
                    throw LeaderSupersededException("[$dbName] superseded: read term $term > our term ${it.term} at ${record.msgId}")
            }

            applyRecord(record, leadership)
        } else {
            LOG.debug {
                "[$dbName] replica: discarding fenced record ${record.msgId} " +
                        "(term ${LeaderTerm.format(term)} < ${LeaderTerm.format(termFence.highest)})"
            }
        }

        latestReplicaMsgId = record.msgId
    }

    override fun close() {
        allocator.close()
    }
}
