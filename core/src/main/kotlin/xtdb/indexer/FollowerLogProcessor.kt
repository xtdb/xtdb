package xtdb.indexer

import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.first
import org.apache.arrow.memory.BufferAllocator
import xtdb.api.DatabaseName
import xtdb.api.log.*
import xtdb.compactor.Compactor
import xtdb.database.Database
import xtdb.database.PartitionState
import xtdb.api.error.Incorrect
import xtdb.api.error.Interrupted
import xtdb.types.MessageId
import xtdb.storage.BufferPool
import xtdb.util.debug
import xtdb.util.error
import xtdb.util.logger

private val LOG = FollowerLogProcessor::class.logger

/**
 * Reading one partition's replica log while this node is not leading: the tail, and the position
 * everything else waits on.
 *
 * Applying is [ReplicaApplier]'s, and is the same work a leader does on read-back — this class opens it
 * with no [Leadership], which is the whole of the difference.
 */
class FollowerLogProcessor @JvmOverloads constructor(
    allocator: BufferAllocator,
    replicaLog: PartitionLog<ReplicaMessage>,
    bufferPool: BufferPool,
    partitionState: PartitionState,
    private val dbName: DatabaseName,
    compactor: Compactor.ForDatabase,
    private val watchers: Watchers,
    dbCatalog: Database.Catalog?,
    pendingBlock: PendingBlock?,
    afterReplicaMsgId: MessageId,
    scope: CoroutineScope,
    hasExternalSource: Boolean,
    meterRegistry: MeterRegistry? = null,
    maxBufferedRecords: Int = 1024,
) : LogProcessor.Processor<ReplicaMessage> {

    private val applier = ReplicaApplier(
        allocator, "follower-log-processor", bufferPool, partitionState, dbName, compactor, watchers,
        dbCatalog, leadership = null,
        pendingBlock = pendingBlock, afterReplicaMsgId = afterReplicaMsgId,
        hasExternalSource = hasExternalSource,
        meterRegistry = meterRegistry,
        maxBufferedRecords = maxBufferedRecords,
    )

    val pendingBlock: PendingBlock? get() = applier.pendingBlock

    private val termFence = partitionState.termFence

    private sealed interface ReplicaState {
        data class Active(val msgId: MessageId) : ReplicaState
        data class Failed(val msgId: MessageId, val exception: Throwable) : ReplicaState
    }

    private val replicaState = MutableStateFlow<ReplicaState>(ReplicaState.Active(afterReplicaMsgId))

    private fun ReplicaState.activeOrThrow(): ReplicaState.Active = when (this) {
        is ReplicaState.Active -> this
        is ReplicaState.Failed -> throw exception
    }

    override val latestReplicaMsgId: MessageId get() = applier.latestReplicaMsgId

    /**
     * Refuse a leader term the replica log has already moved past: a claim stamped with it would be
     * discarded by every reader, so leading under it would index nothing.
     *
     * Call this once the claim itself has been read back, at which point it is expected to *be* the max
     * — so a higher one is someone else's. In practice that means the election counter regressed
     * underneath us, which is what the term's epoch is there to declare (see [LeaderTerm]). Refusing
     * costs liveness only and never safety, so unlike the fence's ordering it is sound to decide from
     * whatever we happen to have read.
     */
    fun checkTermUnfenced(term: Long) {
        val maxTerm = termFence.highest
        if (maxTerm > term)
            throw Incorrect(
                "[$dbName] leader term ${LeaderTerm.format(term)} is already fenced by " +
                        "${LeaderTerm.format(maxTerm)} on the replica log — the leader-election counter " +
                        "has regressed (a recreated Kafka consumer group, or a restarted local log), so " +
                        "bump the log's termEpoch above ${LeaderTerm.epochOf(maxTerm)}",
                "xtdb/leader-term-fenced",
                mapOf(
                    "db-name" to dbName,
                    "term" to LeaderTerm.format(term),
                    "fenced-by" to LeaderTerm.format(maxTerm),
                ),
            )
    }

    override suspend fun processRecords(records: List<Log.Record<ReplicaMessage>>) {
        for (record in records) {
            try {
                applier.apply(record)
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
    private val job = scope.launch {
        try {
            replicaLog.tailAll(afterReplicaMsgId, this@FollowerLogProcessor)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Throwable) {
            notifyError(e); throw e
        }
    }

    suspend fun cancelAndJoin() = job.cancelAndJoin()

    override fun close() = applier.close()
}
