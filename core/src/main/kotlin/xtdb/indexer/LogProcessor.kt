package xtdb.indexer

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import xtdb.api.log.*
import xtdb.api.log.Log.AtomicProducer.Companion.withTx
import xtdb.database.DatabaseState
import xtdb.database.DatabaseStorage
import xtdb.error.Interrupted
import xtdb.util.closeOnCatch
import xtdb.util.debug
import xtdb.util.error
import xtdb.util.info
import xtdb.util.logger

private val LOG = LogProcessor::class.logger

class LogProcessor(
    private val procFactory: ProcessorFactory,
    dbStorage: DatabaseStorage,
    private val dbState: DatabaseState,
    private val watchers: Watchers,
    private val blockUploader: BlockUploader,
    private val scope: CoroutineScope,
    meterRegistry: MeterRegistry? = null,
) : Log.SubscriptionListener<SourceMessage> {

    interface Processor<M> : Log.RecordProcessor<M> {
        val latestReplicaMsgId: MessageId
        suspend fun cancelAndJoin()
    }

    interface LeaderProcessor : Processor<SourceMessage> {
        val pendingBlock: PendingBlock?
    }

    interface TransitionProcessor : Processor<ReplicaMessage>

    interface FollowerProcessor : Processor<ReplicaMessage> {
        val pendingBlock: PendingBlock?
        suspend fun awaitReplicaMsgId(target: MessageId)
        fun notifyError(e: Throwable)
    }

    private val dbName = dbState.name
    private val replicaLog = dbStorage.replicaLog

    interface ProcessorFactory {
        fun openLeaderSystem(
            replicaProducer: Log.AtomicProducer<ReplicaMessage>,
            afterReplicaMsgId: MessageId,
        ): LeaderSystem

        fun openTransition(
            replicaProducer: Log.AtomicProducer<ReplicaMessage>,
            afterReplicaMsgId: MessageId,
        ): TransitionProcessor

        fun openFollower(
            pendingBlock: PendingBlock?,
            afterReplicaMsgId: MessageId,
        ): FollowerProcessor
    }

    sealed interface SubSystem {
        suspend fun cancelAndJoin()
    }

    interface LeaderSystem : SubSystem {
        val proc: LeaderProcessor
    }

    private class FollowerSystem(val proc: FollowerProcessor, private val job: Job) : SubSystem {
        override suspend fun cancelAndJoin() {
            job.cancelAndJoin()
            proc.cancelAndJoin()
        }
    }

    private fun openFollowerSystem(
        latestReplicaMsgId: MessageId,
        pendingBlock: PendingBlock? = null,
    ): FollowerSystem {
        val proc = procFactory.openFollower(pendingBlock, latestReplicaMsgId)
        return try {
            LOG.info {
                buildString {
                    append("[$dbName] starting follower: ")
                    append("pending block: ${pendingBlock != null}, ")
                    append("src: ${watchers.latestSourceMsgId}, ")
                    append("replica: $latestReplicaMsgId")
                }
            }

            FollowerSystem(proc, scope.launch {
                try {
                    replicaLog.tailAll(latestReplicaMsgId, proc)
                } catch (e: Throwable) {
                    proc.notifyError(e); throw e
                }
            })
        } catch (t: Throwable) {
            runBlocking { proc.cancelAndJoin() }
            throw t
        }
    }

    @Volatile
    private var sys: SubSystem =
        openFollowerSystem(dbState.blockCatalog.boundaryReplicaMsgId ?: -1)

    init {
        meterRegistry?.let { reg ->
            Gauge.builder("xtdb.log.leader", this) { if (it.sys is LeaderSystem) 1.0 else 0.0 }
                .description("1 if this node is the log leader, 0 if follower")
                .tag("db", dbState.name)
                .register(reg)
        }
    }

    override suspend fun onPartitionsAssigned(partitions: Collection<Int>): Log.TailSpec<SourceMessage>? {
        if (partitions != listOf(0)) return null

        return when (val oldSys = sys) {
            is LeaderSystem -> {
                LOG.info("[$dbName] partitions assigned: $partitions — already leader, no transition needed")
                null
            }

            is FollowerSystem -> {
                LOG.info("[$dbName] partitions assigned: $partitions — transitioning to leader")

                try {
                    replicaLog.openAtomicProducer("${dbState.name}-leader").closeOnCatch { replicaProducer ->
                        val followerProc = oldSys.proc

                        // Send a NoOp to get a known msgId we can await —
                        // we can't use latestSubmittedMsgId because Kafka's endOffsets
                        // includes transaction marker offsets that consumers never deliver.
                        val replayTarget = replicaProducer.withTx { it.appendMessage(ReplicaMessage.NoOp()) }.await().msgId

                        followerProc.awaitReplicaMsgId(replayTarget)
                        LOG.debug("[$dbName] transition: closing follower system")
                        oldSys.cancelAndJoin()

                        val pendingBlock = followerProc.pendingBlock

                        val transition = procFactory.openTransition(replicaProducer, followerProc.latestReplicaMsgId)
                        try {
                            if (pendingBlock != null) {
                                LOG.debug("[$dbName] transition: finishing pending block b${pendingBlock.blockIdx} with ${pendingBlock.bufferedRecords.size} buffered records")
                                blockUploader.uploadBlock(
                                    replicaProducer, pendingBlock.boundaryMsgId, pendingBlock.boundaryMessage,
                                )
                                LOG.debug("[$dbName] transition: replaying ${pendingBlock.bufferedRecords.size} buffered records through transition processor")
                                transition.processRecords(pendingBlock.bufferedRecords)
                            }

                            LOG.debug("[$dbName] transition: opening leader processor")

                            val sys = procFactory.openLeaderSystem(replicaProducer, replayTarget)
                            this.sys = sys

                            val resumeMsgId = watchers.latestSourceMsgId
                            LOG.info("[$dbName] leader startup complete, resuming after $resumeMsgId")
                            Log.TailSpec(resumeMsgId, sys.proc)
                        } finally {
                            transition.cancelAndJoin()
                        }
                    }
                } catch (e: InterruptedException) {
                    throw e
                } catch (e: Interrupted) {
                    throw e
                } catch (e: Throwable) {
                    LOG.error(e, "[$dbName] transition: failed to transition to leader")
                    watchers.notifyError(e)
                    throw e
                }
            }
        }
    }

    override suspend fun onPartitionsRevoked(partitions: Collection<Int>) {
        if (partitions != listOf(0)) return

        when (val oldSys = sys) {
            is LeaderSystem -> {
                LOG.info("[$dbName] partitions revoked: $partitions — was leader, transitioning to follower")
                // Close first: Kafka guarantees no concurrent processing during rebalance,
                // and close() only releases the allocator — watermark fields remain readable.
                oldSys.cancelAndJoin()
                val proc = oldSys.proc
                this.sys = openFollowerSystem(proc.latestReplicaMsgId, proc.pendingBlock)
            }

            is FollowerSystem -> {
                LOG.debug("[$dbName] partitions revoked: $partitions — already follower, no transition needed")
            }
        }
    }

    suspend fun cancelAndJoin() {
        sys.cancelAndJoin()
    }

    /**
     * Run one cycle of every garbage collector owned by the leader (block + trie) and wait for
     * both. No-op on follower systems — GC only runs on the leader. Bypasses the collectors'
     * `enabled` flag (which gates the auto-signal from the block-boundary path, not direct calls).
     */
    fun gcAll() {
        val proc = (sys as? LeaderSystem)?.proc as? LeaderLogProcessor ?: return
        proc.blockGc.awaitNoGarbageBlocking()
        proc.trieGc.awaitNoGarbageBlocking()
    }
}
