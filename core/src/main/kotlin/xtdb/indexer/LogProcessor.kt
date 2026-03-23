package xtdb.indexer

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import xtdb.api.log.*
import xtdb.api.log.Log.AtomicProducer.Companion.withTx
import xtdb.database.DatabaseState
import xtdb.database.DatabaseStorage
import xtdb.util.closeOnCatch
import xtdb.util.debug
import xtdb.util.info
import xtdb.util.logger

private val LOG = LogProcessor::class.logger

class LogProcessor(
    private val procFactory: ProcessorFactory,
    dbStorage: DatabaseStorage,
    private val dbState: DatabaseState,
    private val blockUploader: BlockUploader,
    private val watchers: Watchers,
    meterRegistry: MeterRegistry? = null,
) : Log.SubscriptionListener<SourceMessage>, AutoCloseable {

    interface Processor<M> : Log.RecordProcessor<M>, AutoCloseable {
        val latestSourceMsgId: MessageId
        val latestReplicaMsgId: MessageId
    }

    interface LeaderProcessor : Processor<SourceMessage> {
        val pendingBlock: PendingBlock?
    }

    interface TransitionProcessor : Processor<ReplicaMessage>

    interface FollowerProcessor : Processor<ReplicaMessage> {
        val pendingBlock: PendingBlock?
    }

    private val replicaLog = dbStorage.replicaLog

    interface ProcessorFactory {
        fun openLeaderProcessor(
            replicaProducer: Log.AtomicProducer<ReplicaMessage>,
            afterSourceMsgId: MessageId,
            afterReplicaMsgId: MessageId,
        ): LeaderProcessor

        fun openTransition(
            replicaProducer: Log.AtomicProducer<ReplicaMessage>,
            afterSourceMsgId: MessageId,
            afterReplicaMsgId: MessageId,
        ): TransitionProcessor

        fun openFollower(
            pendingBlock: PendingBlock?,
            afterSourceMsgId: MessageId,
            afterReplicaMsgId: MessageId,
        ): FollowerProcessor
    }

    private sealed interface SubSystem : AutoCloseable

    private class LeaderSystem(val proc: LeaderProcessor) : SubSystem {
        override fun close() = proc.close()
    }

    private class FollowerSystem(val proc: FollowerProcessor, private val sub: Log.Subscription) : SubSystem {
        override fun close() {
            sub.close()
            proc.close()
        }
    }

    private fun openFollowerSystem(
        latestSourceMsgId: MessageId,
        latestReplicaMsgId: MessageId,
        pendingBlock: PendingBlock? = null,
    ): FollowerSystem =
        procFactory.openFollower(pendingBlock, latestSourceMsgId, latestReplicaMsgId).closeOnCatch { proc ->
            LOG.info {
                buildString {
                    append("starting follower: ")
                    append("pending block: ${pendingBlock != null}, ")
                    append("src: $latestSourceMsgId, ")
                    append("replica: $latestReplicaMsgId")
                }
            }

            FollowerSystem(proc, replicaLog.tailAll(latestReplicaMsgId, proc))
        }

    @Volatile
    private var sys: SubSystem =
        dbState.blockCatalog.let { openFollowerSystem(it.latestProcessedMsgId ?: -1, it.boundaryReplicaMsgId ?: -1) }

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
                LOG.info("partitions assigned: $partitions — already leader, no transition needed")
                null
            }

            is FollowerSystem -> {
                LOG.info("partitions assigned: $partitions — transitioning to leader")

                // Fence: open atomic producer on replica log.
                LOG.debug("transition: opening atomic producer on replica log")
                replicaLog.openAtomicProducer("${dbState.name}-leader").closeOnCatch { replicaProducer ->
                    // Send a NoOp to get a known msgId we can await —
                    // we can't use latestSubmittedMsgId because Kafka's endOffsets
                    // includes transaction marker offsets that consumers never deliver.
                    val replayTarget = replicaProducer.withTx { it.appendMessage(ReplicaMessage.NoOp) }.await().msgId
                    LOG.debug("transition: awaiting replica watcher catch-up to $replayTarget (replica latest: ${replicaLog.latestSubmittedMsgId})")
                    watchers.awaitReplica(replayTarget)
                    LOG.debug("transition: replica watchers caught up to $replayTarget")

                    val followerProc = oldSys.proc
                    LOG.debug("transition: closing follower system")
                    oldSys.close()
                    val pendingBlock = followerProc.pendingBlock

                    procFactory.openTransition(
                        replicaProducer,
                        followerProc.latestSourceMsgId,
                        followerProc.latestReplicaMsgId
                    )
                        .use { transition ->
                            if (pendingBlock != null) {
                                LOG.debug("transition: finishing pending block b${pendingBlock.blockIdx} with ${pendingBlock.bufferedRecords.size} buffered records")
                                blockUploader.uploadBlock(
                                    replicaProducer, pendingBlock.boundaryMsgId, pendingBlock.boundaryMessage,
                                )
                                LOG.debug("transition: replaying ${pendingBlock.bufferedRecords.size} buffered records through transition processor")
                                transition.processRecords(pendingBlock.bufferedRecords)
                            }

                            val latestSourceMsgId = transition.latestSourceMsgId
                            LOG.debug("transition: opening leader processor")

                            val proc = procFactory.openLeaderProcessor(replicaProducer, latestSourceMsgId, replayTarget)
                            this.sys = LeaderSystem(proc)

                            LOG.info("leader startup complete, resuming after $latestSourceMsgId")
                            Log.TailSpec(latestSourceMsgId, proc)
                        }
                }
            }
        }
    }

    override suspend fun onPartitionsRevoked(partitions: Collection<Int>) {
        if (partitions != listOf(0)) return

        when (val oldSys = sys) {
            is LeaderSystem -> {
                LOG.info("partitions revoked: $partitions — was leader, transitioning to follower")
                oldSys.close() // close first so no further processing can change the watermarks
                val proc = oldSys.proc
                this.sys = openFollowerSystem(proc.latestSourceMsgId, proc.latestReplicaMsgId, proc.pendingBlock)
            }

            is FollowerSystem -> {
                LOG.debug("partitions revoked: $partitions — already follower, no transition needed")
            }
        }
    }

    override fun close() {
        sys.close()
    }
}
