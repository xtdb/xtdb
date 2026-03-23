package xtdb.indexer

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import xtdb.api.log.*
import xtdb.api.log.Log.AtomicProducer.Companion.withTx
import xtdb.database.DatabaseState
import xtdb.database.DatabaseStorage
import xtdb.util.StringUtil.asLexHex
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

    interface LeaderProcessor : Log.RecordProcessor<SourceMessage>, AutoCloseable {
        val pendingBlock: PendingBlock?
    }

    interface TransitionProcessor : Log.RecordProcessor<ReplicaMessage>, AutoCloseable

    interface FollowerProcessor : Log.RecordProcessor<ReplicaMessage>, AutoCloseable {
        val pendingBlock: PendingBlock?
    }

    private val replicaLog = dbStorage.replicaLog

    interface ProcessorFactory {
        fun openLeaderProcessor(
            replicaProducer: Log.AtomicProducer<ReplicaMessage>,
        ): LeaderProcessor

        fun openTransition(
            replicaProducer: Log.AtomicProducer<ReplicaMessage>,
        ): TransitionProcessor

        fun openFollower(
            pendingBlock: PendingBlock?,
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

    private fun openFollowerSystem(pendingBlock: PendingBlock? = null): FollowerSystem =
        procFactory.openFollower(pendingBlock).closeOnCatch { proc ->
            FollowerSystem(proc, replicaLog.tailAll(watchers.latestReplicaMsgId, proc))
        }

    @Volatile
    private var sys: SubSystem = openFollowerSystem()

    init {
        val blockCatalog = dbState.blockCatalog
        LOG.info(
            "starting follower — block: ${blockCatalog.currentBlockIndex?.asLexHex}, " +
                    "source: ${blockCatalog.latestProcessedMsgId}, " +
                    "replica: ${blockCatalog.boundaryReplicaMsgId}"
        )

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

                    val pendingBlock = oldSys.proc.pendingBlock
                    LOG.debug("transition: closing follower system")
                    oldSys.close()

                    procFactory.openTransition(replicaProducer)
                        .use { transition ->
                            if (pendingBlock != null) {
                                LOG.debug("transition: finishing pending block b${pendingBlock.blockIdx} with ${pendingBlock.bufferedRecords.size} buffered records")
                                val uploadedReplicaMsgId = blockUploader.uploadBlock(
                                    replicaProducer,
                                    pendingBlock.boundaryMsgId,
                                    pendingBlock.boundaryMessage,
                                )
                                watchers.notifyMsg(null, uploadedReplicaMsgId)
                                LOG.debug("transition: replaying ${pendingBlock.bufferedRecords.size} buffered records through transition processor")
                                transition.processRecords(pendingBlock.bufferedRecords)
                                watchers.sync()
                            }

                            val latestSourceMsgId = watchers.latestSourceMsgId
                            LOG.debug("transition: opening leader processor")

                            val proc = procFactory.openLeaderProcessor(replicaProducer)
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

        LOG.debug("partitions revoked: $partitions — transitioning to follower")
        when (val oldSys = sys) {
            is LeaderSystem -> {
                LOG.info("partitions revoked: $partitions — was leader, transitioning to follower")
                val pendingBlock = oldSys.proc.pendingBlock
                oldSys.close()
                watchers.sync()
                LOG.debug("revocation: pending block: ${pendingBlock != null}, opening follower system")
                this.sys = openFollowerSystem(pendingBlock)
            }

            is FollowerSystem -> {
                LOG.info("partitions revoked: $partitions — already follower, no transition needed")
            }
        }
    }

    override fun close() {
        sys.close()
    }
}
