package xtdb.indexer

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import xtdb.api.log.*
import xtdb.api.log.Log.AtomicProducer.Companion.withTx
import xtdb.api.log.Log.Companion.tailAll
import xtdb.database.DatabaseState
import xtdb.database.DatabaseStorage
import xtdb.util.MsgIdUtil.offsetToMsgId
import xtdb.util.StringUtil.asLexHex
import xtdb.util.closeOnCatch
import xtdb.util.debug
import xtdb.util.info
import xtdb.util.logger
import kotlin.math.max

private val LOG = LogProcessor::class.logger

class LogProcessor(
    private val procFactory: ProcessorFactory,
    dbStorage: DatabaseStorage,
    private val dbState: DatabaseState,
    private val blockUploader: BlockUploader,
    meterRegistry: MeterRegistry? = null,
) : Log.SubscriptionListener, AutoCloseable {

    interface LeaderProcessor : Log.RecordProcessor<SourceMessage>, AutoCloseable

    interface FollowerProcessor : Log.RecordProcessor<ReplicaMessage>, AutoCloseable {
        val pendingBlock: PendingBlock?
        val latestSourceMsgId: MessageId
        suspend fun replayBuffered()
    }

    private val replicaLog = dbStorage.replicaLog

    interface ProcessorFactory {
        fun openLeaderSystem(
            replicaProducer: Log.AtomicProducer<ReplicaMessage>, replicaWatchers: Watchers,
            afterSourceMsgId: MessageId, afterReplicaMsgId: MessageId,
        ): SubSystem

        fun openFollower(
            replicaWatchers: Watchers, pendingBlock: PendingBlock?, afterSourceMsgId: MessageId
        ): FollowerProcessor
    }

    sealed interface SubSystem : AutoCloseable {
        val pendingBlock: PendingBlock?
    }

    interface LeaderSystem : SubSystem {
        val latestReplicaMsgId: MessageId
    }

    private class FollowerSystem(val proc: FollowerProcessor, private val sub: Log.Subscription) : SubSystem {
        override val pendingBlock: PendingBlock? get() = proc.pendingBlock

        private var unsubscribed = false

        fun unsubscribe() {
            if (!unsubscribed) {
                unsubscribed = true
                sub.close()
            }
        }

        override fun close() {
            unsubscribe()
            proc.close()
        }
    }

    private val initialReplicaMsgId =
        max(dbState.blockCatalog.boundaryReplicaMsgId ?: -1, offsetToMsgId(replicaLog.epoch, -1))

    private val replicaWatchers = Watchers(initialReplicaMsgId)

    private val afterSourceMsgId: MessageId = dbState.blockCatalog.latestProcessedMsgId ?: -1

    private fun openFollowerSystem(
        latestReplicaMsgId: MessageId,
        pendingBlock: PendingBlock? = null,
        afterSourceMsgId: MessageId = this.afterSourceMsgId
    ): FollowerSystem =
        procFactory.openFollower(replicaWatchers, pendingBlock, afterSourceMsgId).closeOnCatch { proc ->
            FollowerSystem(proc, replicaLog.tailAll(latestReplicaMsgId, proc))
        }

    @Volatile
    private var sys: SubSystem = openFollowerSystem(initialReplicaMsgId)

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

    override suspend fun onPartitionsAssigned(partitions: Collection<Int>) {
        if (partitions != listOf(0)) return

        this.sys = when (val oldSys = sys) {
            is LeaderSystem -> {
                LOG.info("partitions assigned: $partitions — already leader, no transition needed")
                oldSys
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
                    replicaWatchers.await(replayTarget)
                    LOG.debug("transition: replica watchers caught up to $replayTarget")

                    val followerProc = oldSys.proc

                    followerProc.pendingBlock?.let { b ->
                        LOG.debug("transition: unsubscribing follower")
                        oldSys.unsubscribe()
                        LOG.debug("transition: uploading pending block b${b.blockIdx} with ${b.bufferedRecords.size} buffered records")
                        blockUploader.uploadBlock(replicaProducer, b.boundaryMsgId, b.boundaryMessage, replicaWatchers)
                        LOG.debug("transition: replaying ${b.bufferedRecords.size} buffered records through follower processor")
                        followerProc.replayBuffered()
                    }

                    val latestSourceMsgId = followerProc.latestSourceMsgId
                    LOG.debug("transition: closing follower system")
                    oldSys.close()

                    procFactory.openLeaderSystem(replicaProducer, replicaWatchers, latestSourceMsgId, replayTarget)
                        .also { LOG.info("leader startup complete, resuming after $latestSourceMsgId") }
                }
            }
        }
    }

    override suspend fun onPartitionsRevoked(partitions: Collection<Int>) {
        if (partitions != listOf(0)) return

        LOG.debug("partitions revoked: $partitions — transitioning to follower")
        this.sys = when (val oldSys = sys) {
            is LeaderSystem -> {
                LOG.info("partitions revoked: $partitions — was leader, transitioning to follower")
                val pendingBlock = oldSys.pendingBlock
                val latestReplica = oldSys.latestReplicaMsgId
                oldSys.close()
                LOG.debug("revocation: pending block: ${pendingBlock != null}, opening follower system from $latestReplica")
                openFollowerSystem(latestReplica, pendingBlock)
            }

            is FollowerSystem -> {
                LOG.info("partitions revoked: $partitions — already follower, no transition needed")
                oldSys
            }
        }
    }

    override fun close() {
        sys.close()
        replicaWatchers.close()
    }
}
