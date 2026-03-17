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
    private val watchers: Watchers,
    private val blockFinisher: BlockFinisher,
    meterRegistry: MeterRegistry? = null,
) : Log.SubscriptionListener, AutoCloseable {

    interface LeaderProcessor : Log.RecordProcessor<SourceMessage>, AutoCloseable

    interface TransitionProcessor : Log.RecordProcessor<ReplicaMessage>, AutoCloseable

    interface FollowerProcessor : Log.RecordProcessor<ReplicaMessage>, AutoCloseable {
        val pendingBlock: PendingBlock?
    }

    private val replicaLog = dbStorage.replicaLog

    interface ProcessorFactory {
        fun openLeaderSystem(replicaProducer: Log.AtomicProducer<ReplicaMessage>, afterMsgId: MessageId): SubSystem

        fun openTransition(
            replicaProducer: Log.AtomicProducer<ReplicaMessage>, replicaWatchers: Watchers
        ): TransitionProcessor

        fun openFollower(replicaWatchers: Watchers, pendingBlock: PendingBlock?): FollowerProcessor
    }

    sealed interface SubSystem : AutoCloseable {
        val pendingBlock: PendingBlock?
    }

    interface LeaderSystem : SubSystem

    private class FollowerSystem(val proc: FollowerProcessor, private val sub: Log.Subscription) : SubSystem {
        override val pendingBlock: PendingBlock? get() = proc.pendingBlock

        override fun close() {
            sub.close()
            proc.close()
        }
    }

    private val replicaWatchers =
        Watchers(max(dbState.blockCatalog.boundaryReplicaMsgId ?: -1, offsetToMsgId(replicaLog.epoch, -1)))

    private fun openFollowerSystem(latestReplicaMsgId: MessageId, pendingBlock: PendingBlock? = null): FollowerSystem =
        procFactory.openFollower(replicaWatchers, pendingBlock).closeOnCatch { proc ->
            FollowerSystem(proc, replicaLog.tailAll(latestReplicaMsgId, proc))
        }

    @Volatile
    private var sys: SubSystem = openFollowerSystem(replicaWatchers.currentMsgId)

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
                    replicaWatchers.await0(replayTarget)
                    LOG.debug("transition: replica watchers caught up to $replayTarget")

                    val followerProc = oldSys.proc
                    LOG.debug("transition: closing follower system")
                    oldSys.close()
                    val pendingBlock = followerProc.pendingBlock

                    procFactory.openTransition(replicaProducer, replicaWatchers).use { transition ->
                        if (pendingBlock != null) {
                            LOG.debug("transition: finishing pending block b${pendingBlock.blockIdx} with ${pendingBlock.bufferedRecords.size} buffered records")
                            blockFinisher.finishBlock(
                                replicaProducer,
                                pendingBlock.boundaryMsgId,
                                pendingBlock.boundaryMessage
                            )
                            LOG.debug("transition: replaying ${pendingBlock.bufferedRecords.size} buffered records through transition processor")
                            transition.processRecords(pendingBlock.bufferedRecords)
                        }

                        LOG.debug("transition: syncing watchers")
                        val latestProcessedMsgId = watchers.sync()

                        LOG.debug("transition: opening leader processor")

                        procFactory.openLeaderSystem(replicaProducer, latestProcessedMsgId)
                            .also { LOG.info("leader startup complete, resuming after $latestProcessedMsgId") }
                    }
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
                LOG.debug("revocation: closing leader system (pendingBlock=${pendingBlock != null})")
                oldSys.close()
                val latestSubmitted = replicaLog.latestSubmittedMsgId
                LOG.debug("revocation: opening follower system from $latestSubmitted")
                openFollowerSystem(latestSubmitted, pendingBlock)
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
