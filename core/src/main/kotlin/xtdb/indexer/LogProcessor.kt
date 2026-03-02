package xtdb.indexer

import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.runInterruptible
import kotlinx.coroutines.withTimeoutOrNull
import xtdb.api.log.*
import xtdb.api.log.Log.AtomicProducer.Companion.withTx
import xtdb.api.log.Log.Companion.tailAll
import xtdb.database.DatabaseState
import xtdb.database.DatabaseStorage
import xtdb.util.MsgIdUtil.offsetToMsgId
import xtdb.util.closeOnCatch
import xtdb.util.debug
import xtdb.util.info
import xtdb.util.logger
import kotlin.math.max
import kotlin.time.Duration.Companion.seconds

private val LOG = LogProcessor::class.logger

class LogProcessor(
    private val procFactory: ProcessorFactory,
    private val dbStorage: DatabaseStorage,
    private val dbState: DatabaseState,
    private val watchers: Watchers,
    private val blockFinisher: BlockFinisher,
) : Log.SubscriptionListener, AutoCloseable {

    interface LeaderProcessor : Log.RecordProcessor<SourceMessage>, AutoCloseable

    interface TransitionProcessor : Log.RecordProcessor<ReplicaMessage>, AutoCloseable

    interface FollowerProcessor : Log.RecordProcessor<ReplicaMessage>, AutoCloseable {
        val pendingBlock: FollowerLogProcessor.PendingBlock?
    }

    private val replicaLog = dbStorage.replicaLog

    interface ProcessorFactory {
        fun openLeader(replicaProducer: Log.AtomicProducer<ReplicaMessage>): LeaderProcessor

        fun openTransition(
            replicaProducer: Log.AtomicProducer<ReplicaMessage>, replicaWatchers: Watchers
        ): TransitionProcessor

        fun openFollower(replicaWatchers: Watchers): FollowerProcessor
    }

    sealed class SubSystem : AutoCloseable {
        abstract val proc: AutoCloseable
        abstract val sub: Log.Subscription

        override fun close() {
            sub.close()
            proc.close()
        }
    }

    private class LeaderSystem(override val proc: LeaderProcessor, override val sub: Log.Subscription) : SubSystem()
    private class FollowerSystem(override val proc: FollowerProcessor, override val sub: Log.Subscription) : SubSystem()

    private val replicaWatchers =
        Watchers(max(dbState.blockCatalog.boundaryReplicaMsgId ?: -1, offsetToMsgId(replicaLog.epoch, -1)))

    private fun openFollowerSystem(latestReplicaMsgId: MessageId): FollowerSystem =
        procFactory.openFollower(replicaWatchers).closeOnCatch { proc ->
            FollowerSystem(proc, replicaLog.tailAll(latestReplicaMsgId, proc))
        }

    @Volatile
    private var sys: SubSystem = openFollowerSystem(replicaWatchers.currentMsgId)

    override fun onPartitionsAssigned(partitions: Collection<Int>) {
        this.sys = when (val oldSys = sys) {
            is LeaderSystem -> {
                LOG.info("partitions assigned: $partitions — already leader, no transition needed")
                oldSys
            }

            is FollowerSystem -> {
                LOG.info("partitions assigned: $partitions — transitioning to leader")

                // Fence: open atomic producer on replica log.
                replicaLog.openAtomicProducer("${dbState.name}-leader").closeOnCatch { replicaProducer ->
                    // Send a NoOp to get a known msgId we can await —
                    // we can't use latestSubmittedMsgId because Kafka's endOffsets
                    // includes transaction marker offsets that consumers never deliver.
                    val replayTarget = replicaProducer.withTx { it.appendMessage(ReplicaMessage.NoOp) }.get().msgId
                    runBlocking { replicaWatchers.await0(replayTarget) }

                    val followerProc = oldSys.proc
                    val pendingBlock = followerProc.pendingBlock
                    oldSys.close()

                    procFactory.openTransition(replicaProducer, replicaWatchers).use { transition ->
                        if (pendingBlock != null) {
                            blockFinisher.finishBlock(
                                replicaProducer,
                                pendingBlock.boundaryMsgId,
                                pendingBlock.boundaryMessage
                            )
                            transition.processRecords(pendingBlock.bufferedRecords)
                        }

                        procFactory.openLeader(replicaProducer).closeOnCatch { proc ->
                            val latestProcessedMsgId = runBlocking { watchers.sync() }
                            val sub = dbStorage.sourceLog.tailAll(latestProcessedMsgId, proc)
                            val newSys = LeaderSystem(proc, sub)

                            LOG.info("leader startup complete, resuming after $latestProcessedMsgId")

                            newSys
                        }
                    }
                }
            }
        }
    }

    override fun onPartitionsRevoked(partitions: Collection<Int>) {
        LOG.debug("partitions revoked: $partitions — transitioning to follower")
        this.sys = when (val oldSys = sys) {
            is LeaderSystem -> {
                LOG.info("partitions revoked: $partitions — was leader, transitioning to follower")
                oldSys.close()
                openFollowerSystem(replicaLog.latestSubmittedMsgId)
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
