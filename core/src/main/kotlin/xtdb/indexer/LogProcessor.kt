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
        suspend fun awaitReplicaMsgId(target: MessageId)
    }

    private val dbName = dbState.name
    private val replicaLog = dbStorage.replicaLog

    interface ProcessorFactory {
        fun openLeaderSystem(
            replicaProducer: Log.AtomicProducer<ReplicaMessage>,
            afterSourceMsgId: MessageId,
            afterReplicaMsgId: MessageId,
        ): LeaderSystem

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

    sealed interface SubSystem : AutoCloseable

    interface LeaderSystem : SubSystem {
        val proc: LeaderProcessor
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
                    append("[$dbName] starting follower: ")
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
                LOG.info("[$dbName] partitions assigned: $partitions — already leader, no transition needed")
                null
            }

            is FollowerSystem -> {
                LOG.info("[$dbName] partitions assigned: $partitions — transitioning to leader")

                replicaLog.openAtomicProducer("${dbState.name}-leader").closeOnCatch { replicaProducer ->
                    val followerProc = oldSys.proc

                    // Send a NoOp to get a known msgId we can await —
                    // we can't use latestSubmittedMsgId because Kafka's endOffsets
                    // includes transaction marker offsets that consumers never deliver.
                    val replayTarget = replicaProducer.withTx { it.appendMessage(ReplicaMessage.NoOp) }.await().msgId

                    followerProc.awaitReplicaMsgId(replayTarget)
                    LOG.debug("[$dbName] transition: closing follower system")
                    oldSys.close()

                    val pendingBlock = followerProc.pendingBlock

                    procFactory.openTransition(
                        replicaProducer,
                        followerProc.latestSourceMsgId,
                        followerProc.latestReplicaMsgId
                    )
                        .use { transition ->
                            if (pendingBlock != null) {
                                LOG.debug("[$dbName] transition: finishing pending block b${pendingBlock.blockIdx} with ${pendingBlock.bufferedRecords.size} buffered records")
                                blockUploader.uploadBlock(
                                    replicaProducer, pendingBlock.boundaryMsgId, pendingBlock.boundaryMessage,
                                )
                                LOG.debug("[$dbName] transition: replaying ${pendingBlock.bufferedRecords.size} buffered records through transition processor")
                                transition.processRecords(pendingBlock.bufferedRecords)
                            }

                            val latestSourceMsgId = transition.latestSourceMsgId
                            LOG.debug("[$dbName] transition: opening leader processor")

                            val sys = procFactory.openLeaderSystem(replicaProducer, latestSourceMsgId, replayTarget)
                            this.sys = sys

                            LOG.info("[$dbName] leader startup complete, resuming after $latestSourceMsgId")
                            Log.TailSpec(latestSourceMsgId, sys.proc)
                        }
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
                oldSys.close()
                val proc = oldSys.proc
                this.sys = openFollowerSystem(proc.latestSourceMsgId, proc.latestReplicaMsgId, proc.pendingBlock)
            }

            is FollowerSystem -> {
                LOG.debug("[$dbName] partitions revoked: $partitions — already follower, no transition needed")
            }
        }
    }

    override fun close() {
        sys.close()
    }
}
