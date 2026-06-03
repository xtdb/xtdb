package xtdb.indexer

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
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

    interface Processor<M> : Log.RecordProcessor<M>, AutoCloseable {
        val latestReplicaMsgId: MessageId
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

    // The same split as the compactor: a SubSystem is constructed inert, started by the owner via
    // `runOn(scope)`, and torn down by the owner cancelling that scope. `close()` only frees the
    // State it holds (the proc's allocator, and the leader's replica producer).
    sealed interface SubSystem {
        fun runOn(scope: CoroutineScope)
        fun close()
    }

    interface LeaderSystem : SubSystem {
        val proc: LeaderProcessor
    }

    private inner class FollowerSystem(
        val proc: FollowerProcessor,
        private val afterReplicaMsgId: MessageId,
    ) : SubSystem {
        override fun runOn(scope: CoroutineScope) {
            scope.launch {
                LOG.info {
                    buildString {
                        append("[$dbName] starting follower: ")
                        append("pending block: ${proc.pendingBlock != null}, ")
                        append("src: ${watchers.latestSourceMsgId}, ")
                        append("replica: $afterReplicaMsgId")
                    }
                }
                try {
                    replicaLog.tailAll(afterReplicaMsgId, proc)
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Throwable) {
                    proc.notifyError(e); throw e
                }
            }
        }

        override fun close() = proc.close()
    }

    private fun openFollowerSystem(
        latestReplicaMsgId: MessageId,
        pendingBlock: PendingBlock? = null,
    ): FollowerSystem {
        val proc = procFactory.openFollower(pendingBlock, latestReplicaMsgId)
        return FollowerSystem(proc, latestReplicaMsgId)
    }

    // Each leadership term (follower or leader) runs under its own `termJob` — a SupervisorJob child
    // of the indexer `scope`, so a term-process failure surfaces as `notifyError` rather than tearing
    // down the source-log subscription. A transition cancel-and-joins the old `termJob` (in the
    // rebalance handler) and starts the next role under a fresh one; a Database teardown cancels
    // `scope`, taking the live term with it.
    private fun newTermJob() = SupervisorJob(scope.coroutineContext.job)
    private var termJob = newTermJob()
    private val termScope get() = CoroutineScope(scope.coroutineContext + termJob)

    @Volatile
    private var sys: SubSystem =
        openFollowerSystem(dbState.blockCatalog.boundaryReplicaMsgId ?: -1).also { it.runOn(termScope) }

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
                        val replayTarget = replicaProducer.withTx { it.appendMessage(ReplicaMessage.NoOp) }.await().msgId

                        followerProc.awaitReplicaMsgId(replayTarget)
                        LOG.debug("[$dbName] transition: stopping follower system")
                        // Stop the follower's term scope (its replica tail) and free it before opening
                        // the leader — both roles share `liveIndex`, so the old term must be fully
                        // joined first. The leader then runs on a fresh term scope.
                        termJob.cancelAndJoin()
                        oldSys.close()
                        termJob = newTermJob()

                        val pendingBlock = followerProc.pendingBlock

                        procFactory.openTransition(replicaProducer, followerProc.latestReplicaMsgId).use { transition ->
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
                            sys.runOn(termScope)
                            this.sys = sys

                            val resumeMsgId = watchers.latestSourceMsgId
                            LOG.info("[$dbName] leader startup complete, resuming after $resumeMsgId")
                            Log.TailSpec(resumeMsgId, sys.proc)
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
                val proc = oldSys.proc
                // Stop the leader's term scope before freeing its resources and opening the follower.
                // Safe on the Kafka poll thread: this cancelAndJoin already ran here (it was inside the
                // old `oldSys.close()`); the leader's loops are on Dispatchers.Default with
                // runInterruptible(IO) Kafka polls, so the join can't wedge the poll thread. `close()`
                // only frees the allocator, so the watermark fields stay readable after it.
                termJob.cancelAndJoin()
                oldSys.close()
                termJob = newTermJob()
                this.sys = openFollowerSystem(proc.latestReplicaMsgId, proc.pendingBlock)
                    .also { it.runOn(termScope) }
            }

            is FollowerSystem -> {
                LOG.debug("[$dbName] partitions revoked: $partitions — already follower, no transition needed")
            }
        }
    }

    fun close() {
        sys.close()
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
