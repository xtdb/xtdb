package xtdb.indexer

import io.micrometer.core.instrument.Gauge
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.job
import org.apache.arrow.memory.BufferAllocator
import xtdb.NodeBase
import xtdb.api.log.*
import xtdb.api.log.Log.AtomicProducer.Companion.withTx
import xtdb.compactor.Compactor
import xtdb.database.Database
import xtdb.database.DatabaseState
import xtdb.database.DatabaseStorage
import xtdb.database.ExternalSource
import xtdb.error.Interrupted
import xtdb.util.closeOnCatch
import xtdb.util.debug
import xtdb.util.error
import xtdb.util.info
import xtdb.util.logger
import java.time.Duration

private val LOG = LogProcessor::class.logger

class LogProcessor(
    private val allocator: BufferAllocator,
    private val base: NodeBase,
    private val crashLogger: CrashLogger,
    private val dbStorage: DatabaseStorage,
    private val dbState: DatabaseState,
    private val watchers: Watchers,
    private val blockUploader: BlockUploader,
    private val compactor: Compactor.ForDatabase,
    private val dbCatalog: Database.Catalog?,
    private val externalSourceFactory: ExternalSource.Factory?,
    private val scope: CoroutineScope,
    private val skipTxs: Set<MessageId> = emptySet(),
    private val flushTimeout: Duration = Duration.ofMinutes(5),
    private val gcDispatcher: CoroutineDispatcher = Dispatchers.IO,
) : Log.SubscriptionListener<SourceMessage>, AutoCloseable {

    interface Processor<M> : Log.RecordProcessor<M>, AutoCloseable {
        val latestReplicaMsgId: MessageId
    }

    private val dbName = dbState.name
    private val replicaLog = dbStorage.replicaLog
    private val hasExternalSource = externalSourceFactory != null

    // The running term: processor + the SupervisorJob its coroutines run under, held as one
    // atomically-swapped value so a job-less or unfreeable term is unrepresentable. `close` MUST
    // follow a returned `cancelAndJoin`. See dev/doc/coroutines.adoc.
    private sealed class SubSystem(private val job: Job, private val closeable: AutoCloseable) : AutoCloseable {
        suspend fun cancelAndJoin() = job.cancelAndJoin()
        override fun close() = closeable.close()
    }

    private class LeaderSystem(val proc: LeaderLogProcessor, job: Job) : SubSystem(job, proc)
    private class FollowerSystem(val proc: FollowerLogProcessor, job: Job) : SubSystem(job, proc)

    // Open a fresh term: a SupervisorJob child of the database scope — so one role's failure surfaces
    // via `notifyError` rather than cancelling the source-log subscription (its sibling) — and the
    // subsystem built (by `build`) on a scope over that job.
    private fun <S : SubSystem> openTerm(build: (CoroutineScope, Job) -> S): S {
        val job = SupervisorJob(scope.coroutineContext.job)
        return try {
            build(CoroutineScope(scope.coroutineContext + job), job)
        } catch (t: Throwable) {
            job.cancel()
            throw t
        }
    }

    private fun openLeader(
        termScope: CoroutineScope,
        replicaProducer: Log.AtomicProducer<ReplicaMessage>,
        afterReplicaMsgId: MessageId,
    ): LeaderLogProcessor =
        // The leader term owns (and frees) its replica producer and ext source.
        LeaderLogProcessor(
            allocator, base, dbStorage, crashLogger,
            dbState, blockUploader, watchers,
            extSource = externalSourceFactory?.open(dbName, base.remotes, base.meterRegistry),
            replicaProducer = replicaProducer,
            skipTxs = skipTxs,
            dbCatalog = dbCatalog,
            partition = 0,
            afterReplicaMsgId = afterReplicaMsgId,
            flushTimeout = flushTimeout,
            scope = termScope,
            gcDispatcher = gcDispatcher,
        )

    private fun openTransition(
        replicaProducer: Log.AtomicProducer<ReplicaMessage>,
        afterReplicaMsgId: MessageId,
    ): TransitionLogProcessor =
        TransitionLogProcessor(
            allocator, dbStorage.bufferPool, dbState, dbState.liveIndex,
            blockUploader, replicaProducer,
            watchers, dbCatalog,
            afterReplicaMsgId,
            hasExternalSource = hasExternalSource,
        )

    private fun openFollower(
        termScope: CoroutineScope,
        pendingBlock: PendingBlock?,
        afterReplicaMsgId: MessageId,
    ): FollowerLogProcessor =
        FollowerLogProcessor(
            termScope, allocator, dbStorage.replicaLog, dbStorage.bufferPool, dbState, compactor,
            watchers, dbCatalog, pendingBlock, afterReplicaMsgId,
            hasExternalSource = hasExternalSource,
            meterRegistry = base.meterRegistry,
        )

    private fun openFollowerSystem(
        latestReplicaMsgId: MessageId,
        pendingBlock: PendingBlock? = null,
    ): FollowerSystem {
        LOG.info {
            buildString {
                append("[$dbName] starting follower: ")
                append("pending block: ${pendingBlock != null}, ")
                append("src: ${watchers.latestSourceMsgId}, ")
                append("replica: $latestReplicaMsgId")
            }
        }

        return openTerm { termScope, job ->
            FollowerSystem(openFollower(termScope, pendingBlock, latestReplicaMsgId), job)
        }
    }

    @Volatile
    private var sys: SubSystem =
        openFollowerSystem(dbState.blockCatalog.boundaryReplicaMsgId ?: -1)

    init {
        base.meterRegistry?.let { reg ->
            Gauge.builder("xtdb.log.leader", this) { if (it.sys is LeaderSystem) 1.0 else 0.0 }
                .description("1 if this node is the log leader, 0 if follower")
                .tag("db", dbState.name)
                .register(reg)
        }
    }

    override suspend fun onPartitionsAssigned(partitions: Collection<Int>): Log.TailSpec<SourceMessage>? {
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
                        oldSys.close()

                        val pendingBlock = followerProc.pendingBlock

                        openTransition(replicaProducer, followerProc.latestReplicaMsgId).use { transition ->
                            if (pendingBlock != null) {
                                LOG.debug("[$dbName] transition: finishing pending block b${pendingBlock.blockIdx} with ${pendingBlock.bufferedRecords.size} buffered records")
                                blockUploader.uploadBlock(
                                    replicaProducer, pendingBlock.boundaryMsgId, pendingBlock.boundaryMessage,
                                )
                                LOG.debug("[$dbName] transition: replaying ${pendingBlock.bufferedRecords.size} buffered records through transition processor")
                                transition.processRecords(pendingBlock.bufferedRecords)
                            }

                            LOG.debug("[$dbName] transition: opening leader processor")

                            val leaderSys = openTerm { termScope, job ->
                                LeaderSystem(openLeader(termScope, replicaProducer, replayTarget), job)
                            }
                            this.sys = leaderSys

                            val resumeMsgId = watchers.latestSourceMsgId
                            LOG.info("[$dbName] leader startup complete, resuming after $resumeMsgId")
                            Log.TailSpec(resumeMsgId, leaderSys.proc)
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
        when (val oldSys = sys) {
            is LeaderSystem -> {
                LOG.info("[$dbName] partitions revoked: $partitions — was leader, transitioning to follower")
                // Cancel first: Kafka guarantees no concurrent processing during rebalance. The
                // leader's watermark fields stay readable after the cancel/close — they're not
                // allocator-backed — so we free the old term before reading them to seed the follower.
                oldSys.cancelAndJoin()
                oldSys.close()
                val proc = oldSys.proc
                this.sys = openFollowerSystem(proc.latestReplicaMsgId, proc.pendingBlock)
            }

            is FollowerSystem -> {
                LOG.debug("[$dbName] partitions revoked: $partitions — already follower, no transition needed")
            }
        }
    }

    override fun close() = sys.close()

    /**
     * Run one cycle of every garbage collector owned by the leader (block + trie) and wait for
     * both. No-op on follower systems — GC only runs on the leader. Bypasses the collectors'
     * `enabled` flag (which gates the auto-signal from the block-boundary path, not direct calls).
     */
    fun gcAll() {
        val proc = (sys as? LeaderSystem)?.proc ?: return
        proc.blockGc.awaitNoGarbageBlocking()
        proc.trieGc.awaitNoGarbageBlocking()
    }
}
