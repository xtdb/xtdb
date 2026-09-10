package xtdb.indexer

import io.micrometer.core.instrument.Gauge
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.first
import org.apache.arrow.memory.BufferAllocator
import xtdb.NodeBase
import xtdb.api.DatabaseName
import xtdb.api.error.Fault
import xtdb.api.error.Interrupted
import xtdb.api.log.*
import xtdb.api.log.Log.TailSpec
import xtdb.api.log.ReplicaMessage.NoOp
import xtdb.api.tx.ExternalSource
import xtdb.compactor.Compactor
import xtdb.database.Database
import xtdb.database.PartitionState
import xtdb.database.PartitionStorage
import xtdb.types.MessageId
import xtdb.util.debug
import xtdb.util.error
import xtdb.util.info
import xtdb.util.logger
import java.time.Duration
import java.util.concurrent.atomic.AtomicLong

private val LOG = LogProcessor::class.logger

// Shutdown, not a fault. MUST NOT reach `Watchers.notifyError`: `Failed` is absorbing, so a clean
// revoke or a node teardown would leave the database unqueryable until the process restarts.
internal val Throwable.isShutdownSignal
    get() = this is CancellationException || this is InterruptedException || this is Interrupted

/**
 * Re-cast a term-teardown cause as a cancellation, preserving the original for the logs.
 *
 * The failure *kind* is load-bearing for anything the transport's poll thread observes: a
 * CancellationException unwinds `processRecords` as cancellation, while anything else reaches the Database
 * scope's `CoroutineExceptionHandler`, which calls `watchers.notifyError`.
 */
internal fun Throwable?.asCancellation(): CancellationException =
    this as? CancellationException
        ?: CancellationException("leader term closed").also { c -> this?.let { c.initCause(it) } }

/**
 * A replica record handed to a leader term, carrying the handle its sender waits on.
 *
 * Application runs on the term's coroutine rather than the tail's because it shares the term's block
 * state, live index and tx resolver with the clauses [LeaderLogProcessor.runTerm] arms — concurrently, a tx could
 * resolve during a block cut, which the term treats as unreachable.
 */
internal class ReplicaApply(val record: Log.Record<ReplicaMessage>) {
    val applied = CompletableDeferred<Unit>()
}

/**
 * Failure arrives as a cancellation whatever its cause, because the term reports its own: a caller that
 * sees this throw learns only that the term is over and its record still needs a home.
 */
internal suspend fun SendChannel<ReplicaApply>.applyAndAwait(record: Log.Record<ReplicaMessage>) =
    ReplicaApply(record).also { send(it) }.applied.await()

class LogProcessor(
    private val allocator: BufferAllocator,
    private val base: NodeBase,
    private val crashLogger: CrashLogger,
    private val partitionStorage: PartitionStorage,
    private val partitionState: PartitionState,
    private val dbName: DatabaseName,
    private val watchers: Watchers,
    private val compactor: Compactor.ForDatabase,
    private val dbCatalog: Database.Catalog?,
    private val externalSource: ExternalSource?,
    private val scope: CoroutineScope,
    private val skipTxs: Set<MessageId> = emptySet(),
    private val flushTimeout: Duration,
    // Injected so a simulation can seed it; each consumer caps its own fan-out off it.
    private val ioDispatcher: CoroutineDispatcher = Dispatchers.IO,
    private val logsDriver: LogsDriver = RealLogsDriver(partitionStorage),
) : Log.SubscriptionListener<SourceMessage>, AutoCloseable {

    /** The partition's log appends, behind one seam, so that a test can fail or stall one. */
    interface LogsDriver {

        /** Needs no atomicity across messages: a superseded leader is fenced by the term its records carry (#5817). */
        suspend fun appendToReplica(msg: ReplicaMessage): Log.MessageMetadata

        /** [expectedBlockIdx] is -1 where no block has been cut yet. */
        suspend fun requestFlushBlock(expectedBlockIdx: Long): MessageId
    }

    class RealLogsDriver(partitionStorage: PartitionStorage) : LogsDriver {
        private val sourceLog = partitionStorage.sourceLog
        private val replicaLog = partitionStorage.replicaLog

        override suspend fun appendToReplica(msg: ReplicaMessage) = replicaLog.appendMessage(msg)

        override suspend fun requestFlushBlock(expectedBlockIdx: Long) =
            sourceLog.appendMessage(SourceMessage.FlushBlock(expectedBlockIdx)).msgId
    }

    private val replicaLog = partitionStorage.replicaLog
    private val hasExternalSource = externalSource != null

    val termFence = TermFence(dbName, partitionState.tableCatalogOrNull?.boundaryTermId ?: 0)

    private sealed interface TailPos {
        /** The last record the tail finished with, applied or discarded. */
        val msgId: MessageId
    }

    private class Reading(override val msgId: MessageId) : TailPos

    /**
     * Terminal rather than merely current: the tail is launched once in `init` and nothing restarts it,
     * so this partition consumes no further replica records for the life of the process.
     */
    private class Stopped(override val msgId: MessageId, val cause: Throwable) : TailPos

    // Written by the tail coroutine alone; a flow because a promotion waits here for its own claim to be
    // read back — see `claimLeadership`.
    private val tailPos: MutableStateFlow<TailPos> =
        MutableStateFlow(Reading(partitionState.tableCatalogOrNull?.boundaryReplicaMsgId ?: -1))

    /**
     * Suspend until the replica tail has finished with [msgId], whether it was applied or discarded.
     *
     * Throws once the tail is [Stopped], wherever it got to — the caller is asking in order to go on and
     * lead, and a term whose replica log nothing is reading would never read its own writes back. So the
     * answer is refused rather than given, even where the tail passed [msgId] before it stopped.
     */
    internal suspend fun awaitReplicaMsg(msgId: MessageId) {
        val pos = tailPos.first { it is Stopped || it.msgId >= msgId }
        if (pos is Stopped) throw pos.cause
    }

    // The role state machine — see allium/log-processor-lifecycle.allium.
    // Written by the transition coroutine and by demoteLeader; they don't race, because a revoke
    // cancel-and-joins the transition before demoteLeader reads it.
    private sealed interface State: AutoCloseable {
        val scope: CoroutineScope

        val job get() = scope.coroutineContext.job

        suspend fun handleReplicaMessage(record: Log.Record<ReplicaMessage>)
    }

    private class Following(val proc: FollowerLogProcessor, override val scope: CoroutineScope) : State {
        override suspend fun handleReplicaMessage(record: Log.Record<ReplicaMessage>) {
            scope.async { proc.handleRecord(record) }.await()
        }

        override fun close() = proc.close()
    }

    private class Leading(
        val proc: LeaderLogProcessor,
        override val scope: CoroutineScope,
        private val replicaMsgs: SendChannel<ReplicaApply>,
    ) : State {
        override suspend fun handleReplicaMessage(record: Log.Record<ReplicaMessage>) {
            scope.async { replicaMsgs.applyAndAwait(record) }.await()
        }

        fun awaitNoGarbageBlocking() = proc.gc.awaitNoGarbageBlocking()

        override fun close() = proc.close()
    }

    // A role's scope is the database's, with a job of its own so that stopping the role leaves the
    // partition's tail running. Derived from `scope` rather than built from the job alone: a bare
    // `CoroutineScope(job)` carries no dispatcher, so every apply would land on Dispatchers.Default —
    // which under a simulation's virtual clock is a deadlock, the scheduler having nothing left to advance.
    private fun roleScope(job: Job) = scope + job

    private suspend fun tailReplica() = coroutineScope {
        var stopCause: Throwable? = null

        try {
            replicaLog.tailAll(tailPos.value.msgId) { recs ->
                recs.forEach { record ->
                    // A role ending cancels the handle mid-record, so the record is offered again to
                    // whatever replaces that role.
                    while (true) {
                        this@coroutineScope.ensureActive()
                        val role = state

                        try {
                            role.handleReplicaMessage(record)
                            break
                        } catch (_: CancellationException) {
                            stateFlow.first { it !== role }
                        } catch (e: Throwable) {
                            LOG.error(
                                e,
                                "[$dbName] failed to process replica record ${record.msgId} (${record.message::class.simpleName})"
                            )
                            throw e
                        }
                    }

                    // Above the apply, this would advance past a record a role cancellation left
                    // unapplied, and that record would be skipped for good. A record the live role
                    // fenced or held advances it all the same, so a transition's catch-up can't hang
                    // waiting for one to be applied.
                    tailPos.value = Reading(record.msgId)
                }
            }
        } catch (e: Throwable) {
            stopCause = e
            if (!e.isShutdownSignal) watchers.notifyError(e)
            throw e
        } finally {
            // Every way out of the tail is terminal, the clean ones included: `tailAll` returns rather
            // than throws when cancellation lands on its `isActive` check instead of inside a poll.
            tailPos.value =
                Stopped(tailPos.value.msgId, stopCause ?: CancellationException("[$dbName] replica tail stopped"))
        }
    }

    private fun openFollower(pendingBlock: PendingBlock? = null): Following {
        LOG.info {
            buildString {
                append("[$dbName] starting follower: ")
                append("pending block: ${pendingBlock != null}, ")
                append("src: ${watchers.latestSourceMsgId}, ")
                append("replica: ${tailPos.value.msgId}")
            }
        }

        val proc = FollowerLogProcessor(
            allocator, partitionStorage.bufferPool, partitionState, dbName, compactor, watchers,
            dbCatalog, pendingBlock, termFence,
            hasExternalSource = hasExternalSource,
            meterRegistry = base.meterRegistry,
        )

        return Following(proc, roleScope(Job(scope.coroutineContext.job)))
    }

    private val stateFlow = MutableStateFlow<State>(openFollower())

    private var state
        get() = stateFlow.value
        set(value) {
            stateFlow.value = value
        }

    // Held here rather than on the term's cutter, because a Micrometer gauge keeps the state object it was
    // registered with: a per-term one would be dropped and the gauge would go on reporting the first
    // term's last upload, which is the one thing it exists to contradict (#5867).
    private val lastBlockUploadEpochSeconds = AtomicLong(0)

    init {
        base.meterRegistry?.let { reg ->
            Gauge.builder("xtdb.log.leader", this) { if (it.state is Leading) 1.0 else 0.0 }
                .description("1 if this node is the log leader, 0 if follower")
                .tag("db", dbName)
                .register(reg)

            // A timer records uploads that happened; this records the absence of one. An external source
            // confirms its upstream position only as far as the last durable block, so a database whose
            // blocks have quietly stopped landing pins the upstream's log while ingestion, queries and
            // healthz all stay green — time-since-last-block is what makes that visible (#5867).
            Gauge.builder("xtdb.block.last_upload_time", lastBlockUploadEpochSeconds) { it.get().toDouble() }
                .description("epoch seconds at which this database's most recent block landed in object storage")
                .baseUnit("seconds")
                .tag("db", dbName)
                .register(reg)
        }

        scope.launch(CoroutineName("$dbName-replica-tail")) { tailReplica() }
    }

    private suspend fun claimLeadership(termId: Long) {
        // Append a NoOp stamped with the new term as the replay target: the follower catches up to it
        // before we cut over, which is what proves our own claim has been read back. A plain append
        // now — the term on read-back is the fence, replacing the transactional producer (#5817).
        val replayTarget = logsDriver.appendToReplica(NoOp(termId = termId)).msgId
        LOG.debug("[${dbName}] transition: awaiting replica catch-up to $replayTarget")
        awaitReplicaMsg(replayTarget)
        LOG.debug("[${dbName}] transition: replica caught up to $replayTarget")

        // Our own claim is now read back, so the follower's max term is the log's — anything above
        // it fences us, and leading would index nothing. Refuse loudly instead (#5817).
        termFence.checkUnfenced(termId)
    }

    override fun transitionToLeader(partition: Int, termId: Long): Deferred<TailSpec<SourceMessage>> {
        // Transport contract: transition only from Following (see SubscriptionListener). A raw cast
        // would surface an out-of-order call as a cryptic ClassCastException; name it instead.
        val following = (state as? Following)
            ?: throw Fault(
                "[$dbName] transitionToLeader while not following (${state::class.simpleName})",
                "xtdb/log-transition-not-following"
            )

        // Launched on the database scope (not the caller's): the transition is a child of the db job
        // tree, so the transport joins/cancels this handle while db teardown cancels-and-joins it
        // before close(). See dev/doc/coroutines.adoc and allium/log-processor-lifecycle.allium.
        return scope.async {
            try {
                claimLeadership(termId)

                var pendingBlock: PendingBlock? = null

                // The point of no return. Once the follower is stopped, `state` references a dead term until
                // Leading is published, so any early exit — a revoke cancelling us mid-cutover — has to
                // re-open a live follower, seeded from where this one got to. That recovery is structural
                // rather than flag-guarded: reaching the catch below *is* "the follower was stopped".
                try {
                    LOG.debug("[${dbName}] transition: closing follower")
                    // A cancellation landing inside the join would close the allocator with the follower's
                    // coroutines still unwinding, and Arrow won't close a parent allocator while a child
                    // buffer is live. Bounded — those coroutines only unwind.
                    withContext(NonCancellable) {
                        @Suppress("ConvertTryFinallyToUseCall")
                        try {
                            following.job.cancelAndJoin()

                            // Read after the join, not before it: the follower goes on applying records
                            // until then, and one of them may be the upload that closes this very block.
                            // Taking the block while it can still be closed underneath us finishes it a
                            // second time.
                            pendingBlock = following.proc.pendingBlock
                        } finally {
                            following.close()
                        }
                    }

                    checkNotSuperseded(termId, pendingBlock)

                    val replicaAppender = ReplicaLogAppender(logsDriver)

                    val blockCutter =
                        BlockCutter(
                            partitionStorage, partitionState, dbName, termId, replicaAppender, logsDriver,
                            compactor, dbCatalog, base.meterRegistry, lastBlockUploadEpochSeconds, scope,
                            ioDispatcher
                        )

                    val proc = LeaderLogProcessor(
                        allocator, base, partitionStorage, crashLogger, partitionState, dbName, logsDriver,
                        blockCutter, watchers,
                        replicaAppender, termFence,
                        externalSource,
                        skipTxs, dbCatalog,
                        leaderTerm = termId,
                        flushTimeout = flushTimeout,
                        ioDispatcher = ioDispatcher,
                    )

                    pendingBlock?.let { pending ->
                        LOG.debug("[${dbName}] transition: producing pending block b${pending.blockIdx} with ${pending.bufferedRecords.size} held records")

                        // Produced here, but closed — and its held records applied — only once the term
                        // below reads this upload back. So the block stays held throughout, and a failure
                        // in between hands it to a re-opened follower that closes it on the same message.
                        // The held records cannot apply any earlier than that: their rows belong to the
                        // block this one is about to open, and the live index has already snapshotted the
                        // one it is still on.
                        blockCutter.upload(pending)
                    }

                    val resumeAfterMsgId = watchers.latestSourceMsgId

                    // The handover from the partition's tail. Unbuffered: the tail waits on each record's
                    // own handle, so a buffer would only let it read ahead of a term about to end.
                    val replicaMsgs = Channel<ReplicaApply>()

                    val termJob = scope.launch { proc.runTerm(replicaMsgs) }

                    state = Leading(proc, roleScope(termJob), replicaMsgs)

                    LOG.info("[${dbName}] leader startup complete, resuming after $resumeAfterMsgId")
                    TailSpec(resumeAfterMsgId, proc.srcLogProc)
                } catch (e: Throwable) {
                    state = openFollower(pendingBlock)
                    throw e
                }
            } catch (e: Throwable) {
                // Cutover already restored a live `state` if it had to; here we only report. A
                // supersession is reported the way a term reports its own — this node is merely not the
                // leader, and poisoning the watchers over it would leave a healthy database unqueryable.
                when {
                    e is LeaderSupersededException -> LOG.info("[$dbName] transition: ${e.message}")

                    !e.isShutdownSignal -> {
                        LOG.error(e, "[${dbName}] transition: failed to prepare leader")
                        watchers.notifyError(e)
                    }
                }
                throw e
            }
        }
    }

    /**
     * Refuse a cutover the log has already moved past, before it builds or appends anything.
     *
     * The claim's own unfenced check goes stale: it runs while the follower is still live, and the
     * follower folds until the join. So the fence is asked again here — and asked, separately, of the
     * records the follower was holding, which have not met it at all.
     */
    private fun checkNotSuperseded(termId: Long, pendingBlock: PendingBlock?) {
        val seen = termFence.highestSeen
        if (seen > termId)
            throw LeaderSupersededException(
                "[$dbName] superseded before cutover: log at ${LeaderTerm.format(seen)} " +
                        "> our term ${LeaderTerm.format(termId)}"
            )

        pendingBlock?.bufferedRecords?.forEach { held ->
            val heldTerm = held.message.termId
            if (heldTerm > termId)
                throw LeaderSupersededException(
                    "[$dbName] superseded before cutover: held term ${LeaderTerm.format(heldTerm)} " +
                            "> our term ${LeaderTerm.format(termId)} at ${held.msgId}"
                )
        }
    }

    override suspend fun demoteLeader(partition: Int) {
        val leader = when (val s = state) {
            is Following -> {
                LOG.debug("[$dbName] demote — already follower, no transition needed")
                return
            }

            is Leading -> s
        }

        LOG.info("[$dbName] demote — tearing down leader, re-opening follower")
        leader.job.cancelAndJoin()

        // After the join, as the promotion reads the follower's: the term applies until then, and one of
        // those applies may be the adopt that closes this block.
        val pendingBlock = leader.proc.pendingBlock

        leader.proc.close()
        state = openFollower(pendingBlock)
    }

    override fun close() = state.close()

    /**
     * Run one cycle of every garbage collector owned by the leader (block + trie) and wait for
     * both. No-op unless leading — GC only runs on the leader. Bypasses the collectors'
     * `enabled` flag (which gates the auto-signal from the block-boundary path, not direct calls).
     */
    fun awaitNoGarbageBlocking() = (state as? Leading)?.awaitNoGarbageBlocking()
}
