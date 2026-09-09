package xtdb.indexer

import io.micrometer.core.instrument.Gauge
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.selects.selectUnbiased
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
 * state, live index and tx resolver with the clauses [runLeaderTerm] arms — concurrently, a tx could
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

/**
 * Run a leader term until it ends, then fail everything staged on it.
 *
 * The term's work and its append pump are structured together, so whichever fails first cancels the other
 * and arrives here as the cause. Cancelling the caller is what ends a term that hasn't failed.
 *
 * Which failures reach the watchers is decided here rather than in the term, because the term is not the
 * thing that knows a resignation from a fault: a supersession means this node is merely no longer the
 * leader, and poisoning the watchers over it would leave a healthy database unqueryable (#5817).
 */
internal suspend fun runLeaderTerm(
    dbName: DatabaseName,
    watchers: Watchers,
    term: LeaderLogProcessor,
    replicaMsgs: ReceiveChannel<ReplicaApply>,
    appender: ReplicaLogAppender,
    termFence: TermFence,
) {
    try {
        coroutineScope {
            launch(CoroutineName("$dbName-replica-appender")) { appender.run() }

            while (true) {
                // Ahead of the arming below, not after it: a filled block must admit nothing else, and
                // the GC clause would otherwise slip a TriesDeleted in ahead of the boundary.
                if (term.blockFilled) term.cutFilledBlock()

                selectUnbiased {
                    replicaMsgs.onReceive { pending ->
                        try {
                            val record = pending.record
                            val termId = record.message.termId

                            if (termId > term.leaderTerm)
                                throw LeaderSupersededException("[$dbName] superseded: read term $termId > our term ${term.leaderTerm} at ${record.msgId}")

                            // Our own claim folded before this term opened, so the fence's high-water is
                            // our term and anything it refuses is below ours — which shouldn't appear
                            // past our replay target anyway. The fold has to happen here whatever the
                            // verdict, or the high-water would stand still for the length of the term.
                            if (!termFence.admit(termId)) {
                                LOG.debug { "[$dbName] leader: discarding fenced record ${record.msgId} (term $termId < ${term.leaderTerm})" }
                            } else {
                                term.applyReplicaMessage(record)
                            }

                            pending.applied.complete(Unit)
                        } catch (t: Throwable) {
                            pending.applied.completeExceptionally(t.asCancellation())
                            throw t
                        }
                    }

                    if (term.acceptingResolution) {
                        term.srcLogProc.run { armSelect() }
                        term.extSrcProc?.run { armSelect() }
                        term.gc.run { armSelect() }
                    }
                }
            }
        }
    } catch (t: Throwable) {
        when {
            t is LeaderSupersededException -> {
                LOG.info("[$dbName] ${t.message}")
            }

            !t.isShutdownSignal -> {
                LOG.error(t) { "[$dbName] leader term failed" }
                watchers.notifyError(t)
            }
        }

        term.shutdown(t)
    }
}

class LogProcessor(
    private val allocator: BufferAllocator,
    private val base: NodeBase,
    private val crashLogger: CrashLogger,
    private val partitionStorage: PartitionStorage,
    private val partitionState: PartitionState,
    private val dbName: DatabaseName,
    private val watchers: Watchers,
    private val blockUploader: BlockUploader,
    private val compactor: Compactor.ForDatabase,
    private val dbCatalog: Database.Catalog?,
    private val externalSource: ExternalSource?,
    private val scope: CoroutineScope,
    private val skipTxs: Set<MessageId> = emptySet(),
    private val flushTimeout: Duration,
    private val gcDispatcher: CoroutineDispatcher = Dispatchers.IO,
) : Log.SubscriptionListener<SourceMessage>, AutoCloseable {

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
    private sealed interface State {
        val proc: AutoCloseable

        val scope: CoroutineScope

        val job get() = scope.coroutineContext.job

        suspend fun handleReplicaMessage(record: Log.Record<ReplicaMessage>)
    }

    private class Following(override val proc: FollowerLogProcessor, override val scope: CoroutineScope) : State {
        override suspend fun handleReplicaMessage(record: Log.Record<ReplicaMessage>) {
            scope.async { proc.handleRecord(record) }.await()
        }
    }

    private class Leading(
        override val proc: LeaderLogProcessor,
        override val scope: CoroutineScope,
        private val replicaMsgs: SendChannel<ReplicaApply>,
    ) : State {
        override suspend fun handleReplicaMessage(record: Log.Record<ReplicaMessage>) {
            scope.async { replicaMsgs.applyAndAwait(record) }.await()
        }
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

    init {
        base.meterRegistry?.let { reg ->
            Gauge.builder("xtdb.log.leader", this) { if (it.state is Leading) 1.0 else 0.0 }
                .description("1 if this node is the log leader, 0 if follower")
                .tag("db", dbName)
                .register(reg)
        }

        scope.launch(CoroutineName("$dbName-replica-tail")) { tailReplica() }
    }

    private suspend fun claimLeadership(termId: Long) {
        // Append a NoOp stamped with the new term as the replay target: the follower catches up to it
        // before we cut over, which is what proves our own claim has been read back. A plain append
        // now — the term on read-back is the fence, replacing the transactional producer (#5817).
        val replayTarget = replicaLog.appendMessage(NoOp(termId = termId)).msgId
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
                        following.job.cancelAndJoin()
                        following.proc.close()

                        // Read after the join, not before it: the follower goes on applying records until
                        // then, and one of them may be the upload that closes this very block. Taking the
                        // block while it can still be closed underneath us finishes it a second time.
                        pendingBlock = following.proc.pendingBlock
                    }

                    checkNotSuperseded(termId, pendingBlock)

                    val driver = RealLeaderDriver(partitionStorage, partitionState)
                    val replicaAppender = ReplicaLogAppender(driver)
                    val blockCutter =
                        BlockCutter(partitionState, dbName, termId, replicaAppender, blockUploader)

                    val proc = LeaderLogProcessor(
                        allocator, base, partitionStorage, crashLogger, partitionState, dbName, driver,
                        blockCutter, watchers,
                        replicaAppender,
                        externalSource,
                        skipTxs, dbCatalog,
                        leaderTerm = termId,
                        flushTimeout = flushTimeout,
                        gcDispatcher = gcDispatcher,
                    )

                    pendingBlock?.let { pending ->
                        LOG.debug("[${dbName}] transition: finishing pending block b${pending.blockIdx} with ${pending.bufferedRecords.size} held records")

                        // Through the cutter, not the uploader: closing a block is what resets the row
                        // gauge, and this block was cut before the gauge was seeded from a live index that
                        // still held it.
                        blockCutter.upload(pending.boundaryMsgId, pending.boundaryMessage)

                        // The block is closed on this node from here — catalog refreshed, live index
                        // rolled — so a failure below must not hand it to a re-opened follower, which
                        // would match the upload we have just appended and close it a second time.
                        pendingBlock = null

                        proc.replayHeldRecords(pending)
                    }

                    LOG.debug("[${dbName}] transition: building leader processor")
                    val resumeAfterMsgId = watchers.latestSourceMsgId

                    // The handover from the partition's tail. Unbuffered: the tail waits on each record's
                    // own handle, so a buffer would only let it read ahead of a term about to end.
                    val replicaMsgs = Channel<ReplicaApply>()

                    // The GCs and the external source are the term's to stop, so `shutdown` reaches them.
                    val termJob = scope.launch {
                        launch { proc.gc.runGc() }
                        proc.extSrcProc?.let { extSrcProc -> launch { extSrcProc.run() } }

                        runLeaderTerm(dbName, watchers, proc, replicaMsgs, replicaAppender, termFence)
                    }

                    val leading = Leading(proc, roleScope(termJob), replicaMsgs).also { state = it }

                    LOG.info("[${dbName}] leader startup complete, resuming after $resumeAfterMsgId")
                    TailSpec(resumeAfterMsgId, leading.proc.srcLogProc)
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

    /** Fold and apply what the follower was holding behind its block, in the order the log had it. */
    private suspend fun LeaderLogProcessor.replayHeldRecords(pendingBlock: PendingBlock) {
        LOG.debug("[${dbName}] transition: replaying ${pendingBlock.bufferedRecords.size} held records")

        pendingBlock.bufferedRecords.forEach { held ->
            if (termFence.admit(held.message.termId)) applyReplicaMessage(held)
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
        leader.proc.close()
        state = openFollower()
    }

    override fun close() = state.proc.close()

    /**
     * Run one cycle of every garbage collector owned by the leader (block + trie) and wait for
     * both. No-op unless leading — GC only runs on the leader. Bypasses the collectors'
     * `enabled` flag (which gates the auto-signal from the block-boundary path, not direct calls).
     */
    fun awaitNoGarbageBlocking() = (state as? Leading)?.proc?.gc?.awaitNoGarbageBlocking()
}
