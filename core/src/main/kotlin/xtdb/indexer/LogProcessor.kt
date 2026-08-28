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
) {
    try {
        coroutineScope {
            launch(CoroutineName("$dbName-replica-appender")) { appender.run() }

            while (true)
                selectUnbiased {
                    replicaMsgs.onReceive { pending ->
                        try {
                            val record = pending.record
                            val termId = record.message.termId

                            if (termId > term.leaderTerm)
                                throw LeaderSupersededException("[$dbName] superseded: read term $termId > our term ${term.leaderTerm} at ${record.msgId}")

                            // Below our term should not appear past our replay target; discard defensively, still advancing.

                            if (termId != 0L && termId < term.leaderTerm) {
                                LOG.debug { "[$dbName] leader: discarding stale-term record ${record.msgId} (term $termId < ${term.leaderTerm})" }
                                watchers.notifyApplied(replicaMsgId = record.msgId)
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

    val termFence = TermFence(dbName, partitionState.tableCatalogOrNull?.boundaryTermId ?: LeaderTerm.NONE)

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
        try {
            replicaLog.tailAll(watchers.latestReplicaMsgId) { recs ->
                recs.forEach { record ->
                    // Folded outside the retry below, so a record offered again is not folded twice.
                    if (!termFence.admit(record.message.termId)) {
                        // Fenced: a higher-term leader has superseded this message's writer. Discard it,
                        // but still advance the consume position — discard suppresses application, not
                        // consumption — so a transition catch-up can't hang on a fenced no-op.
                        LOG.debug {
                            "[$dbName] discarding fenced record ${record.msgId} " +
                                    "(term ${LeaderTerm.format(record.message.termId)} < " +
                                    "${LeaderTerm.format(termFence.highestSeen)})"
                        }
                        watchers.notifyApplied(record.msgId)
                        return@forEach
                    }

                    // A role ending cancels the handle mid-record, leaving the record unapplied and its
                    // position unadvanced — so it is offered again to whatever replaces that role.
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
                }
            }
        } catch (e: Throwable) {
            if (!e.isShutdownSignal) watchers.notifyError(e)
            throw e
        }
    }

    private fun openFollower(pendingBlock: PendingBlock? = null): Following {
        LOG.info {
            buildString {
                append("[$dbName] starting follower: ")
                append("pending block: ${pendingBlock != null}, ")
                append("src: ${watchers.latestSourceMsgId}, ")
                append("replica: ${watchers.latestReplicaMsgId}")
            }
        }

        val proc = FollowerLogProcessor(
            allocator, partitionStorage.bufferPool, partitionState, dbName, compactor, watchers,
            dbCatalog, pendingBlock,
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
        watchers.awaitReplicaMsg(replayTarget)
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

                val pendingBlock = following.proc.pendingBlock

                // The point of no return. Once the follower is stopped, `state` references a dead term until
                // Leading is published, so any early exit — a revoke cancelling us mid-cutover — has to
                // re-open a live follower, seeded from where this one got to. That recovery is structural
                // rather than flag-guarded: reaching the catch below *is* "the follower was stopped".
                try {
                    LOG.debug("[${dbName}] transition: closing follower")
                    // Guards the release alone: the follower's allocator must close cleanly once teardown has
                    // begun, whatever the cancellation. Bounded — its coroutines only unwind.
                    withContext(NonCancellable) {
                        following.job.cancelAndJoin()
                        following.proc.close()
                    }

                    val driver = RealLeaderDriver(partitionStorage, partitionState, blockUploader)
                    val replicaAppender = ReplicaLogAppender(driver)

                    val proc = LeaderLogProcessor(
                        allocator, base, partitionStorage, crashLogger, partitionState, dbName, driver, watchers,
                        replicaAppender,
                        externalSource,
                        skipTxs, dbCatalog,
                        leaderTerm = termId,
                        flushTimeout = flushTimeout,
                        gcDispatcher = gcDispatcher,
                    )

                    pendingBlock?.let { proc.applyPendingBlock(it) }

                    LOG.debug("[${dbName}] transition: building leader processor")
                    val resumeAfterMsgId = watchers.latestSourceMsgId

                    // The handover from the partition's tail. Unbuffered: the tail waits on each record's
                    // own handle, so a buffer would only let it read ahead of a term about to end.
                    val replicaMsgs = Channel<ReplicaApply>()

                    // The GCs and the external source are the term's to stop, so `shutdown` reaches them.
                    val termJob = scope.launch {
                        launch { proc.gc.runGc() }
                        proc.extSrcProc?.let { extSrcProc -> launch { extSrcProc.run() } }

                        runLeaderTerm(dbName, watchers, proc, replicaMsgs, replicaAppender)
                    }

                    val leading = Leading(proc, roleScope(termJob), replicaMsgs).also { state = it }

                    LOG.info("[${dbName}] leader startup complete, resuming after $resumeAfterMsgId")
                    TailSpec(resumeAfterMsgId, leading.proc.srcLogProc)
                } catch (e: Throwable) {
                    state = openFollower(pendingBlock)
                    throw e
                }
            } catch (e: Throwable) {
                // Cutover already restored a live `state` if it had to; here we only report.
                if (!e.isShutdownSignal) {
                    LOG.error(e, "[${dbName}] transition: failed to prepare leader")
                    watchers.notifyError(e)
                }
                throw e
            }
        }
    }

    private suspend fun LeaderLogProcessor.applyPendingBlock(pendingBlock: PendingBlock) {
        var oldTerm = pendingBlock.boundaryMessage.termId
        LOG.debug("[${dbName}] transition: finishing pending block b${pendingBlock.blockIdx} with ${pendingBlock.bufferedRecords.size} buffered records")
        blockUploader.uploadBlock(pendingBlock.boundaryMsgId, leaderTerm, pendingBlock.boundaryMessage)
        LOG.debug("[${dbName}] transition: replaying ${pendingBlock.bufferedRecords.size} buffered records")

        pendingBlock.bufferedRecords.forEach {
            val msgTermId = it.message.termId
            if (msgTermId >= oldTerm) {
                oldTerm = msgTermId
                applyReplicaMessage(it)
            } else {
                watchers.notifyApplied(replicaMsgId = it.msgId)
            }
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
        // Cancel first: `pendingBlock` stays readable after the cancel/close — it isn't allocator-backed
        // — so we free the old term before reading it to seed the follower.
        leader.job.cancelAndJoin()
        leader.proc.close()
        state = openFollower(leader.proc.pendingBlock)
    }

    override fun close() = state.proc.close()

    /**
     * Run one cycle of every garbage collector owned by the leader (block + trie) and wait for
     * both. No-op unless leading — GC only runs on the leader. Bypasses the collectors'
     * `enabled` flag (which gates the auto-signal from the block-boundary path, not direct calls).
     */
    fun awaitNoGarbageBlocking() = (state as? Leading)?.proc?.gc?.awaitNoGarbageBlocking()
}
