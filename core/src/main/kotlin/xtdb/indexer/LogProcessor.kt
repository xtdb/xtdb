package xtdb.indexer

import io.micrometer.core.instrument.Gauge
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.SelectBuilder
import kotlinx.coroutines.selects.selectUnbiased
import kotlinx.coroutines.withContext
import org.apache.arrow.memory.BufferAllocator
import xtdb.NodeBase
import xtdb.api.TransactionResult
import xtdb.api.log.*
import xtdb.compactor.Compactor
import xtdb.api.DatabaseName
import xtdb.database.Database
import xtdb.database.PartitionState
import xtdb.database.PartitionStorage
import xtdb.api.tx.ExternalSource
import xtdb.api.error.Fault
import xtdb.api.error.Interrupted
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
 * Run a resolution task, routing failures onto its completion handle (and any external-source result) so
 * that no caller hangs. Interrupts are shutdown signals, not ingestion faults, so they don't poison the
 * watchers. A successful source batch completes its own handle, possibly deferred if a block cut paused it.
 */
internal inline fun Watchers.runTaskGuarded(
    onComplete: CompletableDeferred<Unit>,
    extResult: CompletableDeferred<TransactionResult>? = null,
    block: () -> Unit,
) {
    try {
        block()
    } catch (e: CancellationException) {
        if (!onComplete.isCompleted) onComplete.cancel(e)
        throw e
    } catch (e: Throwable) {
        if (!e.isShutdownSignal) {
            notifyError(e)
            extResult?.let { if (!it.isCompleted) it.completeExceptionally(e) }
        }
        if (!onComplete.isCompleted) onComplete.completeExceptionally(e)
        throw e
    }
}

/**
 * Arm the work a leader term will take right now.
 *
 * Consume-back stays armed throughout: it is the only thing that acks a write, so a term that stopped
 * applying while a block cut was in flight would never see the boundary land. Resolution is the part that
 * pauses, which [LeaderLogProcessor.acceptingResolution] answers for.
 */
private fun SelectBuilder<Unit>.selectLeaderWork(watchers: Watchers, term: LeaderLogProcessor) {
    // supersession fails the term, not the database — rationale on the outer ladder
    term.onReplicaMsg { record ->
        try {
            term.applyRecord(record)
        } catch (e: Throwable) {
            if (!e.isShutdownSignal && e !is LeaderSupersededException) watchers.notifyError(e)
            throw e
        }
    }

    if (term.acceptingResolution) {
        with(term.srcLogProc) { selectWork() }

        term.extSrcProc?.let { extSrcProc ->
            extSrcProc.onTask { task ->
                watchers.runTaskGuarded(task.onComplete, extResult = task.msg.pending) {
                    extSrcProc.handleTask(task)
                }
            }
        }

        term.gc.onTask { task ->
            watchers.runTaskGuarded(task.onComplete) { term.gc.handleTask(task) }
        }
    }
}

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
    dbName: DatabaseName, watchers: Watchers, term: LeaderLogProcessor, appender: ReplicaLogAppender,
) {
    var cause: Throwable? = null
    try {
        coroutineScope {
            launch(CoroutineName("$dbName-append-pump")) { appender.run() }
            while (true) selectUnbiased { selectLeaderWork(watchers, term) }
        }
    } catch (_: CancellationException) {
        // term cancellation
    } catch (e: LeaderSupersededException) {
        // superseded by a newer leader — expected, not a query-facing fault; the transport re-follows on
        // the next rebalance.
        LOG.info("[$dbName] ${e.message}")
        cause = e
    } catch (t: Throwable) {
        // A genuine term fault (e.g. an append fault) surfaces to queries as a failed term. Idempotent —
        // the apply arm may already have notified for its own faults.
        LOG.error(t) { "[$dbName] leader term failed" }
        cause = t
        watchers.notifyError(t)
    } finally {
        term.shutdown(cause ?: CancellationException("leader term closed"))
    }
}

/**
 * Run a follower's work until it ends.
 *
 * A failure is already logged and on the watchers by the time it arrives — `FollowerLogProcessor` applies
 * records inside `processRecords`, which reports before it throws — so there is nothing left to decide.
 */
internal suspend fun runFollower(follower: FollowerLogProcessor) {
    try {
        while (true) selectUnbiased { with(follower) { selectWork() } }
    } catch (e: CancellationException) {
        throw e
    } catch (_: Throwable) {
        // already reported
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
    private val externalSourceFactory: ExternalSource.Factory?,
    private val scope: CoroutineScope,
    private val skipTxs: Set<MessageId> = emptySet(),
    private val flushTimeout: Duration,
    private val gcDispatcher: CoroutineDispatcher = Dispatchers.IO,
) : Log.SubscriptionListener<SourceMessage>, AutoCloseable {

    private val replicaLog = partitionStorage.replicaLog
    private val hasExternalSource = externalSourceFactory != null

    // The committed-role state machine — see allium/log-processor-lifecycle.allium.
    // `state` is written from two places: the transport's serialization point (commitLeader /
    // demoteLeader) and the off-thread transition coroutine (cutoverToLeader → Prepared, or its catch
    // → Following). They don't race: a revoke cancel-and-joins the transition before demoteLeader reads
    // `state`, and the transport commits a leader only for the transition instance it still holds — so the
    // *committed* role change still happens only at the serialization point.
    //
    // The leader and the follower process different message types, so the only thing a state can offer
    // without narrowing to its variant is the teardown: a role is stopped by cancelling its job and freed
    // by closing its processor, in that order (dev/doc/coroutines.adoc).
    private sealed interface State {
        val proc: AutoCloseable
        val job: Job
    }

    private class Following(override val proc: FollowerLogProcessor, override val job: Job) : State

    // A built leader, committed or not.
    private sealed interface Leader : State {
        override val proc: LeaderLogProcessor
    }

    private class Prepared(
        override val proc: LeaderLogProcessor, override val job: Job, val resumeAfterMsgId: MessageId,
    ) : Leader

    private class Leading(override val proc: LeaderLogProcessor, override val job: Job) : Leader

    private fun openLeader(termId: Long, resumeAfterMsgId: MessageId): Prepared {
        // The leader term owns (and frees) its driver and its ext source.
        val driver = RealLeaderDriver(partitionStorage, partitionState, blockUploader)
        val replicaAppender = ReplicaLogAppender(driver)

        val proc = LeaderLogProcessor(
            allocator, base, partitionStorage, crashLogger, partitionState, dbName, driver, watchers,
            replicaAppender,
            externalSourceFactory?.open(dbName, base.remotes, base.meterRegistry),
            skipTxs, dbCatalog,
            leaderTerm = termId,
            flushTimeout = flushTimeout,
            gcDispatcher = gcDispatcher,
        )

        // Reading is the partition's for a leader too — the term's own writes are confirmed by arriving
        // back here. One job over all of them, so a read that fails cancels the term: consume-back is the
        // only thing that acks a write, so a term outliving its reader hangs whatever is staged on it.
        val termJob = scope.launch {
            launch { tailReplica { records -> records.forEach { proc.queueReplicaMessage(it) } } }
            launch { proc.gc.runGc() }
            proc.extSrcProc?.let { extSrcProc -> launch { extSrcProc.run() } }
            runLeaderTerm(dbName, watchers, proc, replicaAppender)
        }

        return Prepared(proc, termJob, resumeAfterMsgId)
    }


    private fun openTransition(termId: Long): TransitionLogProcessor =
        TransitionLogProcessor(
            allocator, partitionStorage.bufferPool, partitionState, dbName, partitionState.liveIndex,
            blockUploader, watchers, dbCatalog,
            hasExternalSource = hasExternalSource,
            termId = termId,
        )

    // Read the partition's replica log into [apply] until cancelled. The reader belongs to the partition
    // rather than to a role: a follower and a leader consume the same log from the same position, and what
    // differs between them is only what a record does when it arrives.
    private suspend fun tailReplica(apply: suspend (List<Log.Record<ReplicaMessage>>) -> Unit) {
        try {
            replicaLog.tailAll(watchers.latestReplicaMsgId, apply)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Throwable) {
            watchers.notifyError(e); throw e
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

        return Following(proc, scope.launch {
            launch { tailReplica(proc::queueRecords) }
            runFollower(proc)
        })
    }

    @Volatile
    private var state: State = openFollower()

    init {
        base.meterRegistry?.let { reg ->
            Gauge.builder("xtdb.log.leader", this) { if (it.state is Leading) 1.0 else 0.0 }
                .description("1 if this node is the log leader, 0 if follower")
                .tag("db", dbName)
                .register(reg)
        }
    }

    override fun launchTransition(partition: Int, termId: Long): Deferred<Unit> {
        // Transport contract: transition only from Following (see SubscriptionListener). A raw cast
        // would surface an out-of-order call as a cryptic ClassCastException; name it instead.
        val following = (state as? Following)
            ?: throw Fault(
                "[$dbName] launchTransition while not following (${state::class.simpleName})",
                "xtdb/log-prepare-not-following"
            )

        // Launched on the database scope (not the caller's): the transition is a child of the db job
        // tree, so the transport joins/cancels this handle while db teardown cancels-and-joins it
        // before close(). See dev/doc/coroutines.adoc and allium/log-processor-lifecycle.allium.
        return scope.async { runTransition(following, termId) }
    }

    private suspend fun runTransition(following: Following, termId: Long) {
        try {
            // Append a NoOp stamped with the new term as the replay target: the follower catches up to it
            // before we cut over, which is what proves our own claim has been read back. A plain append
            // now — the term on read-back is the fence, replacing the transactional producer (#5817).
            val replayTarget = replicaLog.appendMessage(ReplicaMessage.NoOp(termId = termId)).msgId
            LOG.debug("[$dbName] transition: awaiting replica catch-up to $replayTarget")
            watchers.awaitReplicaMsg(replayTarget)
            LOG.debug("[$dbName] transition: replica caught up to $replayTarget")
            // Our own claim is now read back, so the follower's max term is the log's — anything above
            // it fences us, and leading would index nothing. Refuse loudly instead (#5817).
            following.proc.checkTermUnfenced(termId)
            cutoverToLeader(following, termId)
        } catch (e: Throwable) {
            // Cutover already restored a live `state` if it had to; here we only report.
            if (!e.isShutdownSignal) {
                LOG.error(e, "[$dbName] transition: failed to prepare leader")
                watchers.notifyError(e)
            }
            throw e
        }
    }

    // The point of no return: stop the follower, finish its pending block, build the leader. Once the
    // follower is stopped, `state` references a dead term until we publish Prepared — so any early exit
    // (a revoke cancelling us mid-cutover) re-opens a live follower from the catch, seeded from where the
    // follower got to, and `state` never keeps a corpse. Recovery is structural, not flag-guarded:
    // reaching the catch *is* "the follower was stopped", and `state` is exclusively ours until Prepared
    // (the transport writes it only at its serialization point, after joining us), so no staleness guard
    // is needed. NonCancellable guards only the resource release — the follower's allocator must close
    // cleanly once teardown begins, whatever the cancellation (bounded: the follower's coroutines just
    // unwind). Watermark/pendingBlock stay readable after close (not allocator-backed).
    private suspend fun cutoverToLeader(following: Following, termId: Long) {
        val pendingBlock = following.proc.pendingBlock
        try {
            LOG.debug("[$dbName] transition: closing follower")
            withContext(NonCancellable) {
                following.job.cancelAndJoin()
                following.proc.close()
            }

            openTransition(termId).use { transition ->
                if (pendingBlock != null) {
                    LOG.debug("[$dbName] transition: finishing pending block b${pendingBlock.blockIdx} with ${pendingBlock.bufferedRecords.size} buffered records")
                    blockUploader.uploadBlock(
                        pendingBlock.boundaryMsgId, termId, pendingBlock.boundaryMessage,
                    )
                    LOG.debug("[$dbName] transition: replaying ${pendingBlock.bufferedRecords.size} buffered records through transition processor")
                    transition.processRecords(pendingBlock.bufferedRecords)
                }
            }

            LOG.debug("[$dbName] transition: building leader processor")
            val resumeAfterMsgId = watchers.latestSourceMsgId

            // Built, not committed: `state` moves to Prepared but no records flow as leader until
            // commitLeader installs it at the serialization point.
            state = openLeader(termId, resumeAfterMsgId)
        } catch (e: Throwable) {
            state = openFollower(pendingBlock)
            throw e
        }
    }

    override fun commitLeader(partition: Int): Log.TailSpec<SourceMessage> {
        val prepared = (state as? Prepared)
            ?: throw Fault(
                "[$dbName] commitLeader without a prepared leader (${state::class.simpleName})",
                "xtdb/log-commit-not-prepared"
            )
        state = Leading(prepared.proc, prepared.job)
        LOG.info("[$dbName] leader startup complete, resuming after ${prepared.resumeAfterMsgId}")
        return Log.TailSpec(prepared.resumeAfterMsgId, prepared.proc.srcLogProc)
    }

    override suspend fun demoteLeader(partition: Int) {
        // Genuine revoke (Leading) and abandoning an uncommitted leader (Prepared) tear down the
        // same way; an already-following listener is a no-op.
        val leader = when (val s = state) {
            is Following -> {
                LOG.debug("[$dbName] demote — already follower, no transition needed")
                return
            }

            is Leader -> s
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
