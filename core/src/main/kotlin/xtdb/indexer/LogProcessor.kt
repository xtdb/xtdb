package xtdb.indexer

import io.micrometer.core.instrument.Gauge
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.selects.selectUnbiased
import org.apache.arrow.memory.BufferAllocator
import xtdb.NodeBase
import xtdb.api.DatabaseName
import xtdb.api.error.Interrupted
import xtdb.api.log.*
import xtdb.api.log.ReplicaMessage.NoOp
import xtdb.api.tx.ExternalSource
import xtdb.indexer.TermFence.Admission
import xtdb.indexer.TermFence.Admission.CONFERRING
import xtdb.indexer.TermFence.Admission.FENCED
import xtdb.compactor.Compactor
import xtdb.database.Database
import xtdb.database.PartitionState
import xtdb.database.PartitionStorage
import xtdb.types.MessageId
import xtdb.util.closeOnCatch
import xtdb.util.debug
import xtdb.util.error
import xtdb.util.info
import xtdb.util.logger
import java.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

private val LOG = LogProcessor::class.logger

// Shutdown, not a fault. MUST NOT reach `Watchers.notifyError`: `Failed` is absorbing, so a clean
// resignation or a node teardown would leave the database unqueryable until the process restarts.
internal val Throwable.isShutdownSignal
    get() = this is CancellationException || this is InterruptedException || this is Interrupted

/**
 * Re-cast a term-teardown cause as a cancellation, preserving the original for the logs.
 *
 * The failure *kind* is load-bearing for anything the term's source-log tail observes: a
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
    sourceLog: PartitionLog<SourceMessage>,
    resumeAfterMsgId: MessageId,
) {
    try {
        coroutineScope {
            launch(CoroutineName("$dbName-replica-appender")) { appender.run() }
            launch(CoroutineName("$dbName-gc")) { term.gc.runGc() }

            // The source log is the leader's alone to read, so its tail is structured with the term.
            launch(CoroutineName("$dbName-source-tail")) { sourceLog.tailAll(resumeAfterMsgId, term.srcLogProc) }

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

/**
 * How often a leader on this log asserts, and with it — at five to ten times this — how long a follower
 * polls before claiming.
 *
 * An in-process log makes a leader's write visible to a follower in microseconds; Kafka puts a network
 * round-trip and a partition-leader election in front of it, and a failover has to outlast those. An
 * unrecognised log takes the conservative figure, so forgetting one here costs an election's latency
 * rather than its correctness.
 */
private fun assertIntervalFor(replicaLog: Log<*>) = when (replicaLog) {
    is InMemoryLog, is LocalLog -> 100.milliseconds
    else -> 1.seconds
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
    private val electionDriver: ElectionDriver =
        RealElectionDriver(assertIntervalFor(partitionStorage.logs.replicaLog)),
    private val readOnly: Boolean = false,
) : AutoCloseable {

    private val replicaLog = partitionStorage.replicaLog
    private val hasExternalSource = externalSource != null

    val termFence = TermFence(partitionState.tableCatalogOrNull?.boundaryTermId ?: LeaderTerm.NONE)

    // The role state machine — see allium/log-processor-lifecycle.allium.
    private sealed interface State : AutoCloseable {
        val proc: AutoCloseable

        val scope: CoroutineScope

        val pendingBlock: PendingBlock?

        val job get() = scope.coroutineContext.job

        suspend fun handleReplicaMessage(record: Log.Record<ReplicaMessage>)
    }

    private class Following(override val proc: FollowerLogProcessor, override val scope: CoroutineScope) : State {
        /** Set while a claim of ours is in flight, cleared when we read it back — see [adjudicateClaim]. */
        var claimMsgId: MessageId? = null

        override val pendingBlock get() = proc.pendingBlock

        override fun close() = proc.close()

        override suspend fun handleReplicaMessage(record: Log.Record<ReplicaMessage>) {
            scope.async { proc.handleRecord(record) }.await()
        }
    }

    private class Leading(
        override val proc: LeaderLogProcessor,
        override val scope: CoroutineScope,
        private val replicaMsgs: SendChannel<ReplicaApply>,
        private val extSrcJob: Job?,
    ) : State {
        override val pendingBlock get() = proc.pendingBlock

        override fun close() {
            extSrcJob?.cancel()
            proc.close()
        }

        override suspend fun handleReplicaMessage(record: Log.Record<ReplicaMessage>) {
            scope.async { replicaMsgs.applyAndAwait(record) }.await()
        }
    }

    // A role's scope is the database's, with a job of its own so that stopping the role leaves the
    // partition's tail running. Derived from `scope` rather than built from the job alone: a bare
    // `CoroutineScope(job)` carries no dispatcher, so every apply would land on Dispatchers.Default —
    // which under a simulation's virtual clock is a deadlock, the scheduler having nothing left to advance.
    private fun roleScope(job: Job) = scope + job

    /**
     * The partition's replica-log reader, and with it this node's whole part in leader election.
     *
     * An empty poll is what a claim turns on, and polling is what makes that safe: it is this node's own
     * observation that it looked and found the log at its tip. A stopwatch would run down just as happily
     * on a subscription that had stopped delivering, and a node that claimed on one would fence the
     * incumbent it cannot see, fail to confirm its own claim, and repeat — leaving nobody leading at all.
     */
    private suspend fun tailReplica() {
        try {
            replicaLog.withTail(watchers.latestReplicaMsgId) { tail ->
                // No term has ever led here, so nothing read could confer anything and term 1 is the bottom
                // of the ordering — sparing a fresh database an election timeout before its first write.
                if (termFence.highestSeen == LeaderTerm.NONE) claimLeadership()

                while (true) {
                    val records = tail.poll(electionDriver.electionTimeout())

                    currentCoroutineContext().ensureActive()
                    if (state.job.isCompleted) reopenFollower()

                    if (records.isEmpty()) claimLeadership() else records.forEach { handleRecord(it) }
                }
            }
        } catch (e: Throwable) {
            if (!e.isShutdownSignal) watchers.notifyError(e)
            throw e
        }
    }

    /**
     * Replace the live role, whose job has completed, with a follower seeded from where it got to.
     *
     * A leader term ends itself — on supersession, or on a failure it has already reported — so nothing
     * external demotes us, and putting a follower back is the reader's own job.
     */
    private fun reopenFollower() {
        val role = state
        LOG.info("[$dbName] role ended — re-opening follower")
        // `pendingBlock` stays readable after the close — it isn't allocator-backed — so we free the old
        // role before reading it to seed the follower.
        role.close()
        state = openFollower(role.pendingBlock)
    }

    /**
     * Append a no-op one term above the highest seen, if we are in any position to.
     *
     * The claim's identity is its position in the log, so there is no handshake and nothing to match a
     * verdict against: we read our own record back like any other, and [adjudicateClaim] decides there
     * whether it conferred.
     */
    private suspend fun claimLeadership() {
        if (readOnly) return
        val following = state as? Following ?: return
        if (following.claimMsgId != null) return

        val termId = termFence.highestSeen + 1
        following.claimMsgId = replicaLog.appendMessage(NoOp(termId = termId)).msgId
        LOG.debug("[$dbName] claiming leadership at term ${LeaderTerm.format(termId)}")
    }

    private suspend fun handleRecord(record: Log.Record<ReplicaMessage>) {
        // Folded outside the retry below, so a record offered again is not folded twice.
        val admission = termFence.admit(record.message.termId)

        if (admission == FENCED) {
            // A higher-term leader has superseded this message's writer. Discard it, but still advance the
            // consume position — discard suppresses application, not consumption — so a catch-up can't
            // hang on a fenced no-op.
            LOG.debug {
                "[$dbName] discarding fenced record ${record.msgId} " +
                        "(term ${LeaderTerm.format(record.message.termId)} < " +
                        "${LeaderTerm.format(termFence.highestSeen)})"
            }
            watchers.notifyApplied(record.msgId)
        } else {
            // A role ending cancels the handle mid-record, leaving the record unapplied and its
            // position unadvanced — so it is offered again to whatever replaces that role.
            while (true) {
                val role = state

                try {
                    role.handleReplicaMessage(record)
                    break
                } catch (_: CancellationException) {
                    // The cancel came from that role ending, so its job is on its way to completing — join
                    // before replacing it, or `openFollower` would race the teardown it is seeded from.
                    role.job.join()
                    reopenFollower()
                } catch (e: Throwable) {
                    LOG.error(
                        e,
                        "[$dbName] failed to process replica record ${record.msgId} (${record.message::class.simpleName})"
                    )
                    throw e
                }
            }
        }

        // A fenced record is adjudicated too: a claim of ours that lost to a term already ahead of it comes
        // back fenced, and leaving `claimMsgId` set would stop this node ever claiming again.
        adjudicateClaim(record, admission)
    }

    /**
     * Decide what a claim of ours came to, now that we have read it back.
     *
     * Applying before adjudicating is what makes "our claim is read back before we lead" structural rather
     * than a wait: by the time we get here the record has been applied, so there is nothing left to await.
     */
    private suspend fun adjudicateClaim(record: Log.Record<ReplicaMessage>, admission: Admission) {
        val following = state as? Following ?: return
        if (record.msgId != following.claimMsgId) return

        following.claimMsgId = null

        if (admission == CONFERRING) cutOverToLeader(following, record.message.termId)
        else LOG.debug("[$dbName] claim at ${record.msgId} conferred nothing — still following")
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

    // The replica reader is the sole writer, so no flow and no retry-on-change: it reopens the follower
    // itself. Volatile because the `xtdb.log.leader` gauge reads it from the metrics thread.
    @Volatile
    private var state: State = openFollower()

    /**
     * Whether this node holds a LIVE leader term — what `xtdb.log.leader` reports.
     *
     * A term that has ended stays in `state` until the reader's next poll reconciles it, and a node in
     * that window leads nothing.
     */
    val isLeader get() = state.let { it is Leading && !it.job.isCompleted }

    init {
        base.meterRegistry?.let { reg ->
            Gauge.builder("xtdb.log.leader", this) { if (it.isLeader) 1.0 else 0.0 }
                .description("1 if this node is the log leader, 0 if follower")
                .tag("db", dbName)
                .register(reg)
        }

        scope.launch(CoroutineName("$dbName-replica-tail")) { tailReplica() }
    }

    /**
     * Stop following and start leading at [termId].
     *
     * Runs inline on the reader, so nothing is applied while it is in flight — which is what closes the
     * window a rival's claim would otherwise land in. That claim necessarily follows ours in the log, and
     * we meet it on the next poll.
     */
    private suspend fun cutOverToLeader(following: Following, termId: Long) {
        LOG.info("[$dbName] claim at term ${LeaderTerm.format(termId)} conferred leadership")

        val pendingBlock = following.pendingBlock

        // The point of no return. Once the follower is stopped, `state` references a dead role until
        // Leading is published, so any early exit has to re-open a live follower, seeded from where this
        // one got to. That recovery is structural rather than flag-guarded: reaching the catch below *is*
        // "the follower was stopped".
        try {
            // Guards the release alone: the follower's allocator must close cleanly once teardown has
            // begun, whatever the cancellation. Bounded — its coroutines only unwind.
            withContext(NonCancellable) {
                following.job.cancelAndJoin()
                following.proc.close()
            }

            val driver = RealLeaderDriver(partitionStorage, partitionState, blockUploader)
            val replicaAppender = ReplicaLogAppender(driver, termId, electionDriver)

            // Closed here on the way out because it is not in `state` yet, so nothing else can reach it
            // to close it — and its resolver holds a child allocator that would refuse the database's own
            // close for the rest of the node's life.
            val proc = LeaderLogProcessor(
                allocator, base, partitionStorage, crashLogger, partitionState, dbName, driver, watchers,
                replicaAppender,
                externalSource,
                skipTxs, dbCatalog,
                leaderTerm = termId,
                flushTimeout = flushTimeout,
                gcDispatcher = gcDispatcher,
            ).closeOnCatch { proc ->
                pendingBlock?.let { proc.applyPendingBlock(it) }
                proc
            }

            val resumeAfterMsgId = watchers.latestSourceMsgId

            // The handover from the partition's reader. Unbuffered: the reader waits on each record's own
            // handle, so a buffer would only let it read ahead of a term about to end.
            val replicaMsgs = Channel<ReplicaApply>()

            val termJob = scope.launch(CoroutineName("$dbName-term")) {
                runLeaderTerm(
                    dbName, watchers, proc, replicaMsgs, replicaAppender,
                    partitionStorage.sourceLog, resumeAfterMsgId
                )
            }

            // Not launched inside the term: a term that ends hands its cause to the source through the
            // task channel, so the source's next submit is where a fire-and-forget caller learns why
            // (#5711) — and a source cancelled along with the term would learn only that it was
            // cancelled. `retire` cancels it a poll later, so one that never submits again doesn't run on.
            val extSrcJob = proc.extSrcProc?.let { extSrcProc ->
                scope.launch(CoroutineName("$dbName-ext-source")) { extSrcProc.run() }
            }

            state = Leading(proc, roleScope(termJob), replicaMsgs, extSrcJob)

            LOG.info("[$dbName] leader startup complete, resuming after $resumeAfterMsgId")
        } catch (e: Throwable) {
            state = openFollower(pendingBlock)
            if (e.isShutdownSignal) throw e

            // A promotion that fails costs this node the leadership and nothing else — another node's
            // claim confers where ours didn't. Rethrowing would unwind the partition's reader, so the
            // follower just re-opened would never read another record.
            LOG.error(e) { "[$dbName] promotion failed — still following" }
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

    override fun close() = state.proc.close()

    /**
     * Run one cycle of every garbage collector owned by the leader (block + trie) and wait for
     * both. No-op unless leading — GC only runs on the leader. Bypasses the collectors'
     * `enabled` flag (which gates the auto-signal from the block-boundary path, not direct calls).
     */
    fun awaitNoGarbageBlocking() = (state as? Leading)?.proc?.gc?.awaitNoGarbageBlocking()
}
