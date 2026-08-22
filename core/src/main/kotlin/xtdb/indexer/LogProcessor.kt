package xtdb.indexer

import io.micrometer.core.instrument.Gauge
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.cancelChildren
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.job
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
import xtdb.api.error.Interrupted
import xtdb.types.MessageId
import xtdb.util.debug
import xtdb.util.error
import xtdb.util.info
import xtdb.util.logger
import xtdb.util.warn
import java.time.Duration
import java.time.InstantSource
import kotlin.coroutines.coroutineContext
import kotlin.random.Random

private val LOG = LogProcessor::class.logger

// Shutdown, not a fault. MUST NOT reach `Watchers.notifyError`: `Failed` is absorbing, so a clean
// resignation or a node teardown would leave the database unqueryable until the process restarts.
internal val Throwable.isShutdownSignal
    get() = this is CancellationException || this is InterruptedException || this is Interrupted

/**
 * Run a resolution task, routing failures onto its completion handle (and any external-source result) so
 * that no caller hangs. Interrupts are shutdown signals, not ingestion faults, so they don't poison the
 * watchers. A successful source batch completes its own handle, possibly deferred if a block cut paused it.
 */
internal inline fun Watchers.runTaskGuarded(
    onComplete: kotlinx.coroutines.CompletableDeferred<Unit>,
    extResult: kotlinx.coroutines.CompletableDeferred<TransactionResult>? = null,
    block: () -> Unit,
) {
    try {
        block()
    } catch (e: CancellationException) {
        if (!onComplete.isCompleted) onComplete.cancel(e)
        throw e
    } catch (e: InterruptedException) {
        if (!onComplete.isCompleted) onComplete.completeExceptionally(e)
        throw e
    } catch (e: Interrupted) {
        if (!onComplete.isCompleted) onComplete.completeExceptionally(e)
        throw e
    } catch (e: Throwable) {
        notifyError(e)
        if (!onComplete.isCompleted) onComplete.completeExceptionally(e)
        extResult?.let { if (!it.isCompleted) it.completeExceptionally(e) }
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
    // Applying is where a supersession fails the term; let it propagate (interrupts too).
    term.onReplicaMsg { record ->
        try {
            term.applyRecord(record)
        } catch (e: CancellationException) {
            throw e
        } catch (e: LeaderSupersededException) {
            throw e
        } catch (e: InterruptedException) {
            throw e
        } catch (e: Interrupted) {
            throw e
        } catch (e: Throwable) {
            watchers.notifyError(e)
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
        // superseded by a newer leader — expected, not a query-facing fault; the owner re-follows.
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
 *
 * [selectElection] arms whatever the follower's owner concludes from quiet, on this loop rather than a
 * loop of its own, so a conclusion and the record that would have withdrawn it cannot be acted on at once.
 */
internal suspend fun runFollower(
    follower: FollowerLogProcessor,
    selectElection: SelectBuilder<Unit>.() -> Unit,
) {
    try {
        while (true) selectUnbiased { with(follower) { selectWork() }; selectElection() }
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
    private val mayLead: Boolean,
    private val electionConfig: ElectionConfig,
    private val skipTxs: Set<MessageId> = emptySet(),
    private val flushTimeout: Duration,
    private val instantSource: InstantSource = InstantSource.system(),
    electionRandom: Random = Random.Default,
    private val driver: ElectionDriver = QuietDriver(electionConfig, instantSource, electionRandom),
    private val gcDispatcher: CoroutineDispatcher = Dispatchers.IO,
) : AutoCloseable {

    private val replicaLog = partitionStorage.replicaLog
    private val sourceLog = partitionStorage.sourceLog
    private val hasExternalSource = externalSourceFactory != null

    // The role state machine — see allium/log-processor-lifecycle.allium. The spec's `claiming` covers
    // both variants here: `Claiming` while the verdict is open, `TakingOver` once it has conferred and
    // the pipeline swap is in flight.
    //
    // `state` has one writer at a time, by handoff rather than by lock: the elector (on the follower's
    // work coroutine) writes while a follower runs, and stops the moment it publishes `TakingOver`; the
    // takeover coroutine writes only from there, and joins that follower before it swaps; the leader's
    // term job writes only the resignation back to `Following`, after every coroutine of the term it is
    // ending has finished. Each writer creates the loop that will write next.
    //
    // The leader and the follower process different message types, so the only thing a state can offer
    // without narrowing to its variant is the teardown: a role is stopped by cancelling its job and freed
    // by closing its processor, in that order (dev/doc/coroutines.adoc).
    private sealed interface State {
        val proc: AutoCloseable
        val job: Job
    }

    private class Following(override val proc: FollowerLogProcessor, override val job: Job) : State

    private class Claiming(
        override val proc: FollowerLogProcessor, override val job: Job, val claim: Claim,
    ) : State

    // The claim conferred and the pipeline swap is in flight; the follower is still running until the
    // takeover joins it. A separate variant so the verdict is reached exactly once — the elector has
    // nothing left to decide on a state that carries no claim.
    private class TakingOver(override val proc: FollowerLogProcessor, override val job: Job) : State

    private class Leading(override val proc: LeaderLogProcessor, override val job: Job) : State

    // The elector reports reads and draws the one conclusion a *record* supports — a claim's verdict.
    // Everything a stretch of quiet supports is the driver's to raise, and arrives at [onQuiet].
    private val elector = object : Elector {
        override suspend fun onRecord(termBefore: Long, msgId: MessageId) {
            driver.onRecord()

            val s = state
            if (s is Claiming && msgId >= s.claim.msgId) {
                // The verdict (see TheLocalTestDecidesTheSameVerdict in the spec): the claim conferred
                // iff nothing at or above its term preceded it. `termBefore` rather than the fence's
                // current value, which has already absorbed the claim's own term and would find every
                // same-term rival conferring.
                if (termBefore < s.claim.term) {
                    LOG.info("[$dbName] claim at term ${s.claim.term} conferred leadership; taking over")
                    driver.idle()
                    state = TakingOver(s.proc, s.job)
                    scope.launch { takeOver(s.proc, s.job, s.claim.term) }
                } else {
                    LOG.info("[$dbName] lost the election at term ${s.claim.term}; following")
                    driver.await(Quiet.ELECTION)
                    state = Following(s.proc, s.job)
                }
            }
        }

        override suspend fun onEmptyRead() = driver.onEmptyRead()
    }

    /** The log stayed quiet for as long as this state was waiting on, so act on what the state was for. */
    private suspend fun onQuiet() {
        when (val s = state) {
            is Following -> claim(s)

            is Claiming -> {
                // Our reader is not delivering, so no verdict can be reached; holding the claim open on
                // the strength of a prefix we are no longer being given helps nobody, least of all a
                // deployment where we are the only eligible node.
                LOG.warn("[$dbName] claim at term ${s.claim.term} not read back; abandoning")
                driver.backOff()
                state = Following(s.proc, s.job)
            }

            // Neither waits on quiet, so neither can be here.
            is TakingOver, is Leading -> {}
        }
    }

    /** Bid for leadership: one term above everything seen, adjudicated when the log hands it back. */
    private suspend fun claim(following: Following) {
        val term = partitionState.termFence.highest + 1

        val claim = try {
            Claim(term, replicaLog.appendMessage(ReplicaMessage.NoOp(termId = term)).msgId)
        } catch (e: Throwable) {
            if (e.isShutdownSignal) throw e
            // A refused append wrote nothing, so there is nothing to adjudicate: wait afresh, so we
            // don't re-claim on every read for as long as appends are refused.
            LOG.warn(e, "[$dbName] claim append refused; holding off")
            driver.await(Quiet.ELECTION)
            return
        }

        LOG.info("[$dbName] claimed leadership at term ${term} (claim at ${claim.msgId})")
        driver.await(Quiet.CLAIM_VERDICT)
        state = Claiming(following.proc, following.job, claim)
    }

    /**
     * Swap the follower for a leader, the claim having conferred.
     *
     * By the time the verdict is known the claim has been read back, so the follower has already caught
     * up to it — there is no catch-up await left to do. Once the follower is stopped, `state` references
     * a dead pipeline until Leading is published, so any early exit re-opens a live follower from the
     * catch and `state` never keeps a corpse. NonCancellable guards the resource release — the follower's
     * allocator must close cleanly once teardown begins, whatever the cancellation (bounded: the
     * follower's coroutines just unwind). Watermark/pendingBlock stay readable after close (not
     * allocator-backed).
     */
    private suspend fun takeOver(follower: FollowerLogProcessor, followerJob: Job, termId: Long) {
        val pendingBlock = follower.pendingBlock
        try {
            LOG.debug("[$dbName] takeover: closing follower")
            withContext(NonCancellable) {
                followerJob.cancelAndJoin()
                follower.close()
            }

            // The claim conferred against the prefix that preceded it, and the follower went on reading
            // until we joined it just now. A higher term arriving in that window has already superseded
            // this claim — leading under it would acknowledge writes every other node discards, and the
            // consume-back cannot catch it, because this term would resume past the record that carries it.
            if (partitionState.termFence.highest > termId) {
                LOG.info("[$dbName] superseded at term ${partitionState.termFence.highest} before taking over at $termId; following")
                state = openFollower(pendingBlock)
                return
            }

            openTransition(termId).use { transition ->
                if (pendingBlock != null) {
                    LOG.debug("[$dbName] takeover: finishing pending block b${pendingBlock.blockIdx} with ${pendingBlock.bufferedRecords.size} buffered records")
                    blockUploader.uploadBlock(
                        pendingBlock.boundaryMsgId, termId, pendingBlock.boundaryMessage,
                    )
                    LOG.debug("[$dbName] takeover: replaying ${pendingBlock.bufferedRecords.size} buffered records through transition processor")
                    transition.processRecords(pendingBlock.bufferedRecords)
                }
            }

            LOG.debug("[$dbName] takeover: building leader")
            state = openLeader(termId, watchers.latestSourceMsgId)
        } catch (e: Throwable) {
            state = openFollower(pendingBlock)
            if (!e.isShutdownSignal) {
                LOG.error(e, "[$dbName] takeover: failed to build leader")
                watchers.notifyError(e)
            }
        }
    }

    private fun openLeader(termId: Long, resumeAfterMsgId: MessageId): Leading {
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

        // On the reader rather than the work loop, so an upload in flight cannot silence the leader —
        // a leader that falls quiet exactly while it is doing the most is read as absent, and evicted.
        val assertion = LeadershipAssertion(electionConfig.assertionInterval, instantSource) {
            replicaAppender.append(ControlItem(ReplicaMessage.NoOp(termId = termId)))
        }

        // Reading is the partition's for a leader too — the term's own writes are confirmed by arriving
        // back here. One scope over all of them, so a read that fails cancels the term: consume-back is
        // the only thing that acks a write, so a term outliving its reader hangs whatever is staged on it.
        val termJob = scope.launch {
            try {
                coroutineScope {
                    launch {
                        tailReplica { records ->
                            if (records.isEmpty()) assertion.onEmptyRead()
                            else {
                                records.forEach { proc.queueReplicaMessage(it) }
                                assertion.onRecord()
                            }
                        }
                    }
                    launch { proc.gc.runGc() }
                    proc.extSrcProc?.let { extSrcProc -> launch { extSrcProc.run() } }
                    launch { tailSource(resumeAfterMsgId, proc.srcLogProc) }

                    runLeaderTerm(dbName, watchers, proc, replicaAppender)

                    // The term ended itself and reported its own cause; stop its reader and collectors.
                    coroutineContext.job.cancelChildren()
                }
            } catch (e: CancellationException) {
                // External teardown: whoever cancelled owns the state, and the database is going away.
                throw e
            } catch (e: Throwable) {
                // A sibling of the term (its reader, its source tail, a collector) failed and cancelled
                // it; the sibling reported before rethrowing.
                LOG.error(e) { "[$dbName] leader term's pipeline failed" }
            }

            resignToFollower(proc)
        }

        return Leading(proc, termJob)
    }

    // Resign, on the outgoing term's own coroutine: every coroutine of the term has finished by here, so
    // closing the processor is safe, and `pendingBlock` stays readable after the close (not
    // allocator-backed) to seed the follower.
    private fun resignToFollower(proc: LeaderLogProcessor) {
        LOG.info("[$dbName] leader term over; following")
        proc.close()
        state = openFollower(proc.pendingBlock)
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

    // Read the source log into the leader's source processor from where the last term left off.
    private suspend fun tailSource(afterMsgId: MessageId, proc: SourceLogProcessor) {
        try {
            sourceLog.tailAll(afterMsgId, proc)
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
            elector, dbCatalog, pendingBlock,
            hasExternalSource = hasExternalSource,
            meterRegistry = base.meterRegistry,
        )

        // Eligibility decides whether quiet means anything here at all: an ineligible node reads its log
        // like any other follower, and there is no length of silence it is entitled to conclude from.
        if (mayLead) driver.await(Quiet.ELECTION) else driver.idle()

        return Following(proc, scope.launch {
            launch { tailReplica(proc::queueRecords) }
            runFollower(proc) { driver.onTimeout { onQuiet() } }
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

    override fun close() = state.proc.close()

    /**
     * Run one cycle of every garbage collector owned by the leader (block + trie) and wait for
     * both. No-op unless leading — GC only runs on the leader. Bypasses the collectors'
     * `enabled` flag (which gates the auto-signal from the block-boundary path, not direct calls).
     */
    fun awaitNoGarbageBlocking() = (state as? Leading)?.proc?.gc?.awaitNoGarbageBlocking()
}
