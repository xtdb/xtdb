package xtdb.indexer

import io.micrometer.core.instrument.Gauge
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
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
 * Run a leader term until it ends, then fail everything staged on it.
 *
 * The term's work, its append pump and its replica reader are structured together, so whichever fails
 * first cancels the others and arrives here as the cause. Cancelling the caller is what ends a term that
 * hasn't failed. The reader belongs in here for the same reason the pump does: consume-back is the only
 * thing that acks a write, so a term outliving its reader hangs whatever is staged on it.
 *
 * Which failures reach the watchers is decided here rather than in the term, because the term is not the
 * thing that knows a resignation from a fault: a supersession means this node is merely no longer the
 * leader, and poisoning the watchers over it would leave a healthy database unqueryable (#5817).
 */
internal suspend fun runLeaderTerm(
    dbName: DatabaseName,
    watchers: Watchers,
    term: LeaderLogProcessor,
    replicaMsgs: ReceiveChannel<Log.Record<ReplicaMessage>>,
    appender: ReplicaLogAppender,
    readReplica: suspend () -> Unit,
) {
    try {
        coroutineScope {
            launch(CoroutineName("$dbName-replica-appender")) { appender.run() }
            launch(CoroutineName("$dbName-replica-reader")) { readReplica() }

            while (true)
                selectUnbiased {
                    replicaMsgs.onReceive {
                        val msg = it.message
                        val termId = msg.termId

                        if (termId > term.leaderTerm)
                            throw LeaderSupersededException("[$dbName] superseded: read term $termId > our term ${term.leaderTerm} at ${it.msgId}")

                        // Below our term should not appear past our replay target; discard defensively, still advancing.

                        if (termId != 0L && termId < term.leaderTerm) {
                            LOG.debug { "[$dbName] leader: discarding stale-term record ${it.msgId} (term $termId < ${term.leaderTerm})" }
                            watchers.notifyApplied(replicaMsgId = it.msgId)
                        } else {
                            term.applyReplicaMessage(it)
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

    /**
     * The highest leader term this partition has seen on its replica log.
     *
     * Seeded from the persisted block boundary and only ever raised, so it spans every role change —
     * held per role it would be reseeded from the boundary at each one, forgetting every term written
     * since the last block flush, and the same term could be admitted twice either side of a demote.
     */
    val termFence = TermFence(dbName, partitionState.tableCatalogOrNull?.boundaryTermId ?: LeaderTerm.NONE)

    // The role state machine — see allium/log-processor-lifecycle.allium.
    // `state` is written by the off-thread transition coroutine (cutoverToLeader → Leading, or its catch
    // → Following) and by demoteLeader on the transport's thread. They don't race: a revoke
    // cancel-and-joins the transition before demoteLeader reads `state`.
    //
    // The leader and the follower process different message types, so the only thing a state can offer
    // without narrowing to its variant is the teardown: a role is stopped by cancelling its job and freed
    // by closing its processor, in that order (dev/doc/coroutines.adoc).
    private sealed interface State {
        val proc: AutoCloseable
        val job: Job
    }

    private class Following(override val proc: FollowerLogProcessor, override val job: Job) : State

    private class Leading(override val proc: LeaderLogProcessor, override val job: Job) : State

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
            dbCatalog, pendingBlock, termFence,
            hasExternalSource = hasExternalSource,
            meterRegistry = base.meterRegistry,
        )

        val inbound = Channel<List<Log.Record<ReplicaMessage>>>()

        return Following(
            proc,

            scope.launch {
                launch { tailReplica(inbound::send) }

                for (batch in inbound) {
                    proc.processRecords(batch)
                }
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
                        replicaAppender, termFence,
                        externalSource,
                        skipTxs, dbCatalog,
                        leaderTerm = termId,
                        flushTimeout = flushTimeout,
                        gcDispatcher = gcDispatcher,
                    )

                    pendingBlock?.let { proc.applyPendingBlock(it) }

                    LOG.debug("[${dbName}] transition: building leader processor")
                    val resumeAfterMsgId = watchers.latestSourceMsgId

                    // Records read back off the replica log, awaiting application. Its capacity is what bounds
                    // how far the term's reader may run ahead of the apply loop.
                    val replicaMsgs = Channel<Log.Record<ReplicaMessage>>(capacity = 128)

                    // Reading is the partition's for a leader too — the term's own writes are confirmed by
                    // arriving back here. The GCs and the external source are the term's to stop, so
                    // `shutdown` reaches them; the reader is structured into the term instead.
                    val termJob = scope.launch {
                        launch { proc.gc.runGc() }
                        proc.extSrcProc?.let { extSrcProc -> launch { extSrcProc.run() } }

                        runLeaderTerm(dbName, watchers, proc, replicaMsgs, replicaAppender) {
                            tailReplica { records -> records.forEach { replicaMsgs.send(it) } }
                        }
                    }

                    val leading = Leading(proc, termJob).also { state = it }

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
