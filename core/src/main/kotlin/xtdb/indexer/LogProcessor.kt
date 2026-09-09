package xtdb.indexer

import io.micrometer.core.instrument.Gauge
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.SendChannel
import org.apache.arrow.memory.BufferAllocator
import xtdb.NodeBase
import xtdb.api.DatabaseName
import xtdb.api.error.Interrupted
import xtdb.api.log.*
import xtdb.api.log.ReplicaMessage.NoOp
import xtdb.indexer.TermFence.Admission.ADMITTED
import xtdb.indexer.TermFence.Admission.CONFERRING
import xtdb.indexer.TermFence.Admission.FENCED
import xtdb.api.tx.ExternalSource
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
import xtdb.util.warn
import java.time.Duration
import java.util.concurrent.atomic.AtomicLong

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
 * state, live index and tx resolver with the clauses [LeaderLogProcessor.runTerm] arms — concurrently, a
 * tx could resolve during a block cut, which the term treats as unreachable.
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
    private val electionDriver: ElectionDriver = RealElectionDriver(partitionStorage.logs.replicaLog),
    private val readOnly: Boolean = false,
) : AutoCloseable {

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

    val termFence = TermFence(partitionState.tableCatalogOrNull?.boundaryTermId ?: 0)

    /** Volatile: the tail is the only writer, but tests read it from another thread. */
    @Volatile
    internal var latestReplicaMsgId: MessageId =
        partitionState.tableCatalogOrNull?.boundaryReplicaMsgId ?: -1
        private set

    // The role state machine — see allium/log-processor-lifecycle.allium.
    private sealed interface State : AutoCloseable {
        val scope: CoroutineScope

        val job get() = scope.coroutineContext.job

        suspend fun handleReplicaMessage(record: Log.Record<ReplicaMessage>)
    }

    private class Following(val proc: FollowerLogProcessor, override val scope: CoroutineScope) : State {
        /** Set while a claim of ours is in flight, cleared when we read it back — see [adjudicateClaim]. */
        var claimMsgId: MessageId? = null

        override fun close() = proc.close()

        override suspend fun handleReplicaMessage(record: Log.Record<ReplicaMessage>) {
            scope.async { proc.handleRecord(record) }.await()
        }
    }

    private class Leading(
        val proc: LeaderLogProcessor,
        override val scope: CoroutineScope,
        private val replicaMsgs: SendChannel<ReplicaApply>,
    ) : State {
        override fun close() = proc.close()

        override suspend fun handleReplicaMessage(record: Log.Record<ReplicaMessage>) {
            scope.async { replicaMsgs.applyAndAwait(record) }.await()
        }

        fun awaitNoGarbageBlocking() = proc.gc.awaitNoGarbageBlocking()
    }

    // A role's scope is the database's, with a job of its own so that stopping the role leaves the
    // partition's tail running. Derived from `scope` rather than built from the job alone: a bare
    // `CoroutineScope(job)` carries no dispatcher, so every apply would land on Dispatchers.Default —
    // which under a simulation's virtual clock is a deadlock, the scheduler having nothing left to advance.
    private fun roleScope(job: Job) = scope + job

    private suspend fun tailReplica() {
        try {
            replicaLog.withTail(latestReplicaMsgId) { tail ->
                // Claimed before reading anything, so a database nobody has led is writable without waiting out an election.
                if (termFence.highestSeen == 0L) claimLeadership()

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

    private fun reopenFollower() {
        val role = state
        LOG.info("[$dbName] role ended — re-opening follower")

        // Read after the role's job has completed, which is the edge that publishes it: a term applies until then, and one of those applies may be the adopt that closes this block.
        // See [BlockCutter.pendingBlock] for what dropping the handover costs.
        val pendingBlock = (role as? Leading)?.proc?.pendingBlock

        role.close()
        state = openFollower(pendingBlock)
    }

    private suspend fun claimLeadership() {
        if (readOnly) return
        val following = state as? Following ?: return
        if (following.claimMsgId != null) return

        val termId = termFence.highestSeen + 1

        try {
            following.claimMsgId = logsDriver.appendToReplica(NoOp(termId = termId)).msgId
        } catch (e: Throwable) {
            if (e.isShutdownSignal) throw e

            // Not reported: a replica log refusing writes would otherwise fail every follower at once, none of which was leading.
            LOG.warn(e, "[$dbName] could not append a leadership claim — still following")
            return
        }

        LOG.debug("[$dbName] claiming leadership at term ${LeaderTerm.format(termId)}")
    }

    private suspend fun handleRecord(record: Log.Record<ReplicaMessage>) {
        val msg = record.message

        val admission =
            if (msg is ReplicaMessage.BlockUploaded)
                if (termFence.permits(msg.termId)) ADMITTED else FENCED
            else termFence.admit(msg.termId)

        // A fenced BlockUploaded still goes to the role: dropping it here would leave the block it closes open for the life of the process.
        if (admission == FENCED && msg !is ReplicaMessage.BlockUploaded)
            LOG.debug { "[$dbName] discarding fenced record ${record.msgId} (term ${msg.termId} < ${termFence.highestSeen})" }
        else
            // A role ending cancels the handle mid-record, so the record is re-offered to whatever replaces that role.
            while (true) {
                val role = state

                try {
                    role.handleReplicaMessage(record)
                    break
                } catch (_: CancellationException) {
                    // The cancel came from that role ending, so join before replacing it, or `openFollower` would race the teardown it is seeded from.
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

        // Below the apply: a role ending mid-record leaves the position short, which is what re-offers the record.
        // A record the tail fenced or the role held advances it all the same.
        latestReplicaMsgId = record.msgId

        adjudicateClaim(record, admission == CONFERRING)
    }

    private suspend fun adjudicateClaim(record: Log.Record<ReplicaMessage>, conferring: Boolean) {
        val following = state as? Following ?: return
        if (record.msgId != following.claimMsgId) return

        // Cleared whichever way it went, or a node that lost would never claim again.
        following.claimMsgId = null

        if (conferring) cutOverToLeader(following, record.message.termId)
        else LOG.debug("[$dbName] claim at ${record.msgId} conferred nothing — still following")
    }

    private fun openFollower(pendingBlock: PendingBlock? = null): Following {
        LOG.info {
            buildString {
                append("[$dbName] starting follower: ")
                append("pending block: ${pendingBlock != null}, ")
                append("src: ${watchers.latestSourceMsgId}, ")
                append("replica: $latestReplicaMsgId")
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

    // Volatile: the replica reader is the sole writer, but the `xtdb.log.leader` gauge reads it from the metrics thread.
    @Volatile
    private var state: State = openFollower()

    /** A term that has ended stays in [state] until the reader's next poll, and leads nothing meanwhile. */
    val isLeader get() = state.let { it is Leading && !it.job.isCompleted }

    // Held here rather than on the term's cutter, because a Micrometer gauge keeps the state object it was
    // registered with: a per-term one would be dropped and the gauge would go on reporting the first
    // term's last upload, which is the one thing it exists to contradict (#5867).
    private val lastBlockUploadEpochSeconds = AtomicLong(0)

    init {
        base.meterRegistry?.let { reg ->
            Gauge.builder("xtdb.log.leader", this) { if (it.isLeader) 1.0 else 0.0 }
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

    /** Runs inline on the reader, so a rival's claim cannot land between reading our own back and leading. */
    private suspend fun cutOverToLeader(following: Following, termId: Long) {
        LOG.info("[$dbName] claim at term ${LeaderTerm.format(termId)} conferred leadership")

        var pendingBlock: PendingBlock? = null

        // Reaching the catch below *is* "the follower was stopped", so every exit re-opens one — no flag guards it.
        try {
            // Arrow won't close a parent allocator while a child buffer is live, so a cancellation inside the join would leave the follower's allocator unclosable.
            // Bounded: these only unwind.
            withContext(NonCancellable) {
                following.job.cancelAndJoin()
                following.proc.close()

                // After the join: until then the follower may still apply the upload that closes this block, and taking it meanwhile finishes the same block twice.
                pendingBlock = following.proc.pendingBlock
            }

            val replicaAppender = ReplicaLogAppender(logsDriver, termId, electionDriver)

            val blockCutter =
                BlockCutter(
                    partitionStorage, partitionState, dbName, termId, replicaAppender, logsDriver,
                    compactor, dbCatalog, base.meterRegistry, lastBlockUploadEpochSeconds, scope,
                    ioDispatcher
                )

            // Closed on the way out because it is not in `state` yet, so nothing else can reach it to close it.
            // Its resolver holds a child allocator that would otherwise refuse the database's own close for the rest of the node's life.
            val proc = LeaderLogProcessor(
                allocator, base, partitionStorage, crashLogger, partitionState, dbName, logsDriver,
                blockCutter, watchers,
                replicaAppender, termFence,
                externalSource,
                skipTxs, dbCatalog,
                leaderTerm = termId,
                flushTimeout = flushTimeout,
                ioDispatcher = ioDispatcher,
            ).closeOnCatch { proc ->
                pendingBlock?.let { pending ->
                    LOG.debug("[${dbName}] transition: producing pending block b${pending.blockIdx} with ${pending.bufferedRecords.size} held records")

                    // Closed only once the term reads this upload back, so a failure in between hands the block to a re-opened follower that closes it on the same message.
                    blockCutter.upload(pending)
                }
                proc
            }

            val resumeAfterMsgId = watchers.latestSourceMsgId

            // Unbuffered: the reader waits on each record's own handle, so a buffer would only let it read ahead of a term about to end.
            val replicaMsgs = Channel<ReplicaApply>()

            val termJob = scope.launch(CoroutineName("$dbName-term")) {
                proc.runTerm(replicaMsgs, resumeAfterMsgId)
            }

            state = Leading(proc, roleScope(termJob), replicaMsgs)

            LOG.info("[$dbName] leader startup complete, resuming after $resumeAfterMsgId")
        } catch (e: Throwable) {
            state = openFollower(pendingBlock)
            if (e.isShutdownSignal) throw e

            // Reported, not rethrown: unwinding the reader would leave the follower just re-opened reading nothing.
            if (e is LeaderSupersededException) LOG.info("[$dbName] promotion superseded: ${e.message}")
            else LOG.error(e) { "[$dbName] promotion failed — still following" }
        }
    }

    override fun close() = state.close()

    /**
     * Run one cycle of every garbage collector owned by the leader (block + trie) and wait for
     * both. No-op unless leading — GC only runs on the leader. Bypasses the collectors'
     * `enabled` flag (which gates the auto-signal from the block-boundary path, not direct calls).
     */
    fun awaitNoGarbageBlocking() = (state as? Leading)?.awaitNoGarbageBlocking()
}
