package xtdb.indexer

import io.micrometer.core.instrument.Gauge
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.async
import kotlinx.coroutines.withContext
import org.apache.arrow.memory.BufferAllocator
import xtdb.NodeBase
import xtdb.api.log.*
import xtdb.api.log.Log.AtomicProducer.Companion.withTx
import xtdb.compactor.Compactor
import xtdb.database.Database
import xtdb.database.DatabaseState
import xtdb.database.DatabaseStorage
import xtdb.api.tx.ExternalSource
import xtdb.api.error.Fault
import xtdb.api.error.Interrupted
import xtdb.types.MessageId
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

    // `close` MUST follow a returned `cancelAndJoin`. See dev/doc/coroutines.adoc.
    private sealed interface SubSystem : AutoCloseable {
        suspend fun cancelAndJoin()
    }

    private class LeaderSystem(val proc: LeaderLogProcessor) : SubSystem {
        override suspend fun cancelAndJoin() = proc.cancelAndJoin()
        override fun close() = proc.close()
    }

    private class FollowerSystem(val proc: FollowerLogProcessor) : SubSystem {
        override suspend fun cancelAndJoin() = proc.cancelAndJoin()
        override fun close() = proc.close()
    }

    // The committed-role state machine — see allium/log-processor-lifecycle.allium.
    // `state` is written from two places: the transport's serialization point (commitLeader /
    // demoteLeader) and the off-thread transition coroutine (cutoverToLeader → Prepared, or its catch
    // → Following). They don't race: a revoke cancel-and-joins the transition before demoteLeader reads
    // `state`, and the transport commits a leader only for the transition instance it still holds — so the
    // *committed* role change still happens only at the serialization point.
    private sealed interface State {
        val system: SubSystem
    }

    private class Following(override val system: FollowerSystem) : State
    private class Prepared(override val system: LeaderSystem, val resumeAfterMsgId: MessageId) : State
    private class Leading(override val system: LeaderSystem) : State

    private fun openLeader(
        replicaProducer: Log.AtomicProducer<ReplicaMessage>,
        afterReplicaMsgId: MessageId,
    ): LeaderLogProcessor =
        // The leader term owns (and frees) its replica producer and ext source.
        LeaderLogProcessor(
            allocator, base, dbStorage, crashLogger, dbState, blockUploader,
            watchers,
            externalSourceFactory?.open(dbName, base.remotes, base.meterRegistry),
            replicaProducer, skipTxs, dbCatalog,
            partition = 0, afterReplicaMsgId,
            flushTimeout = flushTimeout,
            scope = scope,
            gcDispatcher = gcDispatcher,
        )

    private fun openTransition(
        replicaProducer: Log.AtomicProducer<ReplicaMessage>,
        afterReplicaMsgId: MessageId,
    ): TransitionLogProcessor =
        TransitionLogProcessor(
            allocator, dbStorage.bufferPool, dbState, dbState.liveIndex,
            blockUploader, replicaProducer, watchers, dbCatalog,
            afterReplicaMsgId,
            hasExternalSource = hasExternalSource,
        )

    private fun openFollower(
        pendingBlock: PendingBlock?,
        afterReplicaMsgId: MessageId,
    ): FollowerLogProcessor =
        FollowerLogProcessor(
            allocator, dbStorage.replicaLog, dbStorage.bufferPool, dbState, compactor, watchers,
            dbCatalog, pendingBlock, afterReplicaMsgId, scope,
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

        return FollowerSystem(openFollower(pendingBlock, latestReplicaMsgId))
    }

    @Volatile
    private var state: State =
        Following(openFollowerSystem(dbState.blockCatalog.boundaryReplicaMsgId ?: -1))

    init {
        base.meterRegistry?.let { reg ->
            Gauge.builder("xtdb.log.leader", this) { if (it.state is Leading) 1.0 else 0.0 }
                .description("1 if this node is the log leader, 0 if follower")
                .tag("db", dbState.name)
                .register(reg)
        }
    }

    override fun launchTransition(partition: Int): Deferred<Unit> {
        // Transport contract: transition only from Following (see SubscriptionListener). A raw cast
        // would surface an out-of-order call as a cryptic ClassCastException; name it instead.
        val followerSys = (state as? Following)?.system
            ?: throw Fault("[$dbName] launchTransition while not following (${state::class.simpleName})", "xtdb/log-prepare-not-following")

        // Launched on the database scope (not the caller's): the transition is a child of the db job
        // tree, so the transport joins/cancels this handle while db teardown cancels-and-joins it
        // before close(). See dev/doc/coroutines.adoc and allium/log-processor-lifecycle.allium.
        return scope.async { runTransition(followerSys) }
    }

    private suspend fun runTransition(followerSys: FollowerSystem) {
        try {
            replicaLog.openAtomicProducer("$dbName-leader", partition = 0).closeOnCatch { replicaProducer ->
                val followerProc = followerSys.proc

                // NoOp to get a known msgId we can await — latestSubmittedMsgId won't do, because Kafka's
                // endOffsets counts transaction-marker offsets that consumers never deliver.
                val replayTarget = replicaProducer.withTx { it.appendMessage(ReplicaMessage.NoOp()) }.await().msgId

                // The only step needing the follower alive, and the only unbounded one: as the sole
                // replica-log consumer it must drain up to the leadership-claim point before we stop it.
                // A failure here leaves the follower untouched — `state` still holds a live term, nothing
                // to recover — which is why cutover, the phase that re-follows on exit, only starts here.
                followerProc.awaitReplicaMsgId(replayTarget)

                cutoverToLeader(followerSys, replicaProducer, replayTarget)
            }
        } catch (e: Throwable) {
            // Cutover already restored a live `state` if it had to; here we only report. Cancellation and
            // interruption aren't leader-prep failures, so they don't poison watchers — only a genuine one.
            if (e !is CancellationException && e !is InterruptedException && e !is Interrupted) {
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
    private suspend fun cutoverToLeader(
        followerSys: FollowerSystem,
        replicaProducer: Log.AtomicProducer<ReplicaMessage>,
        replayTarget: MessageId,
    ) {
        val followerProc = followerSys.proc
        val pendingBlock = followerProc.pendingBlock
        try {
            LOG.debug("[$dbName] transition: closing follower system")
            withContext(NonCancellable) {
                followerSys.cancelAndJoin()
                followerSys.close()
            }

            openTransition(replicaProducer, followerProc.latestReplicaMsgId).use { transition ->
                if (pendingBlock != null) {
                    LOG.debug("[$dbName] transition: finishing pending block b${pendingBlock.blockIdx} with ${pendingBlock.bufferedRecords.size} buffered records")
                    blockUploader.uploadBlock(
                        replicaProducer, pendingBlock.boundaryMsgId, pendingBlock.boundaryMessage,
                    )
                    LOG.debug("[$dbName] transition: replaying ${pendingBlock.bufferedRecords.size} buffered records through transition processor")
                    transition.processRecords(pendingBlock.bufferedRecords)
                }
            }

            LOG.debug("[$dbName] transition: building leader processor")
            val resumeAfterMsgId = watchers.latestSourceMsgId

            // Built, not committed: `state` moves to Prepared but no records flow as leader until
            // commitLeader installs it at the serialization point.
            state = Prepared(LeaderSystem(openLeader(replicaProducer, replayTarget)), resumeAfterMsgId)
        } catch (e: Throwable) {
            state = Following(openFollowerSystem(followerProc.latestReplicaMsgId, pendingBlock))
            throw e
        }
    }

    override fun commitLeader(partition: Int): Log.TailSpec<SourceMessage> {
        val prepared = (state as? Prepared)
            ?: throw Fault("[$dbName] commitLeader without a prepared leader (${state::class.simpleName})", "xtdb/log-commit-not-prepared")
        state = Leading(prepared.system)
        LOG.info("[$dbName] leader startup complete, resuming after ${prepared.resumeAfterMsgId}")
        return Log.TailSpec(prepared.resumeAfterMsgId, prepared.system.proc)
    }

    override suspend fun demoteLeader(partition: Int) {
        // Genuine revoke (Leading) and abandoning an uncommitted leader (Prepared) tear down the
        // same way; an already-following listener is a no-op.
        val leaderSys = when (val s = state) {
            is Following -> {
                LOG.debug("[$dbName] demote — already follower, no transition needed")
                return
            }

            is Prepared -> s.system
            is Leading -> s.system
        }

        LOG.info("[$dbName] demote — tearing down leader, re-opening follower")
        // Cancel first: the watermark fields stay readable after the cancel/close — they're not
        // allocator-backed — so we free the old term before reading them to seed the follower.
        leaderSys.cancelAndJoin()
        leaderSys.close()
        state = Following(openFollowerSystem(leaderSys.proc.latestReplicaMsgId, leaderSys.proc.pendingBlock))
    }

    override fun close() = state.system.close()

    /**
     * Run one cycle of every garbage collector owned by the leader (block + trie) and wait for
     * both. No-op unless leading — GC only runs on the leader. Bypasses the collectors'
     * `enabled` flag (which gates the auto-signal from the block-boundary path, not direct calls).
     */
    fun gcAll() {
        val proc = (state as? Leading)?.system?.proc ?: return
        proc.blockGc.awaitNoGarbageBlocking()
        proc.trieGc.awaitNoGarbageBlocking()
    }
}
