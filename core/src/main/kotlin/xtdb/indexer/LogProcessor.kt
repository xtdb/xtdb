package xtdb.indexer

import io.micrometer.core.instrument.Gauge
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
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
                        val replayTarget =
                            replicaProducer.withTx { it.appendMessage(ReplicaMessage.NoOp()) }.await().msgId

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

                            val leaderSys = LeaderSystem(openLeader(replicaProducer, replayTarget))
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
