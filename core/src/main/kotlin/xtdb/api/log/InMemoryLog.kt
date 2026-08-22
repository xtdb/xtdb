package xtdb.api.log

import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.selects.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient
import xtdb.api.Remote
import xtdb.api.RemoteAlias
import xtdb.api.log.Log.*
import xtdb.database.proto.DatabaseConfig
import xtdb.util.MsgIdUtil
import xtdb.util.MsgIdUtil.msgIdToOffset
import xtdb.database.proto.inMemoryLog
import xtdb.types.LogOffset
import xtdb.types.MessageId
import java.time.Instant
import java.time.InstantSource
import java.time.temporal.ChronoUnit.MICROS
import java.time.Duration as JDuration
import kotlin.time.Duration.Companion.milliseconds

class InMemoryLog<M> @JvmOverloads constructor(
    private val instantSource: InstantSource,
    override val epoch: Int,
    private val termEpoch: Int = 0,
    val partitions: Int = 1,
) : Log<M> {

    @SerialName("!InMemory")
    @Serializable
    data class Factory(
        @Transient var instantSource: InstantSource = InstantSource.system(),
        var epoch: Int = 0,
        /**
         * Declares that the leader-election counter behind this log has been reset, so that terms
         * from before the reset still order below terms from after it. Bump it — never lower it —
         * whenever that happens; a node that finds its own term already fenced on the replica log
         * refuses to lead and names this setting. See [LeaderTerm].
         */
        var termEpoch: Int = 0,
    ) : Log.Factory {
        fun instantSource(instantSource: InstantSource) = apply { this.instantSource = instantSource }
        fun epoch(epoch: Int) = apply { this.epoch = epoch }
        fun termEpoch(termEpoch: Int) = apply { this.termEpoch = termEpoch }

        override fun openSourceLog(remotes: Map<RemoteAlias, Remote>, partitions: Int) =
            InMemoryLog<SourceMessage>(instantSource, epoch, termEpoch, partitions)

        override fun openReadOnlySourceLog(remotes: Map<RemoteAlias, Remote>, partitions: Int) =
            ReadOnlyLog(openSourceLog(remotes, partitions))

        override fun openReplicaLog(remotes: Map<RemoteAlias, Remote>, partitions: Int) =
            InMemoryLog<ReplicaMessage>(instantSource, epoch, termEpoch, partitions)

        override fun openReadOnlyReplicaLog(remotes: Map<RemoteAlias, Remote>, partitions: Int) =
            ReadOnlyLog(openReplicaLog(remotes, partitions))

        override fun writeTo(dbConfig: DatabaseConfig.Builder) {
            dbConfig.inMemoryLog = inMemoryLog {
                this.epoch = this@Factory.epoch
                this.termEpoch = this@Factory.termEpoch
            }
        }
    }

    companion object {
        private const val REPLAY_BUFFER_SIZE = 4096
    }

    // In-process, so exactly one participant exists: elections are uncontested and can run in
    // milliseconds, and there is no follower elsewhere for an idle leader to reassure.
    override val tailPollDuration = 10.milliseconds

    override val electionConfig =
        ElectionConfig(
            electionTimeoutMin = JDuration.ofMillis(50),
            electionTimeoutMax = JDuration.ofMillis(150),
            claimTimeout = JDuration.ofMillis(400),
            assertionInterval = null,
        )

    // A msgId embeds the epoch but not the partition — partition is implicit in *which partition it
    // came from*.
    private inner class PartitionState {
        val committedCh = MutableSharedFlow<Record<M>>(replay = REPLAY_BUFFER_SIZE)
        val mutex = Mutex()

        @Volatile
        var latestSubmittedOffset: LogOffset = -1
    }

    private val partitionStates = List(partitions) { PartitionState() }
    private val elections = java.util.concurrent.atomic.AtomicLong(0)

    private fun state(partition: Int): PartitionState =
        partitionStates.getOrNull(partition)
            ?: error("no such partition $partition (partitions=$partitions)")

    override fun latestSubmittedOffset(partition: Int): LogOffset = state(partition).latestSubmittedOffset

    // Mutex ensures offset assignment + emission are atomic per partition,
    // so subscribers always see records in offset order.
    override suspend fun appendMessage(message: M, partition: Int): MessageMetadata {
        val ps = state(partition)
        return ps.mutex.withLock {
            val ts = if (message is SourceMessage.Tx || message is SourceMessage.LegacyTx) instantSource.instant() else Instant.now()
            val record = Record(epoch, ++ps.latestSubmittedOffset, ts.truncatedTo(MICROS), message)
            ps.committedCh.emit(record)
            MessageMetadata(epoch, record.logOffset, ts.truncatedTo(MICROS))
        }
    }

    override fun readLastMessage(partition: Int): M? = null

    override fun readRecords(partition: Int, fromMsgId: MessageId, toMsgId: MessageId) = sequence {
        if (MsgIdUtil.msgIdToEpoch(fromMsgId) != epoch || MsgIdUtil.msgIdToEpoch(toMsgId) != epoch) return@sequence
        val fromOffset = msgIdToOffset(fromMsgId)
        val toOffset = msgIdToOffset(toMsgId)
        for (rec in state(partition).committedCh.replayCache) {
            if (rec.logOffset >= toOffset) break
            if (rec.logOffset >= fromOffset) yield(rec)
        }
    }

    override suspend fun tailAll(partition: Int, afterMsgId: MessageId, processor: RecordProcessor<M>) = coroutineScope {
        var latestCompletedOffset = MsgIdUtil.afterMsgIdToOffset(epoch, afterMsgId)

        val ch = state(partition).committedCh
            .filter {
                val logOffset = it.logOffset
                check(logOffset <= latestCompletedOffset + 1) {
                    "InMemoryLog emitted out-of-order record (expected ${latestCompletedOffset + 1}, got $logOffset)"
                }
                logOffset > latestCompletedOffset
            }
            .onEach { latestCompletedOffset = it.logOffset }
            .buffer(100)
            .produceIn(this)

        while (isActive) {
            // A closed channel is not a read that found nothing: reported as one it would tell the
            // reader its log was quiet, indefinitely and without ever suspending, from a tail that has
            // in fact stopped. Hence null for closed, and an empty list only for a genuine timeout.
            val records = select<List<Record<M>>?> {
                ch.onReceiveCatching { if (it.isClosed) null else listOf(it.getOrThrow()) }

                @OptIn(ExperimentalCoroutinesApi::class)
                onTimeout(tailPollDuration) { emptyList() }
            } ?: break

            processor.processRecords(records)
        }
    }

    // No rebalance to simulate — one launch per partition, each running the full state machine.
    override suspend fun openGroupSubscription(listener: SubscriptionListener<M>) = coroutineScope {
        for (p in 0 until partitions) {
            launch {
                try {
                    listener.launchTransition(p, LeaderTerm.of(termEpoch, elections.incrementAndGet())).await()
                    val spec = listener.commitLeader(p)
                    tailAll(p, spec.afterMsgId, spec.processor)
                } finally {
                    withContext(NonCancellable) { listener.demoteLeader(p) }
                }
            }
        }
    }

    override fun close() {}
}
