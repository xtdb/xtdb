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
import kotlin.time.Duration.Companion.minutes

class InMemoryLog<M> @JvmOverloads constructor(
    private val instantSource: InstantSource,
    override val epoch: Int,
    val partitions: Int = 1,
) : Log<M> {

    @SerialName("!InMemory")
    @Serializable
    data class Factory(
        @Transient var instantSource: InstantSource = InstantSource.system(),
        var epoch: Int = 0,
    ) : Log.Factory {
        fun instantSource(instantSource: InstantSource) = apply { this.instantSource = instantSource }
        fun epoch(epoch: Int) = apply { this.epoch = epoch }

        override fun openSourceLog(remotes: Map<RemoteAlias, Remote>, partitions: Int) =
            InMemoryLog<SourceMessage>(instantSource, epoch, partitions)

        override fun openReadOnlySourceLog(remotes: Map<RemoteAlias, Remote>, partitions: Int) =
            ReadOnlyLog(openSourceLog(remotes, partitions))

        override fun openReplicaLog(remotes: Map<RemoteAlias, Remote>, partitions: Int) =
            InMemoryLog<ReplicaMessage>(instantSource, epoch, partitions)

        override fun openReadOnlyReplicaLog(remotes: Map<RemoteAlias, Remote>, partitions: Int) =
            ReadOnlyLog(openReplicaLog(remotes, partitions))

        override fun writeTo(dbConfig: DatabaseConfig.Builder) {
            dbConfig.inMemoryLog = inMemoryLog { }
        }
    }

    companion object {
        private const val REPLAY_BUFFER_SIZE = 4096
    }

    // A msgId embeds the epoch but not the partition — partition is implicit in *which partition it
    // came from*.
    private inner class PartitionState {
        val committedCh = MutableSharedFlow<Record<M>>(replay = REPLAY_BUFFER_SIZE)
        val mutex = Mutex()

        @Volatile
        var latestSubmittedOffset: LogOffset = -1
    }

    private val partitionStates = List(partitions) { PartitionState() }

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

    override fun openAtomicProducer(transactionalId: String, partition: Int) = object : AtomicProducer<M> {
        override fun openTx() = object : AtomicProducer.Tx<M> {
            private val buffer = mutableListOf<Pair<M, CompletableDeferred<MessageMetadata>>>()
            private var isOpen = true

            override fun appendMessage(message: M): CompletableDeferred<MessageMetadata> {
                check(isOpen) { "Transaction already closed" }
                return CompletableDeferred<MessageMetadata>()
                    .also { buffer.add(message to it) }
            }

            override fun commit() {
                check(isOpen) { "Transaction already closed" }
                isOpen = false
                runBlocking {
                    for ((message, res) in buffer) {
                        res.complete(this@InMemoryLog.appendMessage(message, partition))
                    }
                }
            }

            override fun abort() {
                check(isOpen) { "Transaction already closed" }
                isOpen = false
                buffer.clear()
            }

            override fun close() {
                if (isOpen) abort()
            }
        }

        override fun close() {}
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
            val records = select {
                ch.onReceiveCatching { if (it.isClosed) emptyList() else listOf(it.getOrThrow()) }

                @OptIn(ExperimentalCoroutinesApi::class)
                onTimeout(1.minutes) { emptyList() }
            }

            processor.processRecords(records)
        }
    }

    // No rebalance to simulate — one launch per partition, each running the full state machine.
    override suspend fun openGroupSubscription(listener: SubscriptionListener<M>) = coroutineScope {
        for (p in 0 until partitions) {
            launch {
                try {
                    listener.launchTransition(p).await()
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
