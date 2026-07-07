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
import java.time.Instant
import java.time.InstantSource
import java.time.temporal.ChronoUnit.MICROS
import kotlin.time.Duration.Companion.minutes

class InMemoryLog<M>(
    private val instantSource: InstantSource,
    override val epoch: Int,
) : Log<M> {

    @SerialName("!InMemory")
    @Serializable
    data class Factory(
        @Transient var instantSource: InstantSource = InstantSource.system(),
        var epoch: Int = 0,
    ) : Log.Factory {
        fun instantSource(instantSource: InstantSource) = apply { this.instantSource = instantSource }
        fun epoch(epoch: Int) = apply { this.epoch = epoch }

        override fun openSourceLog(remotes: Map<RemoteAlias, Remote>) =
            InMemoryLog<SourceMessage>(instantSource, epoch)

        override fun openReadOnlySourceLog(remotes: Map<RemoteAlias, Remote>) =
            ReadOnlyLog(openSourceLog(remotes))

        override fun openReplicaLog(remotes: Map<RemoteAlias, Remote>) =
            InMemoryLog<ReplicaMessage>(instantSource, epoch)

        override fun openReadOnlyReplicaLog(remotes: Map<RemoteAlias, Remote>) =
            ReadOnlyLog(openReplicaLog(remotes))

        override fun writeTo(dbConfig: DatabaseConfig.Builder) {
            dbConfig.inMemoryLog = inMemoryLog { }
        }
    }

    private val committedCh = MutableSharedFlow<Record<M>>(replay = REPLAY_BUFFER_SIZE)

    companion object {
        private const val REPLAY_BUFFER_SIZE = 4096
    }

    private val mutex = Mutex()

    @Volatile
    override var latestSubmittedOffset: LogOffset = -1
        private set

    // Mutex ensures offset assignment + emission are atomic,
    // so subscribers always see records in offset order.
    // We only use the instantSource for Tx messages so that the tests
    // that check files can be deterministic.
    override suspend fun appendMessage(message: M): MessageMetadata = mutex.withLock {
        val ts = if (message is SourceMessage.Tx || message is SourceMessage.LegacyTx) instantSource.instant() else Instant.now()
        val record = Record(epoch, ++latestSubmittedOffset, ts.truncatedTo(MICROS), message)
        committedCh.emit(record)
        MessageMetadata(epoch, record.logOffset, ts.truncatedTo(MICROS))
    }

    override fun openAtomicProducer(transactionalId: String) = object : AtomicProducer<M> {
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
                        res.complete(this@InMemoryLog.appendMessage(message))
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

    override fun readLastMessage(): M? = null

    override fun readRecords(fromMsgId: MessageId, toMsgId: MessageId) = sequence {
        if (MsgIdUtil.msgIdToEpoch(fromMsgId) != epoch || MsgIdUtil.msgIdToEpoch(toMsgId) != epoch) return@sequence
        val fromOffset = msgIdToOffset(fromMsgId)
        val toOffset = msgIdToOffset(toMsgId)
        for (rec in committedCh.replayCache) {
            if (rec.logOffset >= toOffset) break
            if (rec.logOffset >= fromOffset) yield(rec)
        }
    }

    override suspend fun tailAll(afterMsgId: MessageId, processor: RecordProcessor<M>) = coroutineScope {
        var latestCompletedOffset = MsgIdUtil.afterMsgIdToOffset(epoch, afterMsgId)

        val ch = committedCh
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

    override suspend fun openGroupSubscription(listener: SubscriptionListener<M>) {
        listener.launchTransition(listOf(0)).await()
        val spec = listener.commitLeader(listOf(0))
        tailAll(spec.afterMsgId, spec.processor)
    }

    override fun close() {}
}
