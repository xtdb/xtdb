package xtdb.api.log

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.selects.*
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient
import xtdb.api.log.Log.*
import xtdb.database.proto.DatabaseConfig
import xtdb.util.MsgIdUtil
import xtdb.util.MsgIdUtil.msgIdToOffset
import xtdb.util.MsgIdUtil.offsetToMsgId
import xtdb.database.proto.inMemoryLog
import java.time.Instant
import java.time.InstantSource
import java.time.temporal.ChronoUnit.MICROS
import kotlin.coroutines.CoroutineContext
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

class InMemoryLog<M> @JvmOverloads constructor(
    private val instantSource: InstantSource,
    override val epoch: Int,
    coroutineContext: CoroutineContext = Dispatchers.Default
) : Log<M> {
    private val scope = CoroutineScope(coroutineContext)

    @SerialName("!InMemory")
    @Serializable
    data class Factory(
        @Transient var instantSource: InstantSource = InstantSource.system(),
        var epoch: Int = 0,
        @Transient var coroutineContext: CoroutineContext = Dispatchers.Default
    ) : Log.Factory {
        fun instantSource(instantSource: InstantSource) = apply { this.instantSource = instantSource }
        fun epoch(epoch: Int) = apply { this.epoch = epoch }
        fun coroutineContext(coroutineContext: CoroutineContext) = apply { this.coroutineContext = coroutineContext }

        override fun openSourceLog(clusters: Map<LogClusterAlias, Cluster>) =
            InMemoryLog<SourceMessage>(instantSource, epoch, coroutineContext)

        override fun openReadOnlySourceLog(clusters: Map<LogClusterAlias, Cluster>) =
            ReadOnlyLog(openSourceLog(clusters))

        override fun openReplicaLog(clusters: Map<LogClusterAlias, Cluster>) =
            InMemoryLog<ReplicaMessage>(instantSource, epoch, coroutineContext)

        override fun openReadOnlyReplicaLog(clusters: Map<LogClusterAlias, Cluster>) =
            ReadOnlyLog(openReplicaLog(clusters))

        override fun writeTo(dbConfig: DatabaseConfig.Builder) {
            dbConfig.inMemoryLog = inMemoryLog { }
        }
    }

    internal data class NewMessage<M>(
        val message: M,
        val onCommit: CompletableDeferred<MessageMetadata>
    )

    private val appendCh: Channel<NewMessage<M>> = Channel(100)
    private val committedCh = appendCh.receiveAsFlow()
        .map { (message, onCommit) ->
            // we only use the instantSource for Tx messages so that the tests
            // that check files can be deterministic
            val ts = if (message is SourceMessage.Tx || message is SourceMessage.LegacyTx) instantSource.instant() else Instant.now()

            val record = Record(epoch, ++latestSubmittedOffset, ts.truncatedTo(MICROS), message)
            onCommit.complete(MessageMetadata(epoch, record.logOffset, ts.truncatedTo(MICROS)))
            record
        }
        .shareIn(scope, SharingStarted.Eagerly, REPLAY_BUFFER_SIZE)

    companion object {
        private const val REPLAY_BUFFER_SIZE = 4096
    }

    @Volatile
    override var latestSubmittedOffset: LogOffset = -1
        private set

    override suspend fun appendMessage(message: M): MessageMetadata =
        CompletableDeferred<MessageMetadata>()
            .also { scope.launch { appendCh.send(NewMessage(message, it)) } }
            .await()

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
        val spec = listener.onPartitionsAssignedSync(listOf(0))
        if (spec != null) {
            try {
                tailAll(spec.afterMsgId, spec.processor)
            } finally {
                listener.onPartitionsRevokedSync(listOf(0))
            }
        }
    }

    override fun close() {
        runBlocking { withTimeout(5.seconds) { scope.coroutineContext.job.cancelAndJoin() } }
    }
}
