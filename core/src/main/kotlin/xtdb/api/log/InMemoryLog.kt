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

    private val subscriberScope = scope + SupervisorJob(scope.coroutineContext.job)

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
        .shareIn(scope, SharingStarted.Eagerly, 100)

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

    override fun tailAll(afterMsgId: MessageId, processor: RecordProcessor<M>): Subscription {
        var latestCompletedOffset = MsgIdUtil.afterMsgIdToOffset(epoch, afterMsgId)

        val job = subscriberScope.launch {
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

        return Subscription { runBlocking { withTimeout(5.seconds) { job.cancelAndJoin() } } }
    }

    override fun openGroupSubscription(listener: SubscriptionListener<M>): Subscription {
        val spec = listener.onPartitionsAssignedSync(listOf(0))
        val sub = spec?.let { tailAll(it.afterMsgId, it.processor) }
        return Subscription {
            sub?.close()
            listener.onPartitionsRevokedSync(listOf(0))
        }
    }

    override fun close() {
        runBlocking { withTimeout(5.seconds) { scope.coroutineContext.job.cancelAndJoin() } }
    }
}
