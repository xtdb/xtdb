package xtdb.api.log

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.future.future
import kotlinx.coroutines.selects.*
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient
import xtdb.api.log.Log.*
import xtdb.database.proto.DatabaseConfig
import xtdb.database.proto.DatabaseConfigKt
import xtdb.database.proto.databaseConfig
import xtdb.database.proto.inMemoryLog
import java.time.Instant
import java.time.InstantSource
import java.time.temporal.ChronoUnit.MICROS
import java.util.concurrent.CompletableFuture
import kotlin.coroutines.CoroutineContext
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

class InMemoryLog(
    private val instantSource: InstantSource,
    override val epoch: Int,
    coroutineContext: CoroutineContext = Dispatchers.Default
) : Log {
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

        override fun openLog(clusters: Map<LogClusterAlias, Cluster>) =
            InMemoryLog(instantSource, epoch, coroutineContext)

        override fun openReadOnlyLog(clusters: Map<LogClusterAlias, Cluster>) =
            ReadOnlyLog(openLog(clusters))

        override fun writeTo(dbConfig: DatabaseConfig.Builder) {
            dbConfig.inMemoryLog = inMemoryLog {  }
        }
    }
    private val subscriberScope = scope + SupervisorJob(scope.coroutineContext.job)

    internal data class NewMessage(
        val message: Message,
        val onCommit: CompletableDeferred<MessageMetadata>
    )

    private val appendCh: Channel<NewMessage> = Channel(100)
    private val committedCh = appendCh.receiveAsFlow()
        .map { (message, onCommit) ->
            // we only use the instantSource for Tx messages so that the tests
            // that check files can be deterministic
            val ts = if (message is Message.Tx) instantSource.instant() else Instant.now()

            val record = Record(++latestSubmittedOffset, ts.truncatedTo(MICROS), message)
            onCommit.complete(MessageMetadata(record.logOffset,ts.truncatedTo(MICROS)))
            record
        }
        .shareIn(scope, SharingStarted.Eagerly, 100)

    @Volatile
    override var latestSubmittedOffset: LogOffset = -1
        private set

    override fun appendMessage(message: Message) =
        scope.future {
            val res = CompletableDeferred<MessageMetadata>()
            appendCh.send(NewMessage(message, res))
            res.await()
        }

    override fun openAtomicProducer(transactionalId: String) = object : AtomicProducer {
        override fun openTx() = object : AtomicProducer.Tx {
            private val buffer = mutableListOf<Pair<Message, CompletableFuture<MessageMetadata>>>()
            private var isOpen = true

            override fun appendMessage(message: Message): CompletableFuture<MessageMetadata> {
                check(isOpen) { "Transaction already closed" }
                val future = CompletableFuture<MessageMetadata>()
                buffer.add(message to future)
                return future
            }

            override fun commit() {
                check(isOpen) { "Transaction already closed" }
                isOpen = false
                for ((message, future) in buffer) {
                    future.complete(this@InMemoryLog.appendMessage(message).join())
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

    override fun readLastMessage(): Message? = null

    override fun tailAll(subscriber: Subscriber, latestProcessedOffset: LogOffset): Subscription {
        val job = subscriberScope.launch {
            var latestCompletedOffset = latestProcessedOffset
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
                val records: List<Record> = select {
                    ch.onReceiveCatching { if (it.isClosed) null else listOf(it.getOrThrow()) }

                    @OptIn(ExperimentalCoroutinesApi::class)
                    onTimeout(1.minutes) { emptyList() }
                } ?: break
                runInterruptible { subscriber.processRecords(records) }
            }
        }

        return Subscription { runBlocking { withTimeout(5.seconds) { job.cancelAndJoin() } } }
    }

    override fun subscribe(subscriber: Subscriber, listener: AssignmentListener): Subscription {
        val offsets = listener.onPartitionsAssigned(listOf(0))
        val nextOffset = offsets[0] ?: 0L
        val subscription = tailAll(subscriber, nextOffset - 1)
        return Subscription {
            subscription.close()
            listener.onPartitionsRevoked(listOf(0))
        }
    }

    override fun close() {
        runBlocking { withTimeout(5.seconds) { scope.coroutineContext.job.cancelAndJoin() } }
    }
}
