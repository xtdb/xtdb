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
import java.time.Instant
import java.time.InstantSource
import java.time.temporal.ChronoUnit.MICROS
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

class InMemoryLog(private val instantSource: InstantSource, override val epoch: Int) : Log {

    @SerialName("!InMemory")
    @Serializable
    data class Factory(
        @Transient var instantSource: InstantSource = InstantSource.system(),
        var epoch: Int = 0
    ) : Log.Factory {
        fun instantSource(instantSource: InstantSource) = apply { this.instantSource = instantSource }
        fun epoch(epoch: Int) = apply { this.epoch = epoch }

        override fun openLog() = InMemoryLog(instantSource, epoch)
    }

    private val scope: CoroutineScope = CoroutineScope(Dispatchers.Default)
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

    override fun subscribe(subscriber: Subscriber, latestProcessedOffset: LogOffset): Subscription {
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

    override fun close() {
        runBlocking { withTimeout(5.seconds) { scope.coroutineContext.job.cancelAndJoin() } }
    }
}
