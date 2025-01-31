package xtdb.api.log

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.future.future
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient
import xtdb.api.log.Log.*
import java.time.Instant
import java.time.InstantSource
import java.time.temporal.ChronoUnit.MICROS
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

class InMemoryLog(private val instantSource: InstantSource) : Log {

    @SerialName("!InMemory")
    @Serializable
    data class Factory(@Transient var instantSource: InstantSource = InstantSource.system()) : Log.Factory {
        fun instantSource(instantSource: InstantSource) = apply { this.instantSource = instantSource }

        override fun openLog() = InMemoryLog(instantSource)
    }

    private val scope: CoroutineScope = CoroutineScope(Dispatchers.Default)

    internal data class NewMessage(
        val message: Message,
        val onCommit: CompletableDeferred<LogOffset>
    )

    private val appendCh: Channel<NewMessage> = Channel(100)
    private val committedCh = MutableSharedFlow<Record>(extraBufferCapacity = 100)

    @Volatile
    override var latestSubmittedOffset: LogOffset = -1
        private set

    init {
        scope.launch {
            for ((message, onCommit) in appendCh) {
                // we only use the instantSource for Tx messages so that the tests
                // that check files can be deterministic
                val ts = if (message is Message.Tx) instantSource.instant() else Instant.now()

                val record = Record(++latestSubmittedOffset, ts.truncatedTo(MICROS), message)
                onCommit.complete(record.logOffset)
                committedCh.emit(record)
            }
        }
    }

    override fun appendMessage(message: Message) =
        scope.future {
            val res = CompletableDeferred<LogOffset>()
            appendCh.send(NewMessage(message, res))
            res.await()
        }

    override fun subscribe(subscriber: Subscriber): Subscription {
        val job = scope.launch(SupervisorJob()) {
            var latestCompletedOffset = subscriber.latestCompletedOffset

            val ch = Channel<Record>()

            committedCh
                .onEach {
                    val logOffset = it.logOffset
                    check(logOffset <= latestCompletedOffset + 1) {
                        "InMemoryLog emitted out-of-order record (expected ${latestCompletedOffset + 1}, got $logOffset)"
                    }
                    if (logOffset > latestCompletedOffset) {
                        latestCompletedOffset = logOffset
                        ch.send(it)
                    }
                }
                .onCompletion { ch.close() }
                .launchIn(this)

            while (true) {
                val msg = withTimeoutOrNull(1.minutes) { ch.receive() }
                runInterruptible { subscriber.processRecords(listOfNotNull(msg)) }
            }
        }

        return Subscription { runBlocking { withTimeout(5.seconds) { job.cancelAndJoin() } } }
    }

    override fun close() {
        runBlocking { withTimeout(5.seconds) { scope.coroutineContext.job.cancelAndJoin() } }
    }
}