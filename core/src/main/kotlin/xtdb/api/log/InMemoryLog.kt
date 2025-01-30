package xtdb.api.log

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.future.future
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient
import xtdb.api.log.Log.*
import java.time.Instant
import java.time.InstantSource
import java.time.temporal.ChronoUnit.MICROS
import kotlin.time.Duration.Companion.minutes

class InMemoryLog(private val instantSource: InstantSource) : Log {

    @SerialName("!InMemory")
    @Serializable
    data class Factory(@Transient var instantSource: InstantSource = InstantSource.system()) : Log.Factory {
        fun instantSource(instantSource: InstantSource) = apply { this.instantSource = instantSource }

        override fun openLog() = InMemoryLog(instantSource)
    }

    @Volatile
    private var nextOffset: LogOffset = 0

    private val scope: CoroutineScope = CoroutineScope(Dispatchers.Default)

    internal data class NewMessage(
        val message: Message,
        val onCommit: CompletableDeferred<LogOffset>
    )

    private val appendCh: Channel<NewMessage> = Channel(100)

    private var committedCh: Channel<Record>? = null
    private val mutex = Mutex()

    override val latestSubmittedOffset get() = nextOffset - 1

    init {
        scope.launch {
            for ((message, onCommit) in appendCh) {
                mutex.withLock {
                    // we only use the instantSource for Tx messages so that the tests
                    // that check files can be deterministic
                    val ts = if (message is Message.Tx) instantSource.instant() else Instant.now()

                    val record = Record(nextOffset++, ts.truncatedTo(MICROS), message)
                    onCommit.complete(record.logOffset)
                    committedCh?.send(record)
                }
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
        val ch = runBlocking {
            mutex.withLock {
                val latestCompletedOffset = subscriber.latestCompletedOffset
                check(committedCh == null) { "InMemoryLog only supports one subscriber" }
                check(latestCompletedOffset == nextOffset - 1) {
                    "InMemoryLog doesn't support replay (log next: $nextOffset, sub completed: $latestCompletedOffset)"
                }

                Channel<Record>(100).also { committedCh = it }
            }
        }

        val job = scope.launch {
            while (true) {
                val msg = withTimeoutOrNull(1.minutes) { ch.receive() }
                runInterruptible { subscriber.processRecords(listOfNotNull(msg)) }
            }
        }

        return Subscription {
            runBlocking {
                mutex.withLock { committedCh = null }
                job.cancelAndJoin()
            }
        }
    }

    override fun close() {
        runBlocking { scope.coroutineContext.job.cancelAndJoin() }
    }
}