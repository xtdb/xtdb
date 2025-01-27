package xtdb.api.log

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.future.future
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient
import xtdb.api.log.Log.Record
import xtdb.api.log.Log.Subscriber
import xtdb.api.log.Log.Subscription
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
        val message: Log.Message,
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
                    val record = Record(nextOffset++, instantSource.instant().truncatedTo(MICROS), message)
                    onCommit.complete(record.logOffset)
                    committedCh?.send(record)
                }
            }
        }
    }

    override fun appendMessage(message: Log.Message) =
        scope.future {
            val res = CompletableDeferred<LogOffset>()
            appendCh.send(NewMessage(message, res))
            res.await()
        }

    override fun subscribe(subscriber: Subscriber): Subscription {
        check(subscriber.latestCompletedOffset < 0) { "InMemoryLog cannot re-subscribe with existing subscriber" }

        val ch = runBlocking {
            mutex.withLock {
                check(committedCh == null) { "InMemoryLog only supports one subscriber" }
                check(nextOffset == 0L) { "InMemoryLog doesn't support replay (nextOffset: $nextOffset)" }

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
                mutex.withLock {
                    job.cancelAndJoin()
                    committedCh = null
                }
            }
        }
    }

    override fun close() {
        runBlocking { scope.coroutineContext.job.cancelAndJoin() }
    }
}