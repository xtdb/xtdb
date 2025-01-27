package xtdb.api.log

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.future.future
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient
import xtdb.api.log.Log.Processor
import xtdb.api.log.Log.Record
import java.time.InstantSource
import java.time.temporal.ChronoUnit.MICROS
import kotlin.time.Duration.Companion.minutes

class InMemoryLog(
    private val msgProcessor: Processor?,
    private val instantSource: InstantSource
) : Log {

    @SerialName("!InMemory")
    @Serializable
    data class Factory(@Transient var instantSource: InstantSource = InstantSource.system()) : Log.Factory {
        fun instantSource(instantSource: InstantSource) = apply { this.instantSource = instantSource }

        override fun openLog(msgProcessor: Processor?): InMemoryLog {
            check(msgProcessor == null || msgProcessor.latestCompletedOffset == -1L) {
                "InMemory log must start with an empty log"
            }
            return InMemoryLog(msgProcessor, instantSource)
        }
    }

    @Volatile
    private var nextOffset: LogOffset = 0

    private val scope: CoroutineScope = CoroutineScope(Dispatchers.Default)

    internal data class NewMessage(
        val message: Log.Message,
        val onCommit: CompletableDeferred<LogOffset>
    )

    private val appendCh: Channel<NewMessage> = Channel(100)
    private val committedCh: Channel<Record> = Channel(100)

    override val latestSubmittedOffset get() = nextOffset - 1

    init {
        scope.launch {
            for ((message, onCommit) in appendCh) {
                val record = Record(nextOffset++, instantSource.instant().truncatedTo(MICROS), message)
                onCommit.complete(record.logOffset)
                committedCh.send(record)
            }
        }

        scope.launch {
            while (true) {
                val msg = withTimeoutOrNull(1.minutes) { committedCh.receive() }
                runInterruptible { msgProcessor?.processRecords(this@InMemoryLog, listOfNotNull(msg)) }
            }
        }
    }

    override fun appendMessage(message: Log.Message) =
        scope.future {
            val res = CompletableDeferred<LogOffset>()
            appendCh.send(NewMessage(message, res))
            res.await()
        }


    override fun close() {
        runBlocking { scope.coroutineContext.job.cancelAndJoin() }
    }
}