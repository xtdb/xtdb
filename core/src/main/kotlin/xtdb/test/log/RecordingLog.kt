package xtdb.test.log

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient
import xtdb.api.log.Log
import xtdb.api.log.LogClusterAlias
import xtdb.api.log.LogOffset
import xtdb.api.log.ReadOnlyLog
import xtdb.database.proto.DatabaseConfig
import java.time.Instant
import java.time.InstantSource
import java.time.temporal.ChronoUnit
import java.util.concurrent.CompletableFuture

class RecordingLog(private val instantSource: InstantSource, messages: List<Log.Message>) : Log {
    override val epoch = 0
    val messages = messages.toMutableList()

    @SerialName("!Recording")
    @Serializable
    data class Factory(
        @Transient var instantSource: InstantSource = InstantSource.system()
    ) : Log.Factory {
        var messages: List<Log.Message> = emptyList()
        fun instantSource(instantSource: InstantSource) = apply { this.instantSource = instantSource }
        fun messages(messages: List<Log.Message>) = apply { this.messages = messages }

        override fun openLog(clusters: Map<LogClusterAlias, Log.Cluster>) = RecordingLog(instantSource, messages)

        override fun openReadOnlyLog(clusters: Map<LogClusterAlias, Log.Cluster>) =
            ReadOnlyLog(openLog(clusters))

        override fun writeTo(dbConfig: DatabaseConfig.Builder) = Unit
    }

    @Volatile
    override var latestSubmittedOffset: LogOffset = -1
        private set

    override fun appendMessage(message: Log.Message): CompletableFuture<Log.MessageMetadata> {
        messages.add(message)

        // See comment in InMemoryLog
        val ts = if (message is Log.Message.Tx) instantSource.instant() else Instant.now()

        return CompletableFuture.completedFuture(
            Log.MessageMetadata(
                ++latestSubmittedOffset,
                ts.truncatedTo(ChronoUnit.MICROS)
            )
        )
    }

    override fun readLastMessage(): Log.Message? = messages.lastOrNull()

    override fun openAtomicProducer(transactionalId: String) = object : Log.AtomicProducer {
        override fun openTx() = object : Log.AtomicProducer.Tx {
            private val buffer = mutableListOf<Pair<Log.Message, CompletableFuture<Log.MessageMetadata>>>()
            private var isOpen = true

            override fun appendMessage(message: Log.Message): CompletableFuture<Log.MessageMetadata> {
                check(isOpen) { "Transaction already closed" }
                val future = CompletableFuture<Log.MessageMetadata>()
                buffer.add(message to future)
                return future
            }

            override fun commit() {
                check(isOpen) { "Transaction already closed" }
                isOpen = false
                for ((message, future) in buffer) {
                    future.complete(this@RecordingLog.appendMessage(message).join())
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

    override fun subscribe(
        subscriber: Log.Subscriber,
        latestProcessedOffset: LogOffset
    ): Log.Subscription = error("subscribe")

    override fun close() = Unit
}
