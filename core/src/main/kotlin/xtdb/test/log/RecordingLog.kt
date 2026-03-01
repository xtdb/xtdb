package xtdb.test.log

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient
import xtdb.api.log.Log
import xtdb.api.log.LogClusterAlias
import xtdb.api.log.SourceMessage
import xtdb.api.log.ReplicaMessage
import xtdb.api.log.LogOffset
import xtdb.api.log.ReadOnlyLog
import xtdb.database.proto.DatabaseConfig
import java.time.Instant
import java.time.InstantSource
import java.time.temporal.ChronoUnit
import java.util.concurrent.CompletableFuture

class RecordingLog<M>(private val instantSource: InstantSource, messages: List<M>) : Log<M> {
    override val epoch = 0
    val messages = messages.toMutableList()

    @SerialName("!Recording")
    @Serializable
    data class Factory(
        @Transient var instantSource: InstantSource = InstantSource.system()
    ) : Log.Factory {
        var messages: List<SourceMessage> = emptyList()
        fun instantSource(instantSource: InstantSource) = apply { this.instantSource = instantSource }
        fun messages(messages: List<SourceMessage>) = apply { this.messages = messages }

        override fun openSourceLog(clusters: Map<LogClusterAlias, Log.Cluster>) = RecordingLog(instantSource, messages)

        override fun openReadOnlySourceLog(clusters: Map<LogClusterAlias, Log.Cluster>) =
            ReadOnlyLog(openSourceLog(clusters))

        override fun openReplicaLog(clusters: Map<LogClusterAlias, Log.Cluster>) =
            RecordingLog<ReplicaMessage>(instantSource, emptyList())

        override fun writeTo(dbConfig: DatabaseConfig.Builder) = Unit
    }

    @Volatile
    override var latestSubmittedOffset: LogOffset = -1
        private set

    override fun appendMessage(message: M): CompletableFuture<Log.MessageMetadata> {
        messages.add(message)

        val ts = if (message is SourceMessage.Tx) instantSource.instant() else Instant.now()

        return CompletableFuture.completedFuture(
            Log.MessageMetadata(
                ++latestSubmittedOffset,
                ts.truncatedTo(ChronoUnit.MICROS)
            )
        )
    }

    override fun readLastMessage(): M? = messages.lastOrNull()

    override fun openAtomicProducer(transactionalId: String) = object : Log.AtomicProducer<M> {
        override fun openTx() = object : Log.AtomicProducer.Tx<M> {
            private val buffer = mutableListOf<Pair<M, CompletableFuture<Log.MessageMetadata>>>()
            private var isOpen = true

            override fun appendMessage(message: M): CompletableFuture<Log.MessageMetadata> {
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

    override fun openConsumer(): Log.Consumer<M> = error("openConsumer")

    override fun openGroupConsumer(listener: Log.SubscriptionListener): Log.Consumer<M> = error("openGroupConsumer")

    override fun close() = Unit
}
