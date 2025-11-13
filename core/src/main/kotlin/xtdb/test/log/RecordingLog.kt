package xtdb.test.log

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient
import xtdb.api.log.Log
import xtdb.api.log.LogClusterAlias
import xtdb.api.log.LogOffset
import xtdb.database.proto.DatabaseConfig
import java.time.Instant
import java.time.InstantSource
import java.time.temporal.ChronoUnit
import java.util.concurrent.CompletableFuture

class RecordingLog(private val instantSource: InstantSource, ) : Log {
    override val epoch = 0
    val messages = mutableListOf<Log.Message>()

    @SerialName("!Recording")
    @Serializable
    data class Factory(
        @Transient var instantSource: InstantSource = InstantSource.system()
    ) : Log.Factory {
        fun instantSource(instantSource: InstantSource) = apply { this.instantSource = instantSource }

        override fun openLog(clusters: Map<LogClusterAlias, Log.Cluster>) = RecordingLog(instantSource)

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

    override fun subscribe(
        subscriber: Log.Subscriber,
        latestProcessedOffset: LogOffset
    ): Log.Subscription = error("subscribe")

    override fun close() = Unit
}