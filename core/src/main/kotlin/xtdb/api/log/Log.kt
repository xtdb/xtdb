@file:UseSerializers(DurationSerde::class, PathSerde::class)

package xtdb.api.log

import kotlinx.coroutines.runBlocking
import kotlinx.serialization.UseSerializers
import kotlinx.serialization.modules.PolymorphicModuleBuilder
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.polymorphic
import kotlinx.serialization.modules.subclass
import xtdb.DurationSerde
import xtdb.api.PathSerde
import xtdb.api.Remote
import xtdb.api.RemoteAlias
import xtdb.database.proto.DatabaseConfig
import xtdb.database.proto.DatabaseConfig.LogCase.*
import xtdb.types.LogOffset
import xtdb.types.LogTimestamp
import xtdb.types.MessageId
import xtdb.util.MsgIdUtil.offsetToMsgId
import xtdb.util.asPath
import java.nio.file.Path
import java.time.Instant
import java.util.*
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import com.google.protobuf.Any as ProtoAny


interface MessageCodec<M> {
    fun encode(message: M): ByteArray
    fun decode(bytes: ByteArray): M?
}

interface Log<M> : AutoCloseable {

    interface Factory {
        fun openSourceLog(remotes: Map<RemoteAlias, Remote>, partitions: Int = 1): Log<SourceMessage>
        fun openReadOnlySourceLog(remotes: Map<RemoteAlias, Remote>, partitions: Int = 1): Log<SourceMessage>
        fun openReplicaLog(remotes: Map<RemoteAlias, Remote>, partitions: Int = 1): Log<ReplicaMessage>
        fun openReadOnlyReplicaLog(remotes: Map<RemoteAlias, Remote>, partitions: Int = 1): Log<ReplicaMessage>

        fun writeTo(dbConfig: DatabaseConfig.Builder)

        companion object {
            private val otherLogs = ServiceLoader.load(Registration::class.java).associateBy { it.protoTag }

            /** @suppress */
            val serializersModule = SerializersModule {
                polymorphic(Factory::class) {
                    subclass(InMemoryLog.Factory::class)
                    subclass(LocalLog.Factory::class)

                    for (reg in ServiceLoader.load(Registration::class.java))
                        reg.registerSerde(this)
                }
            }

            internal fun fromProto(config: DatabaseConfig): Factory =
                when (config.logCase) {
                    IN_MEMORY_LOG -> config.inMemoryLog.let { inMemoryLog.epoch(it.epoch) }

                    LOCAL_LOG -> config.localLog.let { localLog(it.path.asPath).epoch(it.epoch) }

                    OTHER_LOG -> config.otherLog.let {
                        (otherLogs[it.typeUrl] ?: error("unknown log")).fromProto(it)
                    }

                    else -> error("invalid log: ${config.logCase}")
                }
        }
    }

    fun interface RecordProcessor<in M> {
        /**
         * Called once per read, with the records that read delivered — **including when it delivered
         * none**. An empty list says "the log was read, and had nothing beyond what you have already
         * seen", which is a distinct fact from not being called at all, and one that leader election
         * depends on: see [TAIL_POLL_DURATION].
         */
        suspend fun processRecords(records: List<Record<M>>)
    }

    /**
     * How long a tail waits for a record before reporting a read that found nothing.
     *
     * A participant measures quiet time across its own reads rather than against the clock, so a
     * silent log still has to be seen to have been read — otherwise a node that is no longer being
     * delivered to is indistinguishable from one whose database is merely idle, and the wrong one
     * of the two claims leadership. See `allium/log-processor-lifecycle.allium`.
     *
     * The interval must stay well inside the range this log's election timeouts are drawn from
     * ([electionConfig]). Two followers whose draws differ by less than one interval tip over on the
     * same read, and the randomisation that separates them buys nothing — which is why the in-process
     * logs, whose elections run in milliseconds, tick faster than the default.
     */
    val tailPollDuration: Duration get() = TAIL_POLL_DURATION

    /**
     * The election timeouts for leadership over this log.
     *
     * The log's to supply rather than the node's, because their right order of magnitude is a fact
     * about the log: an in-process log can elect in milliseconds, where a shared log's timeouts have
     * to absorb real scheduling and delivery delay.
     */
    val electionConfig: ElectionConfig get() = ElectionConfig()

    companion object {
        val TAIL_POLL_DURATION: Duration = 1.seconds

        @JvmStatic
        val inMemoryLog get() = InMemoryLog.Factory()

        @JvmStatic
        fun localLog(rootPath: Path) = LocalLog.Factory(rootPath)

        @Suppress("unused")
        @JvmSynthetic
        fun localLog(path: Path, configure: LocalLog.Factory.() -> Unit) = localLog(path).also(configure)
    }

    interface Registration {
        fun registerSerde(builder: PolymorphicModuleBuilder<Factory>)
        val protoTag: String
        fun fromProto(msg: ProtoAny): Factory
    }

    /*
     * We read this once from the existing log at startup,
     * so that if we're starting up a new node it catches up to the latest offset,
     * then it's the latest-submitted-offset of _this_ node.
     */
    fun latestSubmittedOffset(partition: Int = 0): LogOffset

    val epoch: Int

    fun latestSubmittedMsgId(partition: Int = 0): MessageId = offsetToMsgId(epoch, latestSubmittedOffset(partition))

    class MessageMetadata(
        val epoch: Int,
        val logOffset: LogOffset,
        val logTimestamp: LogTimestamp
    ) {
        val msgId: MessageId get() = offsetToMsgId(epoch, logOffset)
    }

    suspend fun appendMessage(message: M, partition: Int = 0): MessageMetadata

    fun appendMessageBlocking(message: M, partition: Int = 0): MessageMetadata =
        runBlocking { appendMessage(message, partition) }

    fun readLastMessage(partition: Int = 0): M?

    /**
     * Reads records in the range [fromMsgId, toMsgId) (start-inclusive, end-exclusive).
     * Returns a lazy sequence of decoded records in offset order.
     * If toMsgId exceeds the latest submitted offset, reads up to the latest available record.
     */
    fun readRecords(partition: Int, fromMsgId: MessageId, toMsgId: MessageId): Sequence<Record<M>>

    suspend fun tailAll(partition: Int, afterMsgId: MessageId, processor: RecordProcessor<M>)

    class Record<out M>(
        val epoch: Int,
        val logOffset: LogOffset,
        val logTimestamp: Instant,
        val message: M
    ) {
        val msgId: MessageId get() = offsetToMsgId(epoch, logOffset)
    }

}
