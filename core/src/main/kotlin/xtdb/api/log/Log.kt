@file:UseSerializers(DurationSerde::class, PathSerde::class)

package xtdb.api.log

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Deferred
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
                    IN_MEMORY_LOG -> inMemoryLog
                    LOCAL_LOG -> localLog(config.localLog.path.asPath)
                    OTHER_LOG -> config.otherLog.let {
                        (otherLogs[it.typeUrl] ?: error("unknown log")).fromProto(it)
                    }

                    else -> error("invalid log: ${config.logCase}")
                }
        }
    }

    fun interface RecordProcessor<in M> {
        suspend fun processRecords(records: List<Record<M>>)
    }

    companion object {
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

    /**
     * @param transactionalId uniquely identifies this producer for Kafka's transaction coordinator.
     *   Must be stable across restarts for transaction recovery.
     */
    fun openAtomicProducer(transactionalId: String, partition: Int): AtomicProducer<M>

    interface AtomicProducer<M> : AutoCloseable {
        fun openTx(): Tx<M>

        interface Tx<M> : AutoCloseable {
            fun appendMessage(message: M): CompletableDeferred<MessageMetadata>
            fun commit()
            fun abort()
        }

        /** @suppress */
        companion object {
            inline fun <M, R> AtomicProducer<M>.withTx(block: (Tx<M>) -> R): R =
                openTx().use { tx ->
                    try {
                        block(tx).also { tx.commit() }
                    } catch (e: Throwable) {
                        try {
                            tx.abort()
                        } catch (abortEx: Throwable) {
                            e.addSuppressed(abortEx)
                        }
                        throw e
                    }
                }
        }
    }

    fun readLastMessage(partition: Int = 0): M?

    /**
     * Reads records in the range [fromMsgId, toMsgId) (start-inclusive, end-exclusive).
     * Returns a lazy sequence of decoded records in offset order.
     * If toMsgId exceeds the latest submitted offset, reads up to the latest available record.
     */
    fun readRecords(partition: Int, fromMsgId: MessageId, toMsgId: MessageId): Sequence<Record<M>>

    suspend fun tailAll(partition: Int, afterMsgId: MessageId, processor: RecordProcessor<M>)
    suspend fun openGroupSubscription(listener: SubscriptionListener<M>)

    /**
     * The transport's handle on one partition's leader election, driven as a three-step state
     * machine (follower → prepared → leading). See allium/log-processor-lifecycle.allium.
     *
     * [launchTransition] launches the follower→leader transition on the listener's *own* scope and
     * returns its [Deferred] — it builds the leader term but does NOT commit the role, so it runs off the
     * transport's thread (which joins/cancels the handle) and, because it runs under the listener's
     * scope, is torn down by that scope's cancellation on shutdown. [commitLeader] installs the prepared
     * leader and hands back the offset to resume from and the processor to feed; it (and [demoteLeader])
     * are the only committers, run at the transport's serialization point. This split keeps the
     * committed role single-writer while the unbounded catch-up runs off the poll thread.
     */
    interface SubscriptionListener<M> {
        fun launchTransition(partition: Int): Deferred<Unit>
        fun commitLeader(partition: Int): TailSpec<M>
        suspend fun demoteLeader(partition: Int)
    }

    data class TailSpec<M>(val afterMsgId: MessageId, val processor: RecordProcessor<M>)

    class Record<out M>(
        val epoch: Int,
        val logOffset: LogOffset,
        val logTimestamp: Instant,
        val message: M
    ) {
        val msgId: MessageId get() = offsetToMsgId(epoch, logOffset)
    }

}
