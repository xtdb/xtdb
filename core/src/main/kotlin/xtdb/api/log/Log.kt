@file:UseSerializers(DurationSerde::class, PathWithEnvVarSerde::class)

package xtdb.api.log

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.UseSerializers
import kotlinx.serialization.modules.PolymorphicModuleBuilder
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.polymorphic
import kotlinx.serialization.modules.subclass
import xtdb.DurationSerde
import xtdb.api.PathWithEnvVarSerde
import xtdb.database.proto.DatabaseConfig
import xtdb.database.proto.DatabaseConfig.LogCase.*
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

    interface Cluster : AutoCloseable {

        interface Factory<C : Cluster> {
            fun open(): C

            companion object {
                val serializersModule = SerializersModule {
                    polymorphic(Factory::class) {
                        for (reg in ServiceLoader.load(Registration::class.java))
                            reg.registerSerde(this)
                    }
                }
            }
        }

        interface Registration {
            fun registerSerde(builder: PolymorphicModuleBuilder<Factory<*>>)
        }
    }

    interface Factory {
        fun openSourceLog(clusters: Map<LogClusterAlias, Cluster>): Log<SourceMessage>
        fun openReadOnlySourceLog(clusters: Map<LogClusterAlias, Cluster>): Log<SourceMessage>
        fun openReplicaLog(clusters: Map<LogClusterAlias, Cluster>): Log<ReplicaMessage>
        fun openReadOnlyReplicaLog(clusters: Map<LogClusterAlias, Cluster>): Log<ReplicaMessage>

        fun writeTo(dbConfig: DatabaseConfig.Builder)

        companion object {
            private val otherLogs = ServiceLoader.load(Registration::class.java).associateBy { it.protoTag }

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

    fun interface Subscription : AutoCloseable

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
    val latestSubmittedOffset: LogOffset

    val epoch: Int

    val latestSubmittedMsgId: MessageId
        get() = offsetToMsgId(epoch, latestSubmittedOffset)

    class MessageMetadata(
        val epoch: Int,
        val logOffset: LogOffset,
        val logTimestamp: LogTimestamp
    ) {
        val msgId: MessageId get() = offsetToMsgId(epoch, logOffset)
    }

    suspend fun appendMessage(message: M): MessageMetadata

    fun appendMessageBlocking(message: M): MessageMetadata = runBlocking { appendMessage(message) }

    /**
     * @param transactionalId uniquely identifies this producer for Kafka's transaction coordinator.
     *   Must be stable across restarts for transaction recovery.
     */
    fun openAtomicProducer(transactionalId: String): AtomicProducer<M>

    interface AtomicProducer<M> : AutoCloseable {
        fun openTx(): Tx<M>

        interface Tx<M> : AutoCloseable {
            fun appendMessage(message: M): CompletableDeferred<MessageMetadata>
            fun commit()
            fun abort()
        }

        companion object {
            inline fun <M, R> AtomicProducer<M>.withTx(block: (Tx<M>) -> R): R =
                openTx().use { tx ->
                    try {
                        block(tx).also { tx.commit() }
                    } catch (e: Throwable) {
                        tx.abort()
                        throw e
                    }
                }
        }
    }

    fun readLastMessage(): M?

    /**
     * Reads records in the range [fromMsgId, toMsgId) (start-inclusive, end-exclusive).
     * Returns a lazy sequence of decoded records in offset order.
     * If toMsgId exceeds the latest submitted offset, reads up to the latest available record.
     */
    fun readRecords(fromMsgId: MessageId, toMsgId: MessageId): Sequence<Record<M>>

    suspend fun tailAll(afterMsgId: MessageId, processor: RecordProcessor<M>)
    suspend fun openGroupSubscription(listener: SubscriptionListener<M>)

    interface SubscriptionListener<M> {
        suspend fun onPartitionsAssigned(partitions: Collection<Int>): TailSpec<M>?
        fun onPartitionsAssignedSync(partitions: Collection<Int>): TailSpec<M>? = runBlocking { onPartitionsAssigned(partitions) }
        suspend fun onPartitionsRevoked(partitions: Collection<Int>)
        fun onPartitionsRevokedSync(partitions: Collection<Int>) = runBlocking { onPartitionsRevoked(partitions) }
        suspend fun onPartitionsLost(partitions: Collection<Int>) = onPartitionsRevoked(partitions)
        fun onPartitionsLostSync(partitions: Collection<Int>) = runBlocking { onPartitionsLost(partitions) }
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
