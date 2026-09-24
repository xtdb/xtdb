package xtdb.test

import com.google.protobuf.Struct
import com.google.protobuf.Value
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.dropWhile
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.modules.PolymorphicModuleBuilder
import kotlinx.serialization.modules.subclass
import xtdb.InternalApi
import xtdb.api.Remote
import xtdb.api.RemoteAlias
import xtdb.api.error.Incorrect
import xtdb.api.tx.ExternalSource
import xtdb.api.tx.ExternalSourceToken
import xtdb.api.tx.OpenTx
import xtdb.api.tx.TxIndexer
import xtdb.api.tx.TxIndexer.TxResult
import java.nio.ByteBuffer
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import com.google.protobuf.Any as ProtoAny

/**
 * An upstream fed by hand: one replayable stream per partition, read by the [ExternalSource]s [open]ed over it.
 *
 * [publish] a message to a partition, and a term leading that partition turns it into a transaction.
 * Await it through that partition's `Watchers`.
 *
 * Each stream keeps every message, and a term reads from just after the token it resumes from, so a term that
 * dies mid-message re-reads it, and every source opened over this upstream sees every message.
 *
 * A database config names it through [factory], which carries only a name: `ATTACH`, a block's record of its
 * secondaries and a restart all find this upstream by that name, within the JVM that created it.
 * [close] it after the nodes that use it — a database opened over a closed upstream fails to open.
 */
class InMemoryExternalSource(
    partitions: Int = 1,

    /**
     * How a message becomes a transaction — by default the blocking [TxIndexer.executeTx]; pass a
     * `submitTx`-based one to drive the fire-and-forget path.
     */
    private val index: suspend TxIndexer.(Msg) -> Unit = { executeTx(it.token, writer = it.writer) },
) : AutoCloseable {

    /** One upstream event: its resume marker, and what its transaction writes. */
    class Msg(val token: ExternalSourceToken, val writer: suspend (OpenTx) -> TxResult)

    private class Stream {
        val mutex = Mutex()
        var nextOffset = 0L

        // unbounded: a term replays from wherever it resumes, and a capped buffer would drop the oldest
        // messages while nothing is reading, which is when tests publish
        val msgs = MutableSharedFlow<Pair<Long, Msg>>(replay = Int.MAX_VALUE)
    }

    private val streams = List(partitions) { Stream() }

    private val name = UUID.randomUUID().toString()

    init {
        REGISTRY[name] = this
    }

    val factory get() = Factory(name)

    /** @return the message's resume marker, as the database will persist it with the transaction. */
    suspend fun publish(
        partition: Int = 0,
        writer: suspend (OpenTx) -> TxResult = { TxResult.Committed() },
    ): ExternalSourceToken {
        val stream = streams[partition]

        // offsets are assigned and emitted under one lock, so every reader sees them in order
        return stream.mutex.withLock {
            val offset = stream.nextOffset++
            val token = offset.toToken()
            stream.msgs.emit(offset to Msg(token, writer))
            token
        }
    }

    private inner class Source : ExternalSource {
        override suspend fun onPartitionAssigned(
            partition: Int, afterToken: ExternalSourceToken?, txIndexer: TxIndexer
        ) {
            val after = afterToken?.toOffset() ?: -1

            streams[partition].msgs
                .dropWhile { (offset, _) -> offset <= after }
                .collect { (_, msg) -> txIndexer.index(msg) }
        }

        // the upstream outlives every source opened over it
        override fun close() = Unit
    }

    /** A source over this upstream, as a database opens one: called for each partition this node leads. */
    fun open(): ExternalSource = Source()

    override fun close() {
        REGISTRY.remove(name, this)
    }

    @Serializable
    @SerialName("!InMemorySource")
    data class Factory(val name: String) : ExternalSource.Factory {

        @InternalApi
        override val maxPartitions get() = Int.MAX_VALUE

        override fun open(dbName: String, remotes: Map<RemoteAlias, Remote>, meterRegistry: MeterRegistry?) =
            (REGISTRY[name]
                ?: throw Incorrect(
                    "no in-memory external source named '$name' — has it been closed?",
                    "xtdb.test/no-such-in-memory-source", mapOf("name" to name)
                )).open()

        class Registration : ExternalSource.Registration<Factory> {
            // a well-known type, as no module generates protobuf from a test source set
            override val protoTag get() = "$PROTO_TAG_PREFIX/google.protobuf.Struct"

            override val factoryClass get() = Factory::class.java

            override fun toProto(factory: Factory): ProtoAny = ProtoAny.pack(
                Struct.newBuilder()
                    .putFields("name", Value.newBuilder().setStringValue(factory.name).build())
                    .build(),
                PROTO_TAG_PREFIX,
            )

            // decodes only: whether the upstream still exists is `open`'s to find out, as a real source
            // finds out its upstream has gone when it connects
            override fun fromProto(msg: ProtoAny) =
                Factory(msg.unpack(Struct::class.java).getFieldsOrThrow("name").stringValue)

            override fun registerSerde(builder: PolymorphicModuleBuilder<ExternalSource.Factory>) {
                builder.subclass(Factory::class)
            }
        }
    }

    private companion object {
        const val PROTO_TAG_PREFIX = "proto.xtdb.com"

        val REGISTRY = ConcurrentHashMap<String, InMemoryExternalSource>()

        fun Long.toToken(): ExternalSourceToken = ByteBuffer.allocate(Long.SIZE_BYTES).putLong(this).array()
        fun ExternalSourceToken.toOffset() = ByteBuffer.wrap(this).long
    }
}
