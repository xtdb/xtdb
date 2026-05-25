@file:UseSerializers(DurationSerde::class)

package xtdb.api.log

import kotlinx.coroutines.*
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import kotlinx.serialization.modules.PolymorphicModuleBuilder
import kotlinx.serialization.modules.subclass
import xtdb.DurationSerde
import xtdb.api.Remote
import xtdb.api.RemoteAlias
import xtdb.database.proto.DatabaseConfig
import xtdb.util.MsgIdUtil.afterMsgIdToOffset
import xtdb.util.MsgIdUtil.msgIdToEpoch
import xtdb.util.MsgIdUtil.msgIdToOffset
import xtdb.util.logger
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpRequest.BodyPublishers
import java.net.http.HttpResponse.BodyHandlers
import java.time.Duration
import java.time.Instant
import java.util.Base64
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.CoroutineContext
import com.google.protobuf.Any as ProtoAny

private val LOG = DurableStreamsLog::class.logger

// DS uses application/json so each POST is a one-element array and GET returns
// a positionally-stable array — no custom binary framing needed.
private const val DS_CONTENT_TYPE = "application/json"
private const val HDR_NEXT_OFFSET = "Stream-Next-Offset"

private val B64_ENC = Base64.getEncoder()
private val B64_DEC = Base64.getDecoder()

// Encodes one XTDB byte-array message as a one-element JSON array for DS POST.
// e.g.  ["SGVsbG8="]
private fun encodeOne(bytes: ByteArray): ByteArray =
    "[\"${B64_ENC.encodeToString(bytes)}\"]".toByteArray(Charsets.UTF_8)

// Parses a GET response body (JSON array of base64 strings) into byte arrays.
// Base64 chars are ASCII-safe so no escape sequences appear inside the strings;
// a simple scan is correct and avoids a full JSON library dependency.
internal fun parseMessages(body: String): List<ByteArray> {
    val trimmed = body.trim()
    if (trimmed == "[]" || trimmed.isEmpty()) return emptyList()
    val inner = trimmed.removePrefix("[").removeSuffix("]")
    val result = mutableListOf<ByteArray>()
    var i = 0
    while (i < inner.length) {
        while (i < inner.length && inner[i] != '"') i++   // skip to open quote
        if (i >= inner.length) break
        val start = i + 1
        val end = inner.indexOf('"', start)
        if (end < 0) break
        result += B64_DEC.decode(inner.substring(start, end))
        i = end + 1
    }
    return result
}

/**
 * XTDB log backed by a Durable Streams HTTP server (DS protocol §5).
 *
 * Messages are base64-encoded inside one-element JSON arrays so that
 * each POST is exactly one addressable DS message and GET responses are
 * split cleanly, without custom binary framing.
 *
 * YAML config:
 * ```yaml
 * remotes:
 *   my-ds:
 *     !DurableStreams
 *     baseUrl: https://my-worker.workers.dev
 *
 * log:
 *   !DurableStreams
 *   cluster: my-ds
 *   topic:   xtdb-log
 * ```
 */
class DurableStreamsLog(
    val baseUrl: String,
    private val connectTimeout: Duration = Duration.ofSeconds(10),
    // Kept slightly above the server's 25 s hold so the client never times out first.
    private val longPollTimeout: Duration = Duration.ofSeconds(30),
    coroutineContext: CoroutineContext = Dispatchers.IO,
) : Remote {

    private val http: HttpClient = HttpClient.newBuilder()
        .connectTimeout(connectTimeout)
        .build()

    val scope: CoroutineScope = CoroutineScope(SupervisorJob() + coroutineContext)

    // ---- HTTP helpers -------------------------------------------------------

    /** Idempotently creates the DS stream (PUT §5.1). */
    internal fun ensureStream(url: String) {
        val req = HttpRequest.newBuilder(URI.create(url))
            .PUT(BodyPublishers.noBody())
            .header("Content-Type", DS_CONTENT_TYPE)
            .build()
        val resp = http.send(req, BodyHandlers.discarding())
        check(resp.statusCode() in 200..201) {
            "PUT $url returned ${resp.statusCode()} — stream creation failed"
        }
    }

    /** Returns the offset of the latest message, or -1 if the stream is empty / not found. */
    private fun fetchLatestOffset(url: String): Long {
        val req = HttpRequest.newBuilder(URI.create(url))
            .method("HEAD", BodyPublishers.noBody())
            .build()
        val resp = http.send(req, BodyHandlers.discarding())
        if (resp.statusCode() == 404) return -1L
        check(resp.statusCode() == 200) { "HEAD $url returned ${resp.statusCode()}" }
        // Stream-Next-Offset is the next vacant slot; latest written = nextOffset - 1.
        return resp.headers().firstValue(HDR_NEXT_OFFSET)
            .map { it.toLong() - 1L }
            .orElse(-1L)
    }

    // ---- Inner log ----------------------------------------------------------

    inner class DsLog<M>(
        private val codec: MessageCodec<M>,
        private val streamUrl: String,
        override val epoch: Int,
    ) : Log<M> {

        private val latestOffset0 = AtomicLong(fetchLatestOffset(streamUrl))

        override val latestSubmittedOffset: Long
            get() = latestOffset0.get()

        // §5.2 Append
        override suspend fun appendMessage(message: M): Log.MessageMetadata {
            val body = encodeOne(codec.encode(message))
            val req = HttpRequest.newBuilder(URI.create(streamUrl))
                .POST(BodyPublishers.ofByteArray(body))
                .header("Content-Type", DS_CONTENT_TYPE)
                .build()

            val resp = runInterruptible(Dispatchers.IO) {
                http.send(req, BodyHandlers.discarding())
            }
            check(resp.statusCode() in 200..204) {
                "POST $streamUrl returned ${resp.statusCode()}"
            }

            // Stream-Next-Offset is the offset after the message we just wrote.
            val nextOffset = resp.headers().firstValue(HDR_NEXT_OFFSET)
                .orElseThrow { IllegalStateException("Missing $HDR_NEXT_OFFSET in POST response from $streamUrl") }
                .toLong()

            val logOffset = nextOffset - 1L
            latestOffset0.updateAndGet { it.coerceAtLeast(logOffset) }
            return Log.MessageMetadata(epoch, logOffset, Instant.now())
        }

        // §5.6 + §5.5: read the last message in the stream
        override fun readLastMessage(): M? {
            val latest = fetchLatestOffset(streamUrl)
            if (latest < 0L) return null
            val url = "$streamUrl?offset=$latest"
            val resp = http.send(HttpRequest.newBuilder(URI.create(url)).GET().build(), BodyHandlers.ofString())
            if (resp.statusCode() == 404) return null
            check(resp.statusCode() == 200) { "GET $url returned ${resp.statusCode()}" }
            return parseMessages(resp.body()).lastOrNull()?.let { codec.decode(it) }
        }

        // §5.6 Catch-up read across a [fromMsgId, toMsgId) range
        override fun readRecords(fromMsgId: MessageId, toMsgId: MessageId): Sequence<Log.Record<M>> {
            // Silently skip cross-epoch reads — matches Kafka behaviour.
            if (msgIdToEpoch(fromMsgId) != epoch || msgIdToEpoch(toMsgId) != epoch) return emptySequence()
            val fromOffset = msgIdToOffset(fromMsgId)
            val toOffset = msgIdToOffset(toMsgId)
            if (fromOffset >= toOffset) return emptySequence()

            return sequence {
                val url = "$streamUrl?offset=$fromOffset"
                val resp = http.send(
                    HttpRequest.newBuilder(URI.create(url)).GET().build(),
                    BodyHandlers.ofString()
                )
                if (resp.statusCode() == 404) return@sequence
                check(resp.statusCode() == 200) { "GET $url returned ${resp.statusCode()}" }

                // DS returns all messages from fromOffset to current tail in one response.
                // Slice to the requested window.
                val approxTimestamp = Instant.now()
                for ((i, bytes) in parseMessages(resp.body()).withIndex()) {
                    val offset = fromOffset + i
                    if (offset >= toOffset) break
                    val msg = codec.decode(bytes) ?: continue
                    yield(Log.Record(epoch, offset, approxTimestamp, msg))
                }
            }
        }

        // §SCR-246 long-poll: tail the stream indefinitely, delivering records to processor.
        // Each iteration issues one long-poll GET; 200 = data arrived, 204 = timeout (loop again).
        override suspend fun tailAll(afterMsgId: MessageId, processor: Log.RecordProcessor<M>) =
            coroutineScope {
                // Compute starting offset from afterMsgId.
                val prevOffset = afterMsgIdToOffset(epoch, afterMsgId)
                var offset = if (prevOffset < 0L) 0L else prevOffset + 1L

                while (isActive) {
                    val url = "$streamUrl?offset=$offset&live=long-poll"
                    val req = HttpRequest.newBuilder(URI.create(url))
                        .GET()
                        .timeout(longPollTimeout)
                        .build()

                    val resp = runInterruptible(Dispatchers.IO) {
                        http.send(req, BodyHandlers.ofString())
                    }

                    when (resp.statusCode()) {
                        200 -> {
                            val body = resp.body()
                            val messages = parseMessages(body)
                            if (messages.isNotEmpty()) {
                                val batchTime = Instant.now()
                                val records = messages.mapIndexedNotNull { i, bytes ->
                                    codec.decode(bytes)?.let { msg ->
                                        Log.Record(epoch, offset + i, batchTime, msg)
                                    }
                                }
                                if (records.isNotEmpty()) {
                                    processor.processRecords(records)
                                    offset += messages.size.toLong()
                                    latestOffset0.updateAndGet { it.coerceAtLeast(offset - 1) }
                                }
                            }
                            // Advance offset from header when up-to-date (empty batch from long-poll).
                            resp.headers().firstValue(HDR_NEXT_OFFSET).ifPresent { hdr ->
                                val nextOff = hdr.toLong()
                                if (nextOff > offset) offset = nextOff
                            }
                        }

                        204 -> {
                            // Long-poll timeout or stream closed at tail — nothing new, loop.
                            resp.headers().firstValue(HDR_NEXT_OFFSET).ifPresent { hdr ->
                                val nextOff = hdr.toLong()
                                if (nextOff > offset) offset = nextOff
                            }
                        }

                        else -> {
                            LOG.warn { "Unexpected ${resp.statusCode()} from long-poll $url — backing off 1 s" }
                            delay(1_000)
                        }
                    }
                }
            }

        // Group subscription: DS has no consumer-group protocol, so we model the stream
        // as a single-partition group.  The listener assigns itself to partition 0 and
        // we run tailAll for as long as the coroutine lives.
        override suspend fun openGroupSubscription(listener: Log.SubscriptionListener<M>) {
            val tailSpec = listener.onPartitionsAssigned(listOf(0)) ?: return
            try {
                tailAll(tailSpec.afterMsgId, tailSpec.processor)
            } finally {
                withContext(NonCancellable) {
                    listener.onPartitionsRevokedSync(listOf(0))
                }
            }
        }

        override fun close() = Unit
    }

    // ---- Lifecycle ----------------------------------------------------------

    override fun close() {
        runBlocking { withTimeout(5_000) { scope.coroutineContext.job.cancelAndJoin() } }
    }

    // ---- Serializable factories ---------------------------------------------

    @Serializable
    @SerialName("!DurableStreams")
    data class ClusterFactory @JvmOverloads constructor(
        val baseUrl: String,
        var connectTimeout: Duration = Duration.ofSeconds(10),
        var longPollTimeout: Duration = Duration.ofSeconds(30),
    ) : Remote.Factory<DurableStreamsLog> {

        fun connectTimeout(d: Duration) = apply { connectTimeout = d }
        fun longPollTimeout(d: Duration) = apply { longPollTimeout = d }

        override fun open() = DurableStreamsLog(baseUrl, connectTimeout, longPollTimeout)
    }

    @Serializable
    @SerialName("!DurableStreams")
    data class LogFactory @JvmOverloads constructor(
        val cluster: RemoteAlias,
        val topic: String,
        var replicaTopic: String = "$topic-replica",
        var epoch: Int = 0,
        var tenant: String = "xtdb",
        var createStream: Boolean = true,
    ) : Log.Factory {

        fun replicaTopic(t: String) = apply { replicaTopic = t }
        fun epoch(e: Int) = apply { epoch = e }
        fun tenant(t: String) = apply { tenant = t }
        fun createStream(c: Boolean) = apply { createStream = c }

        private fun resolve(remotes: Map<RemoteAlias, Remote>): DurableStreamsLog {
            val alias = cluster
            return requireNotNull(remotes[alias] as? DurableStreamsLog) {
                "Missing DurableStreams remote: '$alias'"
            }
        }

        private fun <M> openLog(
            ds: DurableStreamsLog,
            codec: MessageCodec<M>,
            streamName: String,
        ): Log<M> {
            val url = "${ds.baseUrl}/streams/$tenant/$streamName"
            if (createStream) ds.ensureStream(url)
            return ds.DsLog(codec, url, epoch)
        }

        override fun openSourceLog(remotes: Map<RemoteAlias, Remote>): Log<SourceMessage> =
            openLog(resolve(remotes), SourceMessage.Codec, topic)

        override fun openReadOnlySourceLog(remotes: Map<RemoteAlias, Remote>) =
            ReadOnlyLog(openSourceLog(remotes))

        override fun openReplicaLog(remotes: Map<RemoteAlias, Remote>): Log<ReplicaMessage> =
            openLog(resolve(remotes), ReplicaMessage.Codec, replicaTopic)

        override fun openReadOnlyReplicaLog(remotes: Map<RemoteAlias, Remote>) =
            ReadOnlyLog(openReplicaLog(remotes))

        // writeTo serialises the log config to protobuf for multi-node followers to
        // reconstruct their log handle.  Single-node DS deployments don't need this;
        // a proto definition can be added when multi-node support is required.
        override fun writeTo(dbConfig: DatabaseConfig.Builder) {
            throw UnsupportedOperationException(
                "DurableStreams log does not yet support multi-node log config persistence via writeTo; " +
                "add a proto definition when follower nodes are needed."
            )
        }
    }

    // ---- SPI registrations --------------------------------------------------

    /**
     * @suppress
     */
    class Registration : Log.Registration {
        override val protoTag: String
            get() = throw UnsupportedOperationException("DurableStreams log has no proto tag")

        override fun fromProto(msg: ProtoAny): Log.Factory =
            throw UnsupportedOperationException("DurableStreams log cannot be reconstructed from proto")

        override fun registerSerde(builder: PolymorphicModuleBuilder<Log.Factory>) {
            builder.subclass(LogFactory::class)
        }
    }

    /**
     * @suppress
     */
    class ClusterRegistration : Remote.Registration {
        override fun registerSerde(builder: PolymorphicModuleBuilder<Remote.Factory<*>>) {
            builder.subclass(ClusterFactory::class)
        }
    }
}
