@file:UseSerializers(DurationSerde::class, PathWithEnvVarSerde::class)

package xtdb.api.log

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.future.future
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient
import kotlinx.serialization.UseSerializers
import xtdb.DurationSerde
import xtdb.api.PathWithEnvVarSerde
import xtdb.api.log.Log.*
import xtdb.time.InstantUtil.asMicros
import xtdb.time.InstantUtil.fromMicros
import java.io.DataInputStream
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.nio.channels.ClosedByInterruptException
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption.*
import java.time.Instant
import java.time.InstantSource
import kotlin.io.path.createParentDirectories
import kotlin.io.path.exists
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.Int.Companion.SIZE_BYTES as INT_BYTES
import kotlin.Long.Companion.SIZE_BYTES as LONG_BYTES

class LocalLog(rootPath: Path, private val instantSource: InstantSource, override val epoch: Int) : Log {
    companion object {
        private val Path.logFilePath get() = resolve("LOG")

        private fun messageSizeBytes(size: Int) = 1 + INT_BYTES + LONG_BYTES + size + LONG_BYTES

        private const val RECORD_SEPARATOR = 0x1E.toByte()

        private fun readLatestSubmittedOffset(path: Path): LogOffset {
            val logFilePath = path.logFilePath
            if (!logFilePath.exists()) return -1

            return FileChannel.open(logFilePath).use { ch ->
                val chSize = ch.size()

                if (chSize == 0L) return -1

                try {
                    val buf = ByteBuffer.allocateDirect(LONG_BYTES)

                    check(ch.read(buf, chSize - LONG_BYTES) == LONG_BYTES) {
                        "Failed to read last offset in log file"
                    }

                    buf.flip().getLong()
                        .also { offset ->
                            check(offset in 0..<chSize) { "Invalid offset in log file: $offset" }
                            ch.position(offset)
                            DataInputStream(Channels.newInputStream(ch)).use { dataStream ->
                                check(dataStream.readByte() == RECORD_SEPARATOR) {
                                    "log file corrupted - expected record separator at $offset"
                                }

                                val size = dataStream.readInt()
                                check(chSize == offset + messageSizeBytes(size)) {
                                    "log file corrupted - record at $offset specifies size $size, but file size is $chSize"
                                }
                            }
                        }
                } catch (e: Exception) {
                    throw IllegalStateException("Failed to read log file", e)
                }
            }
        }

        private fun FileChannel.readMessage(): Record {
            val pos = position()
            val headerBuf = ByteBuffer.allocateDirect(1 + INT_BYTES + LONG_BYTES)
                .also { read(it); it.flip() }

            check(headerBuf.get() == RECORD_SEPARATOR) { "log file corrupted at $pos - expected record separator" }
            val size = headerBuf.getInt()

            return Record(
                pos,
                fromMicros(headerBuf.getLong()),
                Message.parse(ByteBuffer.allocate(size).also { read(it); it.flip() })
            ).also {
                position(pos + messageSizeBytes(size))
            }
        }
    }

    private val scope: CoroutineScope = CoroutineScope(Dispatchers.Default)

    internal data class NewMessage(
        val message: Message,
        val onCommit: CompletableDeferred<Record>
    )

    private val appendCh = Channel<NewMessage>(capacity = 10)

    private val logFilePath = rootPath.logFilePath

    private val logFileChannel =
        FileChannel.open(logFilePath.createParentDirectories(), CREATE, WRITE, APPEND)

    private fun writeMessages(msgs: List<NewMessage>): Array<Record> {
        val initialOffset = logFileChannel.position()

        try {
            val res = Array(msgs.size) { idx ->
                val (msg) = msgs[idx]
                // we only use the instantSource for Tx messages so that the tests
                // that check files can be deterministic
                val ts = if (msg is Message.Tx) instantSource.instant() else Instant.now()
                val payload = msg.encode()
                val size = payload.remaining()
                val offset = logFileChannel.position()

                logFileChannel.write(
                    ByteBuffer
                        .allocateDirect(messageSizeBytes(size))
                        .run {
                            put(RECORD_SEPARATOR)
                            putInt(size)
                            putLong(ts.asMicros)
                            put(payload)
                            putLong(offset)
                            flip()
                        })

                Record(offset, ts, msg)
            }

            logFileChannel.force(true)

            return res
        } catch (t: Throwable) {
            logFileChannel.truncate(initialOffset)
            throw t
        }
    }

    @Volatile
    override var latestSubmittedOffset: LogOffset = readLatestSubmittedOffset(rootPath)
        private set

    @Volatile
    private var committedCh = MutableSharedFlow<Record>(extraBufferCapacity = 100)

    private val mutex = Mutex()

    init {
        scope.launch {
            try {
                while (true) {
                    val msgs = mutableListOf(appendCh.receive())

                    while (true) {
                        if (msgs.size >= 10) break
                        msgs.add(appendCh.tryReceive().getOrNull() ?: break)
                    }

                    val records = writeMessages(msgs)

                    msgs.forEachIndexed { idx, msg ->
                        records[idx].also {
                            msg.onCommit.complete(it)

                            mutex.withLock {
                                committedCh.emit(it)
                                latestSubmittedOffset = it.logOffset
                            }
                        }
                    }
                }
            } catch (e: ClosedByInterruptException) {
                cancel()
            } catch (e: InterruptedException) {
                cancel()
            }
        }

        scope.launch {
        }
    }

    override fun appendMessage(message: Message) =
        scope.future {
            val onCommit = CompletableDeferred<Record>()
            appendCh.send(NewMessage(message, onCommit))
            val record = onCommit.await()
            MessageMetadata(record.logOffset, record.logTimestamp)
        }

    override fun subscribe(subscriber: Subscriber, latestProcessedOffset: LogOffset): Subscription {
        var latestCompletedOffset = latestProcessedOffset

        val ch = Channel<Record>(100)

        val subscription = scope.launch(SupervisorJob()) {
            launch {
                committedCh
                    .onSubscription {
                        val targetOffset = mutex.withLock { latestSubmittedOffset }
                        if (targetOffset < 0) return@onSubscription

                        runInterruptible {
                            FileChannel.open(logFilePath).use { ch ->
                                val latestCompleted = latestCompletedOffset
                                if (latestCompleted >= 0) {
                                    ch.position(latestCompleted)
                                    ch.readMessage()
                                }

                                while (ch.position() <= targetOffset) {
                                    subscriber.processRecords(listOf(ch.readMessage()))
                                }
                            }
                        }
                    }
                    .onEach {
                        if (it.logOffset > latestCompletedOffset) {
                            latestCompletedOffset = it.logOffset
                            ch.send(it)
                        }
                    }
                    .onCompletion { ch.close() }
                    .collect()
            }

            while (true) {
                val msg = withTimeoutOrNull(1.minutes) { ch.receive() }
                runInterruptible { subscriber.processRecords(listOfNotNull(msg)) }
            }
        }

        return Subscription { runBlocking { withTimeout(5.seconds) { subscription.cancelAndJoin() } } }
    }

    override fun close() {
        runBlocking { withTimeout(5.seconds) { scope.coroutineContext.job.cancelAndJoin() } }
        logFileChannel.close()
    }

    /**
     * Used to set configuration options for a local directory based XTDB Log.
     *
     * Example usage, as part of a node config:
     * ```kotlin
     * Xtdb.openNode {
     *    log = localLog(Path("test-path")) {
     *      instantSource = InstantSource.system()
     *      bufferSize = 4096
     *      pollSleepDuration = Duration.ofMillis(100)
     *    }
     *    ...
     * }
     * ```
     */
    @SerialName("!Local")
    @Serializable
    data class Factory @JvmOverloads constructor(
        val path: Path,
        @Transient var instantSource: InstantSource = InstantSource.system(),
        var epoch: Int = 0
    ) : Log.Factory {

        @Suppress("unused")
        fun instantSource(instantSource: InstantSource) = apply { this.instantSource = instantSource }
        fun epoch(epoch: Int) = apply { this.epoch = epoch }

        override fun openLog() = LocalLog(path, instantSource, epoch)
    }
}
