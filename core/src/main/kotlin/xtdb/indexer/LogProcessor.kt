package xtdb.indexer

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.future.await
import kotlinx.coroutines.runBlocking
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.ArrowStreamReader
import xtdb.api.log.Log
import xtdb.api.log.Log.Message
import xtdb.api.log.LogOffset
import xtdb.api.log.Watchers
import xtdb.arrow.asChannel
import xtdb.trie.TrieCatalog
import java.time.Duration
import java.time.Instant

class LogProcessor(
    allocator: BufferAllocator,
    private val indexer: IIndexer,
    private val liveIndex: LiveIndex,
    private val log: Log,
    private val trieCatalog: TrieCatalog,
    meterRegistry: MeterRegistry,
    flushTimeout: Duration
) : Log.Subscriber, AutoCloseable {

    private val watchers = Watchers(liveIndex.latestCompletedTx?.txId ?: -1)

    val ingestionError get() = watchers.exception

    data class Flusher(
        val flushTimeout: Duration,
        var lastFlushCheck: Instant,
        var previousChunkTxId: Long,
        var flushedTxId: Long
    ) {
        constructor(flushTimeout: Duration, liveIndex: LiveIndex) : this(
            flushTimeout, Instant.now(),
            previousChunkTxId = liveIndex.latestCompletedChunkTx?.txId ?: -1,
            flushedTxId = -1
        )

        fun checkChunkTimeout(now: Instant, currentChunkTxId: Long, latestCompletedTxId: Long): Message? =
            when {
                lastFlushCheck + flushTimeout >= now || flushedTxId == latestCompletedTxId -> null
                currentChunkTxId != previousChunkTxId -> {
                    lastFlushCheck = now
                    previousChunkTxId = currentChunkTxId
                    null
                }

                else -> {
                    lastFlushCheck = now
                    Message.FlushChunk(currentChunkTxId)
                }
            }

        fun checkChunkTimeout(liveIndex: LiveIndex) =
            checkChunkTimeout(
                Instant.now(),
                currentChunkTxId = liveIndex.latestCompletedChunkTx?.txId ?: -1,
                latestCompletedTxId = liveIndex.latestCompletedTx?.txId ?: -1
            )
    }

    private val allocator =
        allocator.newChildAllocator("log-processor", 0, Long.MAX_VALUE)
            .also {
                Gauge.builder("watcher.allocator.allocated_memory", allocator) { it.allocatedMemory.toDouble() }
                    .baseUnit("bytes")
                    .register(meterRegistry)
            }

    private val flusher = Flusher(flushTimeout, liveIndex)

    private val subscription = log.subscribe(this)

    override fun close() {
        subscription.close()
        allocator.close()
    }

    override val latestCompletedOffset: LogOffset
        get() = liveIndex.latestCompletedTx?.txId ?: -1

    override fun processRecords(records: List<Log.Record>) = runBlocking {
        flusher.checkChunkTimeout(liveIndex)?.let { flushMsg ->
            flusher.flushedTxId = log.appendMessage(flushMsg).await()
        }

        records.forEach { record ->
            val offset = record.logOffset

            try {
                val res = when (val msg = record.message) {
                    is Message.Tx -> {
                        msg.payload.asChannel.use { txOpsCh ->
                            ArrowStreamReader(txOpsCh, allocator).use { reader ->
                                reader.vectorSchemaRoot.use { root ->
                                    reader.loadNextBatch()

                                    indexer.indexTx(offset, record.logTimestamp, root)
                                }
                            }
                        }
                    }

                    is Message.FlushChunk -> {
                        liveIndex.forceFlush(record, msg)
                        null
                    }

                    is Message.TriesAdded -> {
                        msg.tries.forEach(trieCatalog::addTrie)
                        null
                    }
                }

                watchers.notify(offset, res)
            } catch (e: Throwable) {
                watchers.notify(offset, e)
            }
        }
    }

    @JvmOverloads
    fun awaitAsync(offset: LogOffset = log.latestSubmittedOffset) = watchers.awaitAsync(offset)
}
