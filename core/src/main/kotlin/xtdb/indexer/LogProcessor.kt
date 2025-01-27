package xtdb.indexer

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.future.await
import kotlinx.coroutines.runBlocking
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.ArrowStreamReader
import xtdb.api.log.Log
import xtdb.api.log.LogOffset
import xtdb.arrow.asChannel
import java.time.Duration
import java.time.Instant

class LogProcessor(
    allocator: BufferAllocator,
    private val indexer: IIndexer,
    meterRegistry: MeterRegistry,
    flushTimeout: Duration
) : Log.Processor {

    data class Flusher(
        val flushTimeout: Duration,
        var lastFlushCheck: Instant,
        var previousChunkTxId: Long,
        var flushedTxId: Long
    ) {
        constructor(flushTimeout: Duration, indexer: IIndexer) :
                this(
                    flushTimeout, Instant.now(),
                    previousChunkTxId = indexer.latestCompletedChunkTx()?.txId ?: -1,
                    flushedTxId = -1
                )

        fun checkChunkTimeout(now: Instant, currentChunkTxId: Long, latestCompletedTxId: Long): Log.Message? =
            when {
                lastFlushCheck.plus(flushTimeout) >= now || flushedTxId == latestCompletedTxId -> null
                currentChunkTxId != previousChunkTxId -> {
                    lastFlushCheck = now
                    previousChunkTxId = currentChunkTxId
                    null
                }

                else -> {
                    lastFlushCheck = now
                    Log.Message.FlushChunk(currentChunkTxId)
                }
            }

        fun checkChunkTimeout(indexer: IIndexer) =
            checkChunkTimeout(
                Instant.now(),
                currentChunkTxId = indexer.latestCompletedChunkTx()?.txId ?: -1,
                latestCompletedTxId = indexer.latestCompletedTx()?.txId ?: -1
            )
    }

    private val allocator =
        allocator.newChildAllocator("log-processor", 0, Long.MAX_VALUE)
            .also {
                Gauge.builder("watcher.allocator.allocated_memory", allocator) { it.allocatedMemory.toDouble() }
                    .baseUnit("bytes")
                    .register(meterRegistry)
            }

    private val flusher = Flusher(flushTimeout, indexer)

    override val latestCompletedOffset: LogOffset
        get() = indexer.latestCompletedTx()?.txId ?: -1

    override fun processRecords(log: Log, records: List<Log.Record>) = runBlocking {
        flusher.checkChunkTimeout(indexer)?.let { flushMsg ->
            flusher.flushedTxId = log.appendMessage(flushMsg).await()
        }

        records.forEach { record ->
            when(val msg = record.message) {
                is Log.Message.Tx -> {
                    msg.payload.asChannel.use { txOpsCh ->
                        ArrowStreamReader(txOpsCh, allocator).use { reader ->
                            reader.vectorSchemaRoot.use { root ->
                                reader.loadNextBatch()

                                indexer.indexTx(record.logOffset, record.logTimestamp, root)
                            }
                        }
                    }
                }

                is Log.Message.FlushChunk -> indexer.forceFlush(record)
            }
        }
    }
}
