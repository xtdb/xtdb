package xtdb.indexer

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.future.await
import kotlinx.coroutines.runBlocking
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.ArrowStreamReader
import org.slf4j.LoggerFactory
import xtdb.api.log.Log
import xtdb.api.log.Log.Message
import xtdb.api.log.LogOffset
import xtdb.api.log.Watchers
import xtdb.arrow.asChannel
import xtdb.trie.TrieCatalog
import xtdb.util.error
import xtdb.util.logger
import java.nio.channels.ClosedByInterruptException
import java.time.Duration
import java.time.Instant
import kotlin.coroutines.cancellation.CancellationException

class LogProcessor(
    allocator: BufferAllocator,
    private val indexer: IIndexer,
    private val liveIndex: LiveIndex,
    private val log: Log,
    private val trieCatalog: TrieCatalog,
    meterRegistry: MeterRegistry,
    flushTimeout: Duration,
    private val skipTxs: Set<LogOffset>
) : Log.Subscriber, AutoCloseable {

    companion object {
        private val LOG = LogProcessor::class.logger
    }

    private val watchers = Watchers(liveIndex.latestCompletedTx?.txId ?: -1)
    private val LOGGER = LoggerFactory.getLogger(LogProcessor::class.java)

    val ingestionError get() = watchers.exception

    data class Flusher(
        val flushTimeout: Duration,
        var lastFlushCheck: Instant,
        var previousBlockTxId: Long,
        var flushedTxId: Long
    ) {
        constructor(flushTimeout: Duration, liveIndex: LiveIndex) : this(
            flushTimeout, Instant.now(),
            previousBlockTxId = liveIndex.latestCompletedBlockTx?.txId ?: -1,
            flushedTxId = -1
        )

        fun checkBlockTimeout(now: Instant, currentBlockTxId: Long, latestCompletedTxId: Long): Message? =
            when {
                lastFlushCheck + flushTimeout >= now || flushedTxId == latestCompletedTxId -> null
                currentBlockTxId != previousBlockTxId -> {
                    lastFlushCheck = now
                    previousBlockTxId = currentBlockTxId
                    null
                }

                else -> {
                    lastFlushCheck = now
                    Message.FlushBlock(currentBlockTxId)
                }
            }

        fun checkBlockTimeout(liveIndex: LiveIndex) =
            checkBlockTimeout(
                Instant.now(),
                currentBlockTxId = liveIndex.latestCompletedBlockTx?.txId ?: -1,
                latestCompletedTxId = liveIndex.latestCompletedTx?.txId ?: -1
            )
    }

    private val allocator =
        allocator.newChildAllocator("log-processor", 0, Long.MAX_VALUE)
            .also { allocator ->
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
        flusher.checkBlockTimeout(liveIndex)?.let { flushMsg ->
            flusher.flushedTxId = log.appendMessage(flushMsg).await()
        }

        records.forEach { record ->
            val offset = record.logOffset

            try {
                val res = when (val msg = record.message) {
                    is Message.Tx -> {
                        if (skipTxs.isNotEmpty() && skipTxs.contains(offset)) {
                            LOGGER.warn("Skipping transaction offset $offset - within XTDB_SKIP_TXS")
                            // use abort flow in indexTx
                            indexer.indexTx(offset, record.logTimestamp, null)
                        } else {
                            msg.payload.asChannel.use { txOpsCh ->
                                ArrowStreamReader(txOpsCh, allocator).use { reader ->
                                    reader.vectorSchemaRoot.use { root ->
                                        reader.loadNextBatch()
    
                                        indexer.indexTx(offset, record.logTimestamp, root)
                                    }
                                }
                            }
                        }
                    }

                    is Message.FlushBlock -> {
                        liveIndex.forceFlush(record, msg)
                        null
                    }

                    is Message.TriesAdded -> {
                        msg.tries.forEach(trieCatalog::addTrie)
                        null
                    }
                }
                watchers.notify(offset, res)
            } catch (e: InterruptedException) {
                watchers.notify(offset, e)
                throw CancellationException(e)
            } catch (e: ClosedByInterruptException) {
                val ie = InterruptedException(e.toString())
                watchers.notify(offset, ie)
                throw CancellationException(ie)
            } catch (e: Throwable) {
                watchers.notify(offset, e)
                LOG.error(e, "Ingestion stopped: error processing log record at offset $offset.")
                LOG.error("""
                    XTDB transaction processing has encountered an unrecoverable error and has been stopped to prevent corruption of your data.
                    This node has also been marked unhealthy, so if it is running within a container orchestration system (e.g. Kubernetes) it should be restarted shortly.
                    
                    Please see https://docs.xtdb.com/ops/troubleshooting#ingestion-stopped for more information and next steps.
                """.trimIndent())
                throw CancellationException(e)
            }
        }
    }

    @JvmOverloads
    fun awaitAsync(offset: LogOffset = log.latestSubmittedOffset) = watchers.awaitAsync(offset)
}
