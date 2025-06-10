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
import xtdb.api.log.MessageId
import xtdb.api.log.Watchers
import xtdb.api.storage.Storage
import xtdb.arrow.asChannel
import xtdb.error.Interrupted
import xtdb.trie.TrieCatalog
import xtdb.util.TxIdUtil.offsetToTxId
import xtdb.util.TxIdUtil.txIdToEpoch
import xtdb.util.TxIdUtil.txIdToOffset
import xtdb.util.error
import xtdb.util.logger
import xtdb.arrow.RelationReader
import java.nio.channels.ClosedByInterruptException
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime
import kotlin.coroutines.cancellation.CancellationException

class LogProcessor(
    allocator: BufferAllocator,
    private val indexer: IIndexer,
    private val liveIndex: LiveIndex,
    private val log: Log,
    private val trieCatalog: TrieCatalog,
    meterRegistry: MeterRegistry,
    flushTimeout: Duration,
    private val skipTxs: Set<MessageId>
) : Log.Subscriber, AutoCloseable {

    companion object {
        private val LOG = LogProcessor::class.logger
    }

    private val epoch = log.epoch

    override var latestProcessedMsgId: MessageId =
        liveIndex.latestCompletedTx?.txId?.let {
            if (txIdToEpoch(it) == epoch) it else offsetToTxId(epoch, 0) - 1
        } ?: -1
        private set

    override val latestSubmittedMsgId: MessageId
        get() = offsetToTxId(epoch, log.latestSubmittedOffset)

    private val watchers = Watchers(latestProcessedMsgId)
    private val LOGGER = LoggerFactory.getLogger(LogProcessor::class.java)

    val ingestionError get() = watchers.exception

    data class Flusher(
        val flushTimeout: Duration,
        var lastFlushCheck: Instant,
        var previousBlockTxId: MessageId,
        var flushedTxId: MessageId
    ) {
        constructor(flushTimeout: Duration, liveIndex: LiveIndex) : this(
            flushTimeout, Instant.now(),
            previousBlockTxId = liveIndex.latestCompletedBlockTx?.txId ?: -1,
            flushedTxId = -1
        )

        fun checkBlockTimeout(now: Instant, currentBlockTxId: MessageId, latestCompletedTxId: MessageId): Message? =
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

    override fun close() {
        subscription.close()
        allocator.close()
    }

    private val flusher = Flusher(flushTimeout, liveIndex)

    private val latestProcessedOffset = liveIndex.latestCompletedTx?.txId?.let {
        if (txIdToEpoch(it) == epoch) txIdToOffset(it) else -1
    } ?: -1
    private val subscription = log.subscribe(this, latestProcessedOffset)

    override fun processRecords(records: List<Log.Record>) = runBlocking {
        flusher.checkBlockTimeout(liveIndex)?.let { flushMsg ->
            val offset = log.appendMessage(flushMsg).await().logOffset
            flusher.flushedTxId = offsetToTxId(epoch, offset)
        }

        records.forEach { record ->
            val msgId = offsetToTxId(epoch, record.logOffset)

            try {
                val res = when (val msg = record.message) {
                    is Message.Tx -> {
                        if (skipTxs.isNotEmpty() && skipTxs.contains(msgId)) {
                            LOGGER.warn("Skipping transaction id $msgId - within XTDB_SKIP_TXS")
                            // use abort flow in indexTx
                            indexer.indexTx(
                                msgId, record.logTimestamp,
                                null, null, null, null
                            )
                        } else {
                            msg.payload.asChannel.use { txOpsCh ->
                                ArrowStreamReader(txOpsCh, allocator).use { reader ->
                                    reader.vectorSchemaRoot.use { root ->
                                        reader.loadNextBatch()
                                        val rdr = RelationReader.from(root)

                                        val systemTime =
                                            (rdr["system-time"].getObject(0) as ZonedDateTime?)?.toInstant()

                                        val defaultTz =
                                            (rdr["default-tz"].getObject(0) as String?).let { ZoneId.of(it) }

                                        val user = rdr["user"].getObject(0) as String?

                                        indexer.indexTx(
                                            msgId, record.logTimestamp,
                                            rdr["tx-ops"].listElements,
                                            systemTime, defaultTz, user
                                        )
                                    }
                                }
                            }
                        }
                    }

                    is Message.FlushBlock -> {
                        liveIndex.forceFlush(msg, msgId, record.logTimestamp)
                        null
                    }

                    is Message.TriesAdded -> {
                        if (msg.storageVersion == Storage.VERSION)
                            msg.tries.groupBy { it.tableName }.forEach { (tableName, tries) ->
                                trieCatalog.addTries(tableName, tries, record.logTimestamp)
                            }
                        null
                    }
                }
                latestProcessedMsgId = msgId
                watchers.notify(msgId, res)
            } catch (e: ClosedByInterruptException) {
                watchers.notify(msgId, e)
                throw CancellationException(e)
            } catch (e: InterruptedException) {
                watchers.notify(msgId, e)
                throw CancellationException(e)
            } catch (e: Interrupted) {
                watchers.notify(msgId, e)
                throw CancellationException(e)
            } catch (e: Throwable) {
                watchers.notify(msgId, e)
                LOG.error(
                    e,
                    "Ingestion stopped: error processing log record at id $msgId (epoch: $epoch, logOffset: ${record.logOffset})"
                )
                LOG.error(
                    """
                    XTDB transaction processing has encountered an unrecoverable error and has been stopped to prevent corruption of your data.
                    This node has also been marked unhealthy, so if it is running within a container orchestration system (e.g. Kubernetes) it should be restarted shortly.

                    Please see https://docs.xtdb.com/ops/troubleshooting#ingestion-stopped for more information and next steps.
                """.trimIndent()
                )
                throw CancellationException(e)
            }
        }
    }

    @JvmOverloads
    fun awaitAsync(msgId: MessageId = latestSubmittedMsgId) = watchers.awaitAsync(msgId)
}
