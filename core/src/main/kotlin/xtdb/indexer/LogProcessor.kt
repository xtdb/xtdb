package xtdb.indexer

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.future.await
import kotlinx.coroutines.runBlocking
import org.apache.arrow.memory.BufferAllocator
import xtdb.api.TransactionAborted
import xtdb.api.TransactionCommitted
import xtdb.api.TransactionKey
import xtdb.api.log.Log
import xtdb.api.log.Log.Message
import xtdb.api.log.MessageId
import xtdb.api.log.Watchers
import xtdb.api.storage.Storage
import xtdb.arrow.Relation
import xtdb.arrow.asChannel
import xtdb.catalog.BlockCatalog
import xtdb.compactor.Compactor
import xtdb.database.Database
import xtdb.error.Anomaly
import xtdb.error.Interrupted
import xtdb.table.TableRef
import xtdb.util.MsgIdUtil.msgIdToEpoch
import xtdb.util.MsgIdUtil.msgIdToOffset
import xtdb.util.MsgIdUtil.offsetToMsgId
import xtdb.util.StringUtil.asLexHex
import xtdb.util.debug
import xtdb.util.error
import xtdb.util.logger
import xtdb.util.warn
import java.nio.channels.ClosedByInterruptException
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime
import kotlin.coroutines.cancellation.CancellationException

private val LOG = LogProcessor::class.logger

/**
 * @param dbCatalog supplied iff you want to write secondary databases at the end of a block.
 *                  i.e. iff db == 'xtdb'
 */
class LogProcessor(
    allocator: BufferAllocator,
    meterRegistry: MeterRegistry,
    private val dbCatalog: Database.Catalog?,
    private val db: Database,
    private val indexer: Indexer.ForDatabase,
    private val compactor: Compactor.ForDatabase,
    flushTimeout: Duration,
    private val skipTxs: Set<MessageId>
) : Log.Subscriber, AutoCloseable {

    init {
        assert((dbCatalog != null) == (db.name == "xtdb")) { "dbCatalog supplied iff db == 'xtdb'" }
    }

    private val log = db.log
    private val epoch = log.epoch

    private val blockCatalog = db.blockCatalog
    private val trieCatalog = db.trieCatalog
    private val liveIndex = db.liveIndex

    @Volatile
    override var latestProcessedMsgId: MessageId =
        blockCatalog.latestProcessedMsgId?.let {
            // used if the epoch is incremented so that we seek to the start of the new log
            if (msgIdToEpoch(it) == epoch) it else offsetToMsgId(epoch, 0) - 1
        } ?: -1
        private set

    private val latestProcessedOffset = blockCatalog.latestProcessedMsgId?.let {
        if (msgIdToEpoch(it) == epoch) msgIdToOffset(it) else -1
    } ?: -1

    override val latestSubmittedMsgId: MessageId
        get() = offsetToMsgId(epoch, log.latestSubmittedOffset)

    private val watchers = Watchers(latestProcessedMsgId)

    val ingestionError get() = watchers.exception

    data class Flusher(
        val flushTimeout: Duration,
        var lastFlushCheck: Instant,
        var previousBlockTxId: MessageId,
        var flushedTxId: MessageId
    ) {
        constructor(flushTimeout: Duration, blockCatalog: BlockCatalog) : this(
            flushTimeout, Instant.now(),
            previousBlockTxId = blockCatalog.latestCompletedTx?.txId ?: -1,
            flushedTxId = -1
        )

        fun checkBlockTimeout(now: Instant, currentBlockTxId: MessageId, latestCompletedTxId: MessageId): Boolean =
            when {
                lastFlushCheck + flushTimeout >= now || flushedTxId == latestCompletedTxId -> false

                currentBlockTxId != previousBlockTxId -> {
                    lastFlushCheck = now
                    previousBlockTxId = currentBlockTxId
                    false
                }

                else -> {
                    lastFlushCheck = now
                    true
                }
            }

        fun checkBlockTimeout(blockCatalog: BlockCatalog, liveIndex: LiveIndex) =
            checkBlockTimeout(
                Instant.now(),
                currentBlockTxId = blockCatalog.latestCompletedTx?.txId ?: -1,
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

    private val flusher = Flusher(flushTimeout, blockCatalog)

    private val subscription = log.subscribe(this, latestProcessedOffset)

    override fun processRecords(records: List<Log.Record>) = runBlocking {
        if (flusher.checkBlockTimeout(blockCatalog, liveIndex)) {
            val flushMessage = Message.FlushBlock(blockCatalog.currentBlockIndex ?: -1)
            val offset = log.appendMessage(flushMessage).await().logOffset
            flusher.flushedTxId = offsetToMsgId(epoch, offset)
        }

        records.forEach { record ->
            val msgId = offsetToMsgId(epoch, record.logOffset)
            LOG.debug("Processing message $msgId, ${record.message.javaClass.simpleName}")

            try {
                val res = when (val msg = record.message) {
                    is Message.Tx -> {
                        val result = if (skipTxs.isNotEmpty() && skipTxs.contains(msgId)) {
                            LOG.warn("Skipping transaction id $msgId - within XTDB_SKIP_TXS")
                            // use abort flow in indexTx
                            indexer.indexTx(
                                msgId, record.logTimestamp,
                                null, null, null, null
                            )
                        } else {
                            msg.payload.asChannel.use { txOpsCh ->
                                Relation.StreamLoader(allocator, txOpsCh).use { loader ->
                                    Relation(allocator, loader.schema).use { rel ->
                                        loader.loadNextPage(rel)

                                        val systemTime =
                                            (rel["system-time"].getObject(0) as ZonedDateTime?)?.toInstant()

                                        val defaultTz =
                                            (rel["default-tz"].getObject(0) as String?).let { ZoneId.of(it) }

                                        val user = rel["user"].getObject(0) as String?

                                        indexer.indexTx(
                                            msgId, record.logTimestamp,
                                            rel["tx-ops"].listElements,
                                            systemTime, defaultTz, user
                                        )
                                    }
                                }
                            }
                        }

                        if (liveIndex.isFull())
                            finishBlock()

                        result
                    }

                    is Message.FlushBlock -> {
                        val expectedBlockIdx = msg.expectedBlockIdx
                        if (expectedBlockIdx != null && expectedBlockIdx == (blockCatalog.currentBlockIndex ?: -1L))
                            finishBlock()

                        null
                    }

                    is Message.TriesAdded -> {
                        if (msg.storageVersion == Storage.VERSION)
                            msg.tries.groupBy { it.tableName }.forEach { (tableName, tries) ->
                                trieCatalog.addTries(TableRef.parse(db.name, tableName), tries, record.logTimestamp)
                            }
                        null
                    }

                    is Message.AttachDatabase -> {
                        requireNotNull(dbCatalog) { "attach-db received on non-primary database ${db.name}" }
                        val res = try {
                            dbCatalog.attach(msg.dbName, msg.config)
                            TransactionCommitted(msgId, record.logTimestamp)
                        } catch (e: Anomaly.Caller) {
                            TransactionAborted(msgId, record.logTimestamp, e)
                        }

                        indexer.addTxRow(
                            TransactionKey(msgId, record.logTimestamp),
                            (res as? TransactionAborted)?.error
                        )

                        res
                    }

                    is Message.DetachDatabase -> {
                        requireNotNull(dbCatalog) { "detach-db received on non-primary database ${db.name}" }
                        val res = try {
                            dbCatalog.detach(msg.dbName)
                            TransactionCommitted(msgId, record.logTimestamp)
                        } catch (e: Anomaly.Caller) {
                            TransactionAborted(msgId, record.logTimestamp, e)
                        }

                        indexer.addTxRow(
                            TransactionKey(msgId, record.logTimestamp),
                            (res as? TransactionAborted)?.error
                        )

                        res
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

            LOG.debug("Processed message $msgId")
        }
    }

    fun finishBlock() {
        val blockIdx = (blockCatalog.currentBlockIndex ?: -1) + 1
        LOG.debug("finishing block: 'b${blockIdx.asLexHex}'...")
        val tables = liveIndex.finishBlock(blockIdx)
        val secondaryDatabases = dbCatalog?.takeIf { db.name == "xtdb" }?.serialisedSecondaryDatabases

        blockCatalog.finishBlock(
            blockIdx, liveIndex.latestCompletedTx, latestProcessedMsgId,
            tables, secondaryDatabases
        )

        liveIndex.nextBlock()
        compactor.signalBlock()
        LOG.debug("finished block: 'b${blockIdx.asLexHex}'.")
    }

    @JvmOverloads
    fun awaitAsync(msgId: MessageId = latestSubmittedMsgId) = watchers.awaitAsync(msgId)
}
