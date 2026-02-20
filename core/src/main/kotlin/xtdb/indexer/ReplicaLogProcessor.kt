package xtdb.indexer

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.future.await
import kotlinx.coroutines.runBlocking
import org.apache.arrow.memory.BufferAllocator
import xtdb.api.TransactionAborted
import xtdb.api.TransactionCommitted
import xtdb.api.TransactionKey
import xtdb.time.InstantUtil
import xtdb.util.TransitFormat.MSGPACK
import xtdb.util.readTransit
import xtdb.api.TransactionResult
import xtdb.api.log.Log
import xtdb.api.log.Log.Message
import xtdb.api.log.MessageId
import xtdb.api.log.Watchers
import xtdb.api.storage.Storage
import xtdb.catalog.BlockCatalog
import xtdb.catalog.BlockCatalog.Companion.allBlockFiles
import xtdb.compactor.Compactor
import xtdb.database.Database
import xtdb.database.DatabaseState
import xtdb.database.DatabaseStorage
import xtdb.block.proto.Block
import xtdb.database.proto.DatabaseConfig
import xtdb.error.Anomaly
import xtdb.error.Conflict
import xtdb.error.Fault
import xtdb.error.Incorrect
import xtdb.error.Interrupted
import xtdb.error.NotFound
import xtdb.trie.BlockIndex
import xtdb.table.TableRef
import xtdb.util.MsgIdUtil.msgIdToEpoch
import xtdb.util.MsgIdUtil.msgIdToOffset
import xtdb.util.MsgIdUtil.offsetToMsgId
import xtdb.util.StringUtil.asLexHex
import xtdb.util.debug
import xtdb.util.error
import xtdb.util.logger
import java.nio.channels.ClosedByInterruptException
import java.time.Duration
import java.time.Instant
import kotlin.coroutines.cancellation.CancellationException

private val LOG = ReplicaLogProcessor::class.logger

/**
 * Processes all log messages for the replica side of a database.
 * In this first increment, the replica LP is functionally identical to the previous LogProcessor —
 * it handles Tx indexing, block finishing, TriesAdded, etc.
 * It is driven by the source-log subscription forwarding records to it.
 */
class ReplicaLogProcessor @JvmOverloads constructor(
    allocator: BufferAllocator,
    private val dbStorage: DatabaseStorage,
    private val dbState: DatabaseState,
    private val indexer: Indexer.ForDatabase,
    private val compactor: Compactor.ForDatabase,
    flushTimeout: Duration,
    private val skipTxs: Set<MessageId>,
    private val mode: Database.Mode = Database.Mode.READ_WRITE,
    private val maxBufferedRecords: Int = 1024,
    private val dbCatalog: Database.Catalog? = null,
    private val txSource: Indexer.TxSource? = null
) : LogProcessor, Log.Subscriber, AutoCloseable {

    init {
        require((dbCatalog != null) == (dbState.name == "xtdb")) {
            "dbCatalog must be provided iff database is 'xtdb'"
        }
    }

    private val readOnly: Boolean get() = mode == Database.Mode.READ_ONLY

    private val log = dbStorage.sourceLog
    private val epoch = log.epoch
    private val bufferPool = dbStorage.bufferPool

    private val blockCatalog = dbState.blockCatalog
    private val tableCatalog = dbState.tableCatalog
    private val trieCatalog = dbState.trieCatalog
    private val liveIndex = dbState.liveIndex

    private val secondaryDatabases: MutableMap<String, DatabaseConfig> =
        blockCatalog.secondaryDatabases.toMutableMap()

    // Read-only block transition: when the live index is full, we buffer messages
    // until BlockUploaded arrives, then transition and replay.
    private var pendingBlockIdx: BlockIndex? = null
    private val bufferedRecords: MutableList<Log.Record> = mutableListOf()

    @Volatile
    override var latestProcessedMsgId: MessageId =
        blockCatalog.latestProcessedMsgId?.let {
            // used if the epoch is incremented so that we seek to the start of the new log
            if (msgIdToEpoch(it) == epoch) it else offsetToMsgId(epoch, 0) - 1
        } ?: -1
        private set

    override val latestProcessedOffset = blockCatalog.latestProcessedMsgId?.let {
        if (msgIdToEpoch(it) == epoch) msgIdToOffset(it) else -1
    } ?: -1

    override val latestSubmittedMsgId: MessageId
        get() = offsetToMsgId(epoch, log.latestSubmittedOffset)

    private val watchers = Watchers(latestProcessedMsgId)

    override val ingestionError get() = watchers.exception

    private val allocator = allocator.newChildAllocator("log-processor", 0, Long.MAX_VALUE)

    override fun close() {
        allocator.close()
    }

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

    private val flusher = Flusher(flushTimeout, blockCatalog)

    override fun processRecords(records: List<Log.Record>) = runBlocking {
        // Don't send FlushBlock messages in read-only mode - we can't write to the log
        if (!readOnly && flusher.checkBlockTimeout(blockCatalog, liveIndex)) {
            val flushMessage = Message.FlushBlock(blockCatalog.currentBlockIndex ?: -1)
            val offset = log.appendMessage(flushMessage).await().logOffset
            flusher.flushedTxId = offsetToMsgId(epoch, offset)
        }

        val queue = ArrayDeque(records)

        while (queue.isNotEmpty()) {
            val record = queue.removeFirst()
            val msgId = offsetToMsgId(epoch, record.logOffset)

            try {
                if (pendingBlockIdx != null) {
                    val msg = record.message
                    if (msg is Message.BlockUploaded && msg.blockIndex == pendingBlockIdx && msg.storageEpoch == bufferPool.epoch) {
                        doReadOnlyBlockTransition()

                        // Splice buffered records to the front of the queue so they're
                        // processed through the same pendingBlockIdx gate as every other record.
                        // Include the BlockUploaded so it replays through processRecord/watchers.
                        bufferedRecords.add(record)
                        queue.addAll(0, bufferedRecords)
                        bufferedRecords.clear()
                        continue
                    } else {
                        if (bufferedRecords.size >= maxBufferedRecords)
                            throw Fault("read-only buffer overflow: buffered $maxBufferedRecords records waiting for BlockUploaded(b${pendingBlockIdx!!.asLexHex})")

                        bufferedRecords.add(record)
                        continue
                    }
                }

                val res = processRecord(msgId, record)

                // Attach/Detach records share a msgId with the ResolvedTx that follows them
                // (the source emits both); we only notify watchers for the ResolvedTx.
                val msg = record.message
                if (msg !is Message.AttachDatabase && msg !is Message.DetachDatabase)
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
                    "Ingestion stopped for '${dbState.name}' database: error processing log record at id $msgId (epoch: $epoch, logOffset: ${record.logOffset})"
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

    private fun processRecord(msgId: MessageId, record: Log.Record): TransactionResult? {
        LOG.debug("Processing message $msgId, ${record.message.javaClass.simpleName}")

        return when (val msg = record.message) {
                is Message.Tx -> {
                    throw IllegalStateException("Message.Tx should be handled by SourceLogProcessor, not ReplicaLogProcessor")
                }

                is Message.FlushBlock -> {
                    // Source has already finished the block and written it to storage
                    // by the time we see this message (single-threaded forwarding).
                    // We refresh from storage to stay in sync, which also keeps
                    // flush-block! + sync-node working (block transition is inline).
                    val expectedBlockIdx = msg.expectedBlockIdx
                    if (expectedBlockIdx != null && expectedBlockIdx == (blockCatalog.currentBlockIndex ?: -1L)) {
                        if (readOnly) {
                            pendingBlockIdx = expectedBlockIdx + 1
                            LOG.debug("read-only mode: waiting for block 'b${pendingBlockIdx!!.asLexHex}' via BlockUploaded message...")
                        } else {
                            refreshBlockFromStorage()
                        }
                    }
                    null
                }

                is Message.TriesAdded -> {
                    // Source writes TriesAdded to the source log. When the replica
                    // refreshes from storage it calls trieCatalog.refresh() directly,
                    // so we only need this for read-only nodes that haven't refreshed yet.
                    if (msg.storageVersion == Storage.VERSION && msg.storageEpoch == bufferPool.epoch)
                        msg.tries.groupBy { it.tableName }.forEach { (tableName, tries) ->
                            trieCatalog.addTries(TableRef.parse(dbState.name, tableName), tries, record.logTimestamp)
                        }
                    null
                }

                is Message.BlockUploaded -> {
                    // Read-only nodes handle this in the buffering logic above.
                    // Writer nodes: refresh from storage when the source signals a block is done,
                    // but only if we haven't already transitioned (e.g. via isFull() auto-detection).
                    if (!readOnly && msg.storageEpoch == bufferPool.epoch
                        && msg.blockIndex == (blockCatalog.currentBlockIndex ?: -1) + 1) {
                        refreshBlockFromStorage()
                    }
                    null
                }

                is Message.ResolvedTx -> {
                    liveIndex.importTx(msg)

                    txSource?.onCommit(msg)

                    if (liveIndex.isFull()) {
                        if (readOnly) {
                            pendingBlockIdx = (blockCatalog.currentBlockIndex ?: -1) + 1
                            LOG.debug("read-only mode: waiting for block 'b${pendingBlockIdx!!.asLexHex}' via BlockUploaded message...")
                        } else {
                            // Block boundaries are deterministic — the source has already written
                            // the block to storage synchronously before forwarding records to us.
                            refreshBlockFromStorage()
                        }
                    }

                    val systemTime = InstantUtil.fromMicros(msg.systemTimeMicros)
                    if (msg.committed)
                        TransactionCommitted(msg.txId, systemTime)
                    else
                        TransactionAborted(
                            msg.txId, systemTime,
                            readTransit(msg.error, MSGPACK) as Throwable
                        )
                }

                // Source resolves attach/detach as a ResolvedTx that follows this record,
                // so we only handle catalog side-effects here — watchers are notified
                // by the subsequent ResolvedTx, not by these records.

                is Message.AttachDatabase -> {
                    require(dbState.name == "xtdb") { "attach-db on non-primary ${dbState.name}" }
                    if (msg.dbName != "xtdb" && msg.dbName !in secondaryDatabases) {
                        secondaryDatabases[msg.dbName] = msg.config.serializedConfig
                        dbCatalog!!.attach(msg.dbName, msg.config)
                    }
                    null
                }

                is Message.DetachDatabase -> {
                    require(dbState.name == "xtdb") { "detach-db on non-primary ${dbState.name}" }
                    if (msg.dbName != "xtdb" && msg.dbName in secondaryDatabases) {
                        secondaryDatabases.remove(msg.dbName)
                        dbCatalog!!.detach(msg.dbName)
                    }
                    null
                }
            }.also {
                latestProcessedMsgId = msgId
                LOG.debug("Processed message $msgId")
            }
    }

    private fun doReadOnlyBlockTransition() {
        val blockIdx = pendingBlockIdx!!
        LOG.debug("read-only mode: received BlockUploaded for block 'b${blockIdx.asLexHex}', transitioning...")

        val blockFile = bufferPool.allBlockFiles
            .find { BlockCatalog.blockFilePath(blockIdx) == it.key }
            ?: throw Fault("read-only mode: block file for 'b${blockIdx.asLexHex}' not found in object store")

        val block = Block.parseFrom(bufferPool.getByteArray(blockFile.key))
        blockCatalog.refresh(block)
        tableCatalog.refresh()
        trieCatalog.refresh()
        liveIndex.nextBlock()

        pendingBlockIdx = null
        LOG.debug("read-only mode: transitioned to block 'b${blockIdx.asLexHex}'")
    }

    /**
     * Refreshes replica catalogs from the block file in storage (written by the source),
     * then clears the replica's live index. Similar to the read-only block transition
     * but without buffering — the source writes synchronously before forwarding records.
     */
    private fun refreshBlockFromStorage() {
        val blockIdx = (blockCatalog.currentBlockIndex ?: -1) + 1
        LOG.debug("refreshing block from storage: 'b${blockIdx.asLexHex}'...")

        val block = Block.parseFrom(bufferPool.getByteArray(BlockCatalog.blockFilePath(blockIdx)))
        blockCatalog.refresh(block)
        tableCatalog.refresh()
        trieCatalog.refresh()
        liveIndex.nextBlock()
        compactor.signalBlock()
        LOG.debug("refreshed block: 'b${blockIdx.asLexHex}'.")
    }

    fun ingestionStopped(msgId: MessageId, e: Throwable) {
        watchers.notify(msgId, e)
    }

    override fun awaitAsync(msgId: MessageId) = watchers.awaitAsync(msgId)
}
