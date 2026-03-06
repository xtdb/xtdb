package xtdb.indexer

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import org.apache.arrow.memory.BufferAllocator
import xtdb.api.TransactionAborted
import xtdb.api.TransactionCommitted
import xtdb.api.TransactionKey
import xtdb.api.log.Log
import xtdb.api.log.MessageId
import xtdb.api.log.ReplicaMessage
import xtdb.api.log.SourceMessage
import xtdb.api.log.Watchers
import xtdb.api.storage.Storage
import xtdb.arrow.Relation
import xtdb.arrow.asChannel
import xtdb.catalog.BlockCatalog
import xtdb.compactor.Compactor
import xtdb.database.Database
import xtdb.database.DatabaseState
import xtdb.database.DatabaseStorage
import xtdb.error.Anomaly
import xtdb.log.proto.TrieDetails
import xtdb.table.TableRef
import xtdb.util.StringUtil.asLexDec
import xtdb.util.StringUtil.asLexHex
import xtdb.util.asPath
import xtdb.util.debug
import xtdb.util.logger
import xtdb.util.warn
import java.nio.ByteBuffer
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime

private val LOG = SourceLogProcessor::class.logger

/**
 * Subscribes to the source log and resolves SourceMessage.Tx records into ReplicaMessage.ResolvedTx.
 * Also owns block finishing: when the source LiveIndex is full, writes tries, table blocks,
 * and the block file to storage, then appends TriesAdded + BlockUploaded to the source log.
 *
 * Post-processing (watchers notification, attach/detach, compactor signalling)
 * is handled inline after resolving each batch of records.
 */
class SourceLogProcessor(
    allocator: BufferAllocator,
    meterRegistry: MeterRegistry,
    private val dbStorage: DatabaseStorage,
    private val dbState: DatabaseState,
    private val indexer: Indexer.ForDatabase,
    private val liveIndex: LiveIndex,
    private val watchers: Watchers,
    private val compactor: Compactor.ForDatabase,
    private val skipTxs: Set<MessageId>,
    private val readOnly: Boolean = false,
    private val dbCatalog: Database.Catalog? = null,
    flushTimeout: Duration = Duration.ofMinutes(5),
) : Log.RecordProcessor<SourceMessage>, AutoCloseable {

    init {
        require((dbCatalog != null) == (dbState.name == "xtdb")) {
            "dbCatalog must be provided iff database is 'xtdb'"
        }
    }

    private val log = dbStorage.sourceLog
    private val bufferPool = dbStorage.bufferPool

    private val blockCatalog = dbState.blockCatalog
    private val trieCatalog = dbState.trieCatalog
    private val tableCatalog = dbState.tableCatalog

    private var latestProcessedMsgId: MessageId = blockCatalog.latestProcessedMsgId ?: -1

    // Read-only block transition: when the live index is full, we buffer messages
    // until BlockUploaded arrives from the writer, then refresh catalogs and replay.
    private var pendingBlockIdx: Long? = null
    private val bufferedRecords: MutableList<Log.Record<SourceMessage>> = mutableListOf()

    private val allocator =
        allocator.newChildAllocator("log-processor", 0, Long.MAX_VALUE)
            .also { alloc ->
                Gauge.builder("watcher.allocator.allocated_memory", alloc) { it.allocatedMemory.toDouble() }
                    .baseUnit("bytes")
                    .register(meterRegistry)
            }

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

    private fun resolveTx(msgId: MessageId, record: Log.Record<SourceMessage>, msg: SourceMessage.Tx): ReplicaMessage.ResolvedTx {
        return if (skipTxs.isNotEmpty() && skipTxs.contains(msgId)) {
            LOG.warn("Skipping transaction id $msgId - within XTDB_SKIP_TXS")

            val skippedTxPath = "skipped-txs/${msgId.asLexDec}".asPath
            bufferPool.putObject(skippedTxPath, ByteBuffer.wrap(msg.payload))

            indexer.indexTx(msgId, record.logTimestamp, null, null, null, null, null)
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

                        val userMetadata = rel.vectorForOrNull("user-metadata")?.getObject(0)

                        indexer.indexTx(
                            msgId, record.logTimestamp,
                            rel["tx-ops"].listElements,
                            systemTime, defaultTz, user, userMetadata
                        )
                    }
                }
            }
        }
    }

    private fun finishBlock(systemTime: Instant) {
        val blockIdx = (blockCatalog.currentBlockIndex ?: -1) + 1
        LOG.debug("finishing block: 'b${blockIdx.asLexHex}'...")

        val finishedBlocks = liveIndex.finishBlock(blockIdx)

        val addedTries = finishedBlocks.map { (table, fb) ->
            TrieDetails.newBuilder()
                .setTableName(table.schemaAndTable)
                .setTrieKey(fb.trieKey)
                .setDataFileSize(fb.dataFileSize)
                .also { fb.trieMetadata.let { tm -> it.setTrieMetadata(tm) } }
                .build()
        }
        log.appendMessage(
            SourceMessage.TriesAdded(Storage.VERSION, bufferPool.epoch, addedTries)
        )

        finishedBlocks.forEach { (table, _) ->
            val trie = addedTries.find { it.tableName == table.schemaAndTable }!!
            trieCatalog.addTries(table, listOf(trie), systemTime)
        }

        val allTables = finishedBlocks.keys + blockCatalog.allTables
        val tablePartitions = allTables.associateWith { trieCatalog.getPartitions(it) }

        val tableBlocks = tableCatalog.finishBlock(finishedBlocks, tablePartitions)

        for ((table, tableBlock) in tableBlocks) {
            val path = BlockCatalog.tableBlockPath(table, blockIdx)
            bufferPool.putObject(path, ByteBuffer.wrap(tableBlock.toByteArray()))
        }

        val secondaryDatabasesForBlock = dbCatalog?.serialisedSecondaryDatabases

        val block = blockCatalog.buildBlock(
            blockIdx, liveIndex.latestCompletedTx, latestProcessedMsgId,
            tableBlocks.keys, secondaryDatabasesForBlock
        )

        bufferPool.putObject(BlockCatalog.blockFilePath(blockIdx), ByteBuffer.wrap(block.toByteArray()))
        blockCatalog.refresh(block)

        // NOTE: we still send TriesAdded separately above for backwards compat
        log.appendMessage(SourceMessage.BlockUploaded(Storage.VERSION, bufferPool.epoch, blockIdx, latestProcessedMsgId, addedTries))

        liveIndex.nextBlock()
        compactor.signalBlock()
        LOG.debug("finished block: 'b${blockIdx.asLexHex}'.")
    }

    private fun enterPendingBlock() {
        val blockIdx = (blockCatalog.currentBlockIndex ?: -1) + 1
        pendingBlockIdx = blockIdx
        liveIndex.nextBlock()
        LOG.debug("read-only: waiting for block 'b${blockIdx.asLexHex}' via BlockUploaded...")
    }

    override fun processRecords(records: List<Log.Record<SourceMessage>>) {
        if (!readOnly && flusher.checkBlockTimeout(blockCatalog, liveIndex)) {
            val flushMessage = SourceMessage.FlushBlock(blockCatalog.currentBlockIndex ?: -1)
            flusher.flushedTxId = log.appendMessage(flushMessage).get().msgId
        }

        var lastMsgId: MessageId = latestProcessedMsgId
        val queue = ArrayDeque(records)

        try {
            while (queue.isNotEmpty()) {
                val record = queue.removeFirst()
                val msgId = record.msgId
                lastMsgId = msgId
                latestProcessedMsgId = msgId

                if (pendingBlockIdx != null) {
                    val msg = record.message
                    if (msg is SourceMessage.BlockUploaded
                        && msg.blockIndex == pendingBlockIdx
                        && msg.storageEpoch == bufferPool.epoch
                    ) {
                        compactor.signalBlock()
                        pendingBlockIdx = null

                        // Replay buffered records (including this BlockUploaded).
                        bufferedRecords.add(record)
                        queue.addAll(0, bufferedRecords)
                        bufferedRecords.clear()
                        continue
                    } else {
                        bufferedRecords.add(record)
                        continue
                    }
                }

                when (val msg = record.message) {
                    is SourceMessage.Tx -> {
                        val resolvedTx = resolveTx(msgId, record, msg)
                        notifyTx(msgId, resolvedTx)

                        if (liveIndex.isFull()) {
                            if (readOnly) enterPendingBlock()
                            else finishBlock(record.logTimestamp)
                        }
                    }

                    is SourceMessage.FlushBlock -> {
                        val expectedBlockIdx = msg.expectedBlockIdx
                        if (expectedBlockIdx != null && expectedBlockIdx == (blockCatalog.currentBlockIndex ?: -1L)) {
                            if (readOnly) enterPendingBlock()
                            else finishBlock(record.logTimestamp)
                        }
                    }

                    is SourceMessage.AttachDatabase -> {
                        val txKey = TransactionKey(msgId, record.logTimestamp)
                        val error = try {
                            dbCatalog!!.attach(msg.dbName, msg.config)
                            null
                        } catch (e: Anomaly.Caller) { e }

                        indexer.addTxRow(txKey, error)

                        if (error == null) {
                            watchers.notify(msgId, TransactionCommitted(txKey.txId, txKey.systemTime))
                        } else {
                            watchers.notify(msgId, TransactionAborted(txKey.txId, txKey.systemTime, error))
                        }
                    }

                    is SourceMessage.DetachDatabase -> {
                        val txKey = TransactionKey(msgId, record.logTimestamp)
                        val error = try {
                            dbCatalog!!.detach(msg.dbName)
                            null
                        } catch (e: Anomaly.Caller) { e }

                        indexer.addTxRow(txKey, error)

                        if (error == null) {
                            watchers.notify(msgId, TransactionCommitted(txKey.txId, txKey.systemTime))
                        } else {
                            watchers.notify(msgId, TransactionAborted(txKey.txId, txKey.systemTime, error))
                        }
                    }

                    is SourceMessage.TriesAdded -> {
                        if (msg.storageVersion == Storage.VERSION && msg.storageEpoch == bufferPool.epoch)
                            msg.tries.groupBy { it.tableName }.forEach { (tableName, tries) ->
                                trieCatalog.addTries(TableRef.parse(dbState.name, tableName), tries, record.logTimestamp)
                            }
                        watchers.notify(msgId, null)
                    }

                    is SourceMessage.BlockUploaded -> {
                        watchers.notify(msgId, null)
                    }
                }
            }
        } catch (e: Throwable) {
            watchers.notify(lastMsgId, e)
            throw e
        }
    }

    private fun notifyTx(msgId: MessageId, resolvedTx: ReplicaMessage.ResolvedTx) {
        val result = if (resolvedTx.committed) {
            TransactionCommitted(resolvedTx.txId, resolvedTx.systemTime)
        } else {
            TransactionAborted(resolvedTx.txId, resolvedTx.systemTime, resolvedTx.error!!)
        }

        watchers.notify(msgId, result)
    }
}
