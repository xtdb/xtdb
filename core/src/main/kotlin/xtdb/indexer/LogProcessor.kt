package xtdb.indexer

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.future.await
import kotlinx.coroutines.runBlocking
import org.apache.arrow.memory.BufferAllocator
import xtdb.api.TransactionAborted
import xtdb.api.TransactionCommitted
import xtdb.api.TransactionKey
import xtdb.api.TransactionResult
import xtdb.api.log.Log
import xtdb.api.log.Log.Message
import xtdb.api.log.MessageId
import xtdb.api.log.Watchers
import xtdb.api.storage.Storage
import xtdb.arrow.Relation
import xtdb.arrow.asChannel
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
import xtdb.log.proto.TrieDetails
import xtdb.trie.BlockIndex
import xtdb.table.TableRef
import xtdb.util.MsgIdUtil.msgIdToEpoch
import xtdb.util.MsgIdUtil.msgIdToOffset
import xtdb.util.MsgIdUtil.offsetToMsgId
import xtdb.util.StringUtil.asLexDec
import xtdb.util.StringUtil.asLexHex
import xtdb.util.asPath
import xtdb.util.debug
import xtdb.util.error
import xtdb.util.logger
import xtdb.util.warn
import java.nio.ByteBuffer
import java.nio.channels.ClosedByInterruptException
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime
import kotlin.coroutines.cancellation.CancellationException

private val LOG = LogProcessor::class.logger

class LogProcessor @JvmOverloads constructor(
    allocator: BufferAllocator,
    meterRegistry: MeterRegistry,
    private val dbStorage: DatabaseStorage,
    private val dbState: DatabaseState,
    private val indexer: Indexer.ForDatabase,
    private val compactor: Compactor.ForDatabase,
    flushTimeout: Duration,
    private val skipTxs: Set<MessageId>,
    private val mode: Database.Mode = Database.Mode.READ_WRITE,
    private val maxBufferedRecords: Int = 1024
) : Log.Subscriber, AutoCloseable {

    private val readOnly: Boolean get() = mode == Database.Mode.READ_ONLY

    // LogProcessor subscribes to the source-log, which contains transaction messages.
    // For now, both source-log and replica-log point to the same underlying log,
    // so we still receive TriesAdded messages here even though they're written to replica-log.
    // This is intentional for the transition period.
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
    var latestProcessedMsgId: MessageId =
        blockCatalog.latestProcessedMsgId?.let {
            // used if the epoch is incremented so that we seek to the start of the new log
            if (msgIdToEpoch(it) == epoch) it else offsetToMsgId(epoch, 0) - 1
        } ?: -1
        private set

    val latestProcessedOffset = blockCatalog.latestProcessedMsgId?.let {
        if (msgIdToEpoch(it) == epoch) msgIdToOffset(it) else -1
    } ?: -1

    val latestSubmittedMsgId: MessageId
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
        allocator.close()
    }

    private val flusher = Flusher(flushTimeout, blockCatalog)

    override fun processRecords(records: List<Log.Record>) = runBlocking {
        // Don't send FlushBlock messages in read-only mode - we can't write to the log
        if (!readOnly && flusher.checkBlockTimeout(blockCatalog, liveIndex)) {
            val flushMessage = Message.FlushBlock(blockCatalog.currentBlockIndex ?: -1)
            val offset = log.appendMessage(flushMessage).await().logOffset
            flusher.flushedTxId = offsetToMsgId(epoch, offset)
        }

        for (record in records) {
            val msgId = offsetToMsgId(epoch, record.logOffset)

            try {
                if (pendingBlockIdx != null) {
                    // We're waiting for a BlockUploaded message — check if this is it
                    val msg = record.message
                    if (msg is Message.BlockUploaded && msg.blockIndex == pendingBlockIdx && msg.storageEpoch == bufferPool.epoch) {
                        doReadOnlyBlockTransition()
                        replayBufferedRecords()
                        // Fall through to process this record's successors normally
                    } else {
                        if (bufferedRecords.size >= maxBufferedRecords)
                            throw Fault("read-only buffer overflow: buffered $maxBufferedRecords records waiting for BlockUploaded(b${pendingBlockIdx!!.asLexHex})")

                        bufferedRecords.add(record)
                        continue
                    }
                }

                val res = processRecord(msgId, record)
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
        var toFinishBlock = false
        LOG.debug("Processing message $msgId, ${record.message.javaClass.simpleName}")

        return when (val msg = record.message) {
                is Message.Tx -> {
                    val result = if (skipTxs.isNotEmpty() && skipTxs.contains(msgId)) {
                        LOG.warn("Skipping transaction id $msgId - within XTDB_SKIP_TXS")

                        // Store skipped transaction to object store
                        val skippedTxPath = "skipped-txs/${msgId.asLexDec}".asPath
                        bufferPool.putObject(skippedTxPath, ByteBuffer.wrap(msg.payload))
                        LOG.debug("Stored skipped transaction to $skippedTxPath")

                        // use abort flow in indexTx
                        indexer.indexTx(
                            msgId, record.logTimestamp,
                            null, null, null, null, null
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

                    if (liveIndex.isFull())
                        toFinishBlock = true

                    result
                }

                is Message.FlushBlock -> {
                    val expectedBlockIdx = msg.expectedBlockIdx
                    if (expectedBlockIdx != null && expectedBlockIdx == (blockCatalog.currentBlockIndex ?: -1L))
                        toFinishBlock = true

                    null
                }

                is Message.TriesAdded -> {
                    if (msg.storageVersion == Storage.VERSION && msg.storageEpoch == bufferPool.epoch)
                        msg.tries.groupBy { it.tableName }.forEach { (tableName, tries) ->
                            trieCatalog.addTries(TableRef.parse(dbState.name, tableName), tries, record.logTimestamp)
                        }
                    null
                }

                is Message.BlockUploaded -> {
                    // Handled in the buffering logic above; if we reach here
                    // it means we're not waiting for a block (e.g. writer node), so ignore.
                    null
                }

                is Message.AttachDatabase -> {
                    require(dbState.name == "xtdb") { "attach-db on non-primary ${dbState.name}" }
                    val res = try {
                        if (msg.dbName == "xtdb" || msg.dbName in secondaryDatabases)
                            throw Conflict("Database already exists", "xtdb/db-exists", mapOf("db-name" to msg.dbName))
                        secondaryDatabases[msg.dbName] = msg.config.serializedConfig
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
                    require(dbState.name == "xtdb") { "detach-db on non-primary ${dbState.name}" }
                    val res = try {
                        when {
                            msg.dbName == "xtdb" ->
                                throw Incorrect("Cannot detach the primary 'xtdb' database", "xtdb/cannot-detach-primary", mapOf("db-name" to msg.dbName))
                            msg.dbName !in secondaryDatabases ->
                                throw NotFound("Database does not exist", "xtdb/no-such-db", mapOf("db-name" to msg.dbName))
                            else -> {
                                secondaryDatabases.remove(msg.dbName)
                                TransactionCommitted(msgId, record.logTimestamp)
                            }
                        }
                    } catch (e: Anomaly.Caller) {
                        TransactionAborted(msgId, record.logTimestamp, e)
                    }

                    indexer.addTxRow(
                        TransactionKey(msgId, record.logTimestamp),
                        (res as? TransactionAborted)?.error
                    )

                    res
                }
            }.also {
                latestProcessedMsgId = msgId
                if (toFinishBlock) {
                    if (readOnly) {
                        pendingBlockIdx = (blockCatalog.currentBlockIndex ?: -1) + 1
                        LOG.debug("read-only mode: waiting for block 'b${pendingBlockIdx!!.asLexHex}' via BlockUploaded message...")
                    } else {
                        finishBlock(record.logTimestamp)
                    }
                }

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

    private fun replayBufferedRecords() {
        val toReplay = bufferedRecords.toList()
        bufferedRecords.clear()
        for (record in toReplay) {
            val msgId = offsetToMsgId(epoch, record.logOffset)
            val res = processRecord(msgId, record)
            watchers.notify(msgId, res)
        }
    }

    fun finishBlock(systemTime: Instant) {
        val blockIdx = (blockCatalog.currentBlockIndex ?: -1) + 1
        LOG.debug("finishing block: 'b${blockIdx.asLexHex}'...")

        val finishedBlocks = liveIndex.finishBlock(blockIdx)

        // Build TrieDetails and append to replica-log
        val addedTries = finishedBlocks.map { (table, fb) ->
            TrieDetails.newBuilder()
                .setTableName(table.schemaAndTable)
                .setTrieKey(fb.trieKey)
                .setDataFileSize(fb.dataFileSize)
                .also { fb.trieMetadata.let { tm -> it.setTrieMetadata(tm) } }
                .build()
        }
        dbStorage.replicaLog.appendMessage(
            Message.TriesAdded(Storage.VERSION, bufferPool.epoch, addedTries)
        )

        // Add tries to trie catalog
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

        val secondaryDatabasesForBlock = secondaryDatabases.takeIf { dbState.name == "xtdb" }

        val block = blockCatalog.buildBlock(
            blockIdx, liveIndex.latestCompletedTx, latestProcessedMsgId,
            tableBlocks.keys, secondaryDatabasesForBlock
        )

        bufferPool.putObject(BlockCatalog.blockFilePath(blockIdx), ByteBuffer.wrap(block.toByteArray()))
        blockCatalog.refresh(block)

        // Notify read-only nodes that the block is available
        log.appendMessage(Message.BlockUploaded(blockIdx, latestProcessedMsgId, bufferPool.epoch))

        liveIndex.nextBlock()
        compactor.signalBlock()
        LOG.debug("finished block: 'b${blockIdx.asLexHex}'.")
    }

    @JvmOverloads
    fun awaitAsync(msgId: MessageId = latestSubmittedMsgId) = watchers.awaitAsync(msgId)
}
