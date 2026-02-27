package xtdb.indexer

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.runBlocking
import org.apache.arrow.memory.BufferAllocator
import xtdb.api.TransactionAborted
import xtdb.api.TransactionCommitted
import xtdb.api.TransactionResult
import xtdb.api.log.DbOp
import xtdb.api.log.Log
import xtdb.api.log.MessageId
import xtdb.api.log.ReplicaMessage
import xtdb.api.log.Watchers
import xtdb.api.storage.Storage
import xtdb.block.proto.Block
import xtdb.catalog.BlockCatalog
import xtdb.catalog.BlockCatalog.Companion.allBlockFiles
import xtdb.compactor.Compactor
import xtdb.database.Database
import xtdb.database.DatabaseState
import xtdb.database.DatabaseStorage
import xtdb.database.proto.DatabaseConfig
import xtdb.error.Fault
import xtdb.error.Interrupted
import xtdb.table.TableRef
import xtdb.time.InstantUtil
import xtdb.trie.BlockIndex
import xtdb.util.MsgIdUtil.msgIdToEpoch
import xtdb.util.MsgIdUtil.offsetToMsgId
import xtdb.util.StringUtil.asLexHex
import xtdb.util.TransitFormat.MSGPACK
import xtdb.util.debug
import xtdb.util.error
import xtdb.util.logger
import xtdb.util.readTransit
import java.nio.channels.ClosedByInterruptException
import kotlin.coroutines.cancellation.CancellationException

private val LOG = ReplicaLogProcessor::class.logger

/**
 * Processes all log messages for the replica side of a database.
 * In this first increment, the replica LP is functionally identical to the previous log processor —
 * it handles Tx indexing, block finishing, TriesAdded, etc.
 * It is driven by the source-log subscription forwarding records to it.
 */
class ReplicaLogProcessor @JvmOverloads constructor(
    allocator: BufferAllocator,
    meterRegistry: MeterRegistry,
    private val dbStorage: DatabaseStorage,
    private val dbState: DatabaseState,
    private val indexer: Indexer.ForDatabase,
    private val liveIndex: LiveIndex,
    private val compactor: Compactor.ForDatabase,
    private val skipTxs: Set<MessageId>,
    private val watchers: Watchers,
    private val maxBufferedRecords: Int = 1024,
    private val dbCatalog: Database.Catalog? = null,
    private val txSource: Indexer.TxSource? = null
) : Log.Subscriber<ReplicaMessage>, AutoCloseable {

    init {
        require((dbCatalog != null) == (dbState.name == "xtdb")) {
            "dbCatalog must be provided iff database is 'xtdb'"
        }
    }

    private val log = dbStorage.sourceLog
    private val epoch = log.epoch
    private val bufferPool = dbStorage.bufferPool

    private val blockCatalog = dbState.blockCatalog
    private val tableCatalog = dbState.tableCatalog
    private val trieCatalog = dbState.trieCatalog

    private val secondaryDatabases: MutableMap<String, DatabaseConfig> =
        blockCatalog.secondaryDatabases.toMutableMap()

    // Read-only block transition: when the live index is full, we buffer messages
    // until BlockUploaded arrives, then transition and replay.
    private var pendingBlockIdx: BlockIndex? = null
    private val bufferedRecords: MutableList<Log.Record<ReplicaMessage>> = mutableListOf()

    @Volatile
    private var latestProcessedMsgId: MessageId =
        blockCatalog.latestProcessedMsgId?.let {
            // used if the epoch is incremented so that we seek to the start of the new log
            if (msgIdToEpoch(it) == epoch) it else offsetToMsgId(epoch, 0) - 1
        } ?: -1

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

    override fun processRecords(records: List<Log.Record<ReplicaMessage>>) = runBlocking {
        val queue = ArrayDeque(records)

        while (queue.isNotEmpty()) {
            val record = queue.removeFirst()
            val msgId = offsetToMsgId(epoch, record.logOffset)

            try {
                if (pendingBlockIdx != null) {
                    val msg = record.message
                    if (msg is ReplicaMessage.BlockUploaded && msg.blockIndex == pendingBlockIdx && msg.storageEpoch == bufferPool.epoch) {
                        doBlockTransition()

                        // Splice buffered records to the front of the queue so they're
                        // processed through the same pendingBlockIdx gate as every other record.
                        // Include the BlockUploaded so it replays through processRecord/watchers.
                        bufferedRecords.add(record)
                        queue.addAll(0, bufferedRecords)
                        bufferedRecords.clear()
                        continue
                    } else {
                        if (bufferedRecords.size >= maxBufferedRecords)
                            throw Fault("buffer overflow: buffered $maxBufferedRecords records waiting for BlockUploaded(b${pendingBlockIdx!!.asLexHex})")

                        bufferedRecords.add(record)
                        continue
                    }
                }

                val res = processRecord(msgId, record)

                when (record.message) {
                    is ReplicaMessage.ResolvedTx, is ReplicaMessage.BlockUploaded, is ReplicaMessage.TriesAdded ->
                        watchers.notify(msgId, res)
                    is ReplicaMessage.BlockBoundary -> {}
                }
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

    private fun processRecord(msgId: MessageId, record: Log.Record<ReplicaMessage>): TransactionResult? {
        LOG.debug("Processing message $msgId, ${record.message.javaClass.simpleName}")

        return when (val msg = record.message) {
                is ReplicaMessage.BlockBoundary -> {
                    pendingBlockIdx = msg.blockIndex
                    LOG.debug("waiting for block 'b${pendingBlockIdx!!.asLexHex}' via BlockUploaded message...")
                    null
                }

                is ReplicaMessage.TriesAdded -> {
                    // Source writes TriesAdded to the source log. When the replica
                    // refreshes from storage it calls trieCatalog.refresh() directly,
                    // so we only need this for read-only nodes that haven't refreshed yet.
                    if (msg.storageVersion == Storage.VERSION && msg.storageEpoch == bufferPool.epoch)
                        msg.tries.groupBy { it.tableName }.forEach { (tableName, tries) ->
                            trieCatalog.addTries(TableRef.parse(dbState.name, tableName), tries, record.logTimestamp)
                        }
                    null
                }

                is ReplicaMessage.ResolvedTx -> {
                    // TODO: importTx disabled — source processor writes to the shared LiveIndex directly via startTx.
                    // liveIndex.importTx(msg)

                    txSource?.onCommit(msg)

                    val systemTime = InstantUtil.fromMicros(msg.systemTimeMicros)

                    if (msg.committed) {
                        // Handle attach/detach catalog side-effects encoded in the ResolvedTx
                        when (val dbOp = msg.dbOp) {
                            is DbOp.Attach -> {
                                secondaryDatabases[dbOp.dbName] = dbOp.config.serializedConfig
                                dbCatalog!!.attach(dbOp.dbName, dbOp.config)
                            }
                            is DbOp.Detach -> {
                                secondaryDatabases.remove(dbOp.dbName)
                                dbCatalog!!.detach(dbOp.dbName)
                            }
                            null -> {}
                        }

                        TransactionCommitted(msg.txId, systemTime)
                    } else {
                        TransactionAborted(
                            msg.txId, systemTime,
                            readTransit(msg.error, MSGPACK) as Throwable
                        )
                    }
                }

                // Block transitions are handled by the pending-block gate in processRecords.
                is ReplicaMessage.BlockUploaded -> null
            }.also {
                latestProcessedMsgId = msgId
                LOG.debug("Processed message $msgId")
            }
    }

    private fun doBlockTransition() {
        val blockIdx = pendingBlockIdx!!
        LOG.debug("received BlockUploaded for block 'b${blockIdx.asLexHex}', transitioning...")

        val blockFile = bufferPool.allBlockFiles
            .find { BlockCatalog.blockFilePath(blockIdx) == it.key }
            ?: throw Fault("block file for 'b${blockIdx.asLexHex}' not found in object store")

        val block = Block.parseFrom(bufferPool.getByteArray(blockFile.key))
        blockCatalog.refresh(block)
        tableCatalog.refresh(blockIdx)
        trieCatalog.refresh(blockIdx)
        // TODO: nextBlock disabled — source processor calls nextBlock on the shared LiveIndex directly.
        // liveIndex.nextBlock()
        compactor.signalBlock()

        pendingBlockIdx = null
        LOG.debug("transitioned to block 'b${blockIdx.asLexHex}'")
    }


    fun ingestionStopped(msgId: MessageId, e: Throwable) {
        watchers.notify(msgId, e)
    }

}
