package xtdb.indexer

import org.apache.arrow.memory.BufferAllocator
import xtdb.api.TransactionAborted
import xtdb.api.TransactionCommitted
import xtdb.api.TransactionResult
import xtdb.api.log.*
import xtdb.api.storage.Storage
import xtdb.block.proto.Block.parseFrom
import xtdb.catalog.BlockCatalog.Companion.allBlockFiles
import xtdb.compactor.Compactor
import xtdb.database.Database
import xtdb.database.DatabaseState
import xtdb.error.Fault
import xtdb.error.Interrupted
import xtdb.log.proto.TrieDetails
import xtdb.storage.BufferPool
import xtdb.table.TableRef
import xtdb.util.StringUtil.asLexHex
import xtdb.util.debug
import xtdb.util.error
import xtdb.util.logger
import xtdb.util.trace

private val LOG = FollowerLogProcessor::class.logger

class FollowerLogProcessor @JvmOverloads constructor(
    allocator: BufferAllocator,
    private val bufferPool: BufferPool,
    private val dbState: DatabaseState,
    private val compactor: Compactor.ForDatabase,
    private val sourceWatchers: Watchers,
    private val replicaWatchers: Watchers,
    private val dbCatalog: Database.Catalog?,
    private val maxBufferedRecords: Int = 1024,
) : LogProcessor.FollowerProcessor {

    inner class PendingBlock(val boundaryMsgId: MessageId, val boundaryMessage: ReplicaMessage.BlockBoundary) {
        val blockIdx get() = boundaryMessage.blockIndex
        private val _bufferedRecords = mutableListOf<Log.Record<ReplicaMessage>>()
        val bufferedRecords: List<Log.Record<ReplicaMessage>> get() = _bufferedRecords

        operator fun plusAssign(record: Log.Record<ReplicaMessage>) {
            if (_bufferedRecords.size >= maxBufferedRecords) {
                throw Fault(
                    "Follower buffer overflow: buffered $maxBufferedRecords records while waiting for BlockUploaded(b${blockIdx.asLexHex})",
                    "xtdb.indexer/follower-buffer-overflow",
                    mapOf("pending-block-idx" to blockIdx, "max-buffered-records" to maxBufferedRecords)
                )
            }
            _bufferedRecords += record
        }
    }

    private val blockCatalog = dbState.blockCatalog
    private val trieCatalog = dbState.trieCatalog
    private val liveIndex = dbState.liveIndex

    private val allocator = allocator.newChildAllocator("follower-log-processor", 0, Long.MAX_VALUE)

    override var pendingBlock: PendingBlock? = null

    private fun addTries(tries: List<TrieDetails>, logTimestamp: LogTimestamp) {
        tries.groupBy { it.tableName }.forEach { (tableName, tries) ->
            trieCatalog.addTries(TableRef.parse(dbState.name, tableName), tries, logTimestamp)
        }
    }

    private fun handleRecord(record: Log.Record<ReplicaMessage>): TransactionResult? {
        val msg = record.message
        LOG.trace { "follower: message ${record.msgId} (${msg::class.simpleName})" }

        pendingBlock?.let { pendingBlock ->
            val pendingBlockIdx = pendingBlock.blockIdx
            if (msg is ReplicaMessage.BlockUploaded
                && msg.blockIndex == pendingBlockIdx
                && msg.storageEpoch == bufferPool.epoch
            ) {
                LOG.debug("follower: block transition for 'b${msg.blockIndex.asLexHex}'")
                val blockFile = bufferPool.allBlockFiles.lastOrNull()
                val block = blockFile?.key?.let { parseFrom(bufferPool.getByteArray(it)) }

                blockCatalog.refresh(block)
                liveIndex.nextBlock()
                compactor.signalBlock()

                val bufferedRecords = pendingBlock.bufferedRecords
                this.pendingBlock = null

                // replay buffered records — already notified to replicaWatchers when first consumed
                for (buffered in bufferedRecords) {
                    handleRecord(buffered)
                }

                return null
            } else {
                LOG.trace { "follower: buffering message ${record.msgId} (${msg::class.simpleName}) during pending block b${pendingBlockIdx} (${pendingBlock.bufferedRecords.size + 1} buffered)" }
                pendingBlock += record
                return null
            }
        }

        return when (msg) {
            is ReplicaMessage.ResolvedTx -> {
                val latestTxId = liveIndex.latestCompletedTx?.txId
                if (latestTxId != null && msg.txId <= latestTxId) return null

                liveIndex.importTx(msg)

                val systemTime = msg.systemTime
                val result = if (msg.committed) {
                    when (val dbOp = msg.dbOp) {
                        is DbOp.Attach -> dbCatalog!!.attach(dbOp.dbName, dbOp.config)
                        is DbOp.Detach -> dbCatalog!!.detach(dbOp.dbName)
                        null -> {}
                    }

                    TransactionCommitted(msg.txId, systemTime)
                } else TransactionAborted(msg.txId, systemTime, msg.error)

                sourceWatchers.notify(msg.txId, result)
                result
            }

            is ReplicaMessage.TriesAdded -> {
                if (msg.storageVersion == Storage.VERSION && msg.storageEpoch == bufferPool.epoch)
                    addTries(msg.tries, record.logTimestamp)
                null
            }

            is ReplicaMessage.BlockBoundary -> {
                pendingBlock = PendingBlock(record.msgId, msg)
                LOG.debug("follower: waiting for block 'b${msg.blockIndex.asLexHex}' via BlockUploaded...")
                null
            }

            is ReplicaMessage.BlockUploaded -> {
                if (msg.storageVersion == Storage.VERSION && msg.storageEpoch == bufferPool.epoch)
                    addTries(msg.tries, record.logTimestamp)
                null
            }

            is ReplicaMessage.NoOp -> null
        }
    }

    override fun processRecords(records: List<Log.Record<ReplicaMessage>>) {
        for (record in records) {
            try {
                val res = handleRecord(record)
                replicaWatchers.notify(record.msgId, res)
            } catch (e: InterruptedException) {
                throw e
            } catch (e: Interrupted) {
                throw e
            } catch (e: Throwable) {
                LOG.error(e, "follower: failed to process log record with msgId ${record.msgId} (${record.message::class.simpleName})")
                sourceWatchers.notify(record.msgId, e) // strictly speaking, a replica msgId.
                replicaWatchers.notify(record.msgId, e)
                throw e
            }
        }
    }

    override fun close() {
        allocator.close()
    }
}
