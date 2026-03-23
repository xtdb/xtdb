package xtdb.indexer

import org.apache.arrow.memory.BufferAllocator
import xtdb.api.TransactionAborted
import xtdb.api.TransactionCommitted
import xtdb.api.log.*
import xtdb.api.storage.Storage
import xtdb.block.proto.Block.parseFrom
import xtdb.catalog.BlockCatalog.Companion.allBlockFiles
import xtdb.compactor.Compactor
import xtdb.database.Database
import xtdb.database.DatabaseState
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
    private val watchers: Watchers,
    private val dbCatalog: Database.Catalog?,
    pendingBlock: PendingBlock?,
    afterSourceMsgId: MessageId,
    private val maxBufferedRecords: Int = 1024,
) : LogProcessor.FollowerProcessor {

    override var pendingBlock: PendingBlock? = pendingBlock
        private set

    override var latestSourceMsgId: MessageId = afterSourceMsgId
        private set

    private val blockCatalog = dbState.blockCatalog
    private val trieCatalog = dbState.trieCatalog
    private val liveIndex = dbState.liveIndex

    private val allocator = allocator.newChildAllocator("follower-log-processor", 0, Long.MAX_VALUE)

    private fun addTries(tries: List<TrieDetails>, logTimestamp: LogTimestamp) {
        tries.groupBy { it.tableName }.forEach { (tableName, tries) ->
            trieCatalog.addTries(TableRef.parse(dbState.name, tableName), tries, logTimestamp)
        }
    }

    private suspend fun handleRecord(record: Log.Record<ReplicaMessage>) {
        val msg = record.message
        LOG.trace { "follower: message ${record.msgId} (${msg::class.simpleName})" }

        pendingBlock?.let { pendingBlock ->
            val pendingBlockIdx = pendingBlock.blockIdx
            if (msg is ReplicaMessage.BlockUploaded
                && msg.blockIndex == pendingBlockIdx
                && msg.storageEpoch == bufferPool.epoch
            ) {
                LOG.debug("block uploaded b${msg.blockIndex.asLexHex}: source=${msg.latestProcessedMsgId}, replica=${record.msgId} (${pendingBlock.bufferedRecords.size} buffered)")
                val blockFile = bufferPool.allBlockFiles.lastOrNull()
                val block = blockFile?.key?.let { parseFrom(bufferPool.getByteArray(it)) }

                blockCatalog.refresh(block)
                liveIndex.nextBlock()
                compactor.signalBlock()

                val bufferedRecords = pendingBlock.bufferedRecords
                this.pendingBlock = null

                // replay buffered records — their typed notifications advance the watermarks
                for (buffered in bufferedRecords) {
                    handleRecord(buffered)
                }

                // advance replica watermark past the BlockUploaded record
                // (source watermark already advanced by replayed records above)
                watchers.notifyMsg(null, record.msgId)

                return
            } else {
                LOG.trace { "follower: buffering message ${record.msgId} (${msg::class.simpleName}) during pending block b${pendingBlockIdx} (${pendingBlock.bufferedRecords.size + 1} buffered)" }
                pendingBlock += record
                return
            }
        }

        when (msg) {
            is ReplicaMessage.ResolvedTx -> {
                val latestTxId = liveIndex.latestCompletedTx?.txId
                if (latestTxId != null && msg.txId <= latestTxId) {
                    watchers.notifyMsg(null, record.msgId)
                    return
                }

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

                latestSourceMsgId = msg.txId
                watchers.notifyTx(result, msg.txId, record.msgId)
            }

            is ReplicaMessage.TriesAdded -> {
                if (msg.storageVersion == Storage.VERSION && msg.storageEpoch == bufferPool.epoch)
                    addTries(msg.tries, record.logTimestamp)

                latestSourceMsgId = msg.sourceMsgId
                watchers.notifyMsg(msg.sourceMsgId, record.msgId)
            }

            is ReplicaMessage.BlockBoundary -> {
                pendingBlock = PendingBlock(record.msgId, msg, maxBufferedRecords)
                LOG.debug("block boundary b${msg.blockIndex.asLexHex}: source=${msg.latestProcessedMsgId}, replica=${record.msgId} — waiting for BlockUploaded...")
                latestSourceMsgId = msg.latestProcessedMsgId
                watchers.notifyMsg(msg.latestProcessedMsgId, record.msgId)
            }

            is ReplicaMessage.BlockUploaded -> {
                if (msg.storageVersion == Storage.VERSION && msg.storageEpoch == bufferPool.epoch)
                    addTries(msg.tries, record.logTimestamp)
                watchers.notifyMsg(msg.latestProcessedMsgId, record.msgId)
            }

            is ReplicaMessage.NoOp -> {
                watchers.notifyMsg(null, record.msgId)
            }
        }
    }

    override suspend fun processRecords(records: List<Log.Record<ReplicaMessage>>) {
        for (record in records) {
            try {
                handleRecord(record)
            } catch (e: InterruptedException) {
                throw e
            } catch (e: Interrupted) {
                throw e
            } catch (e: Throwable) {
                LOG.error(
                    e,
                    "follower: failed to process log record with msgId ${record.msgId} (${record.message::class.simpleName})"
                )
                watchers.notifyError(e)
                throw e
            }
        }
    }

    override fun close() {
        allocator.close()
    }
}
