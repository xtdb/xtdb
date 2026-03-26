package xtdb.indexer

import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.first
import org.apache.arrow.memory.BufferAllocator
import xtdb.api.TransactionAborted
import xtdb.api.TransactionCommitted
import xtdb.api.log.*
import xtdb.api.storage.Storage
import xtdb.block.proto.Block.parseFrom
import xtdb.catalog.BlockCatalog.Companion.blockFilePath
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
    afterReplicaMsgId: MessageId,
    private val maxBufferedRecords: Int = 1024,
) : LogProcessor.FollowerProcessor {

    override var pendingBlock: PendingBlock? = pendingBlock
        private set

    override var latestSourceMsgId: MessageId = afterSourceMsgId
        private set

    private val latestSeenReplicaMsgId = MutableStateFlow(afterReplicaMsgId)

    override val latestReplicaMsgId: MessageId get() = latestSeenReplicaMsgId.value

    private val dbName = dbState.name
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
        LOG.trace { "[$dbName] follower: message ${record.msgId} (${msg::class.simpleName})" }

        pendingBlock?.let { pendingBlock ->
            val pendingBlockIdx = pendingBlock.blockIdx
            if (msg is ReplicaMessage.BlockUploaded
                && msg.blockIndex == pendingBlockIdx
                && msg.storageEpoch == bufferPool.epoch
            ) {
                LOG.debug("[$dbName] block uploaded b${msg.blockIndex.asLexHex}: source=${msg.latestProcessedMsgId}, replica=${record.msgId} (${pendingBlock.bufferedRecords.size} buffered)")
                val block = parseFrom(bufferPool.getByteArray(blockFilePath(pendingBlockIdx)))

                blockCatalog.refresh(block)
                liveIndex.nextBlock()
                compactor.signalBlock()

                val bufferedRecords = pendingBlock.bufferedRecords
                this.pendingBlock = null

                // replay buffered records — their typed notifications advance the watermarks
                bufferedRecords.forEach { handleRecord(it) }
            } else {
                LOG.trace { "[$dbName] follower: buffering message ${record.msgId} (${msg::class.simpleName}) during pending block b${pendingBlockIdx} (${pendingBlock.bufferedRecords.size + 1} buffered)" }
                pendingBlock += record
            }

            return
        }

        when (msg) {
            is ReplicaMessage.ResolvedTx -> {
                val latestTxId = liveIndex.latestCompletedTx?.txId
                if (latestTxId != null && msg.txId <= latestTxId) {
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
                watchers.notifyTx(result, msg.txId, msg.externalSourceToken)
            }

            is ReplicaMessage.TriesAdded -> {
                if (msg.storageVersion == Storage.VERSION && msg.storageEpoch == bufferPool.epoch)
                    addTries(msg.tries, record.logTimestamp)

                latestSourceMsgId = msg.sourceMsgId
                watchers.notifyMsg(msg.sourceMsgId)
            }

            is ReplicaMessage.BlockBoundary -> {
                pendingBlock = PendingBlock(record.msgId, msg, maxBufferedRecords)
                LOG.debug("[$dbName] block boundary b${msg.blockIndex.asLexHex}: source=${msg.latestProcessedMsgId}, replica=${record.msgId} — waiting for BlockUploaded...")
                latestSourceMsgId = msg.latestProcessedMsgId
                watchers.notifyMsg(msg.latestProcessedMsgId)
            }

            is ReplicaMessage.BlockUploaded -> {
                if (msg.storageVersion == Storage.VERSION && msg.storageEpoch == bufferPool.epoch)
                    addTries(msg.tries, record.logTimestamp)
                watchers.notifyMsg(msg.latestProcessedMsgId)
            }

            is ReplicaMessage.NoOp -> Unit
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
                    "[$dbName] follower: failed to process log record with msgId ${record.msgId} (${record.message::class.simpleName})"
                )
                watchers.notifyError(e)
                throw e
            } finally {
                latestSeenReplicaMsgId.value = record.msgId
            }
        }
    }

    override suspend fun awaitReplicaMsgId(target: MessageId) {
        LOG.debug("[$dbName] transition: awaiting replica watcher catch-up to $target")
        latestSeenReplicaMsgId.first { it >= target }
        LOG.debug("[$dbName] transition: replica watchers caught up to $target")
    }

    override fun close() {
        allocator.close()
    }
}
