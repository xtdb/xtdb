package xtdb.indexer

import org.apache.arrow.memory.BufferAllocator
import xtdb.api.TransactionKey
import xtdb.api.TransactionResult
import xtdb.api.log.DbOp
import xtdb.api.log.Log
import xtdb.api.log.MessageId
import xtdb.api.log.ReplicaMessage
import xtdb.api.log.Watchers
import xtdb.api.storage.Storage
import xtdb.error.Anomaly
import xtdb.error.Interrupted
import xtdb.database.Database
import xtdb.database.DatabaseState
import xtdb.storage.BufferPool
import xtdb.table.TableRef
import xtdb.util.*
import xtdb.util.StringUtil.asLexHex

private val LOG = TransitionLogProcessor::class.logger

class TransitionLogProcessor(
    allocator: BufferAllocator,
    private val bufferPool: BufferPool,
    private val dbState: DatabaseState,
    private val liveIndex: LiveIndex,
    private val blockUploader: BlockUploader,
    private val replicaProducer: Log.AtomicProducer<ReplicaMessage>,
    private val watchers: Watchers,
    private val dbCatalog: Database.Catalog?,
    afterSourceMsgId: MessageId,
    afterReplicaMsgId: MessageId,
) : LogProcessor.TransitionProcessor {

    override var latestSourceMsgId: MessageId = afterSourceMsgId
        private set

    override var latestReplicaMsgId: MessageId = afterReplicaMsgId
        private set

    private val dbName = dbState.name
    private val blockCatalog = dbState.blockCatalog
    private val trieCatalog = dbState.trieCatalog

    private val allocator = allocator.newChildAllocator("transition-log-processor", 0, Long.MAX_VALUE)

    private val ReplicaMessage.stale get() =
        when (this) {
            is ReplicaMessage.ResolvedTx -> txId <= latestSourceMsgId
            is ReplicaMessage.TriesAdded -> sourceMsgId <= latestSourceMsgId
            is ReplicaMessage.BlockBoundary -> blockIndex <= (blockCatalog.currentBlockIndex ?: -1)
            is ReplicaMessage.BlockUploaded -> blockIndex <= (blockCatalog.currentBlockIndex ?: -1)
            is ReplicaMessage.NoOp -> false
            is ReplicaMessage.TriesDeleted -> false
        }

    private suspend fun processRecord(record: Log.Record<ReplicaMessage>) {
        val msgId = record.msgId
        when (val msg = record.message) {
            is ReplicaMessage.ResolvedTx -> {
                val latestTxId = liveIndex.latestCompletedTx?.txId
                if (latestTxId == null || msg.txId > latestTxId) {
                    liveIndex.importTx(msg)
                }
                val txKey = TransactionKey(msg.txId, msg.systemTime)
                if (msg.committed) {
                    when (val dbOp = msg.dbOp) {
                        is DbOp.Attach -> if (dbCatalog != null) {
                            try {
                                dbCatalog.attach(dbOp.dbName, dbOp.config)
                            } catch (e: Anomaly.Caller) {
                                LOG.debug(e) { "[$dbName] transition: attach database '${dbOp.dbName}' failed" }
                            }
                        }
                        is DbOp.Detach -> if (dbCatalog != null) {
                            try {
                                dbCatalog.detach(dbOp.dbName)
                            } catch (e: Anomaly.Caller) {
                                LOG.debug(e) { "[$dbName] transition: detach database '${dbOp.dbName}' failed" }
                            }
                        }
                        null -> {}
                    }
                }

                val result =
                    if (msg.committed) TransactionResult.Committed(txKey)
                    else TransactionResult.Aborted(txKey, msg.error)

                latestSourceMsgId = msg.txId
                watchers.notifyTx(result, msg.txId, msg.externalSourceToken)
            }

            is ReplicaMessage.TriesAdded -> {
                if (msg.storageVersion == Storage.VERSION && msg.storageEpoch == bufferPool.epoch) {
                    msg.tries.groupBy { it.tableName }.forEach { (tableName, tries) ->
                        trieCatalog.addTries(
                            TableRef.parse(dbState.name, tableName),
                            tries,
                            record.logTimestamp
                        )
                    }
                }
                latestSourceMsgId = msg.sourceMsgId
                watchers.notifyMsg(msg.sourceMsgId)
            }

            is ReplicaMessage.BlockBoundary -> {
                LOG.debug("[$dbName] block boundary b${msg.blockIndex.asLexHex}: source=${msg.latestProcessedMsgId}, replica=$msgId")
                blockUploader.uploadBlock(replicaProducer, msgId, msg)
                latestSourceMsgId = msg.latestProcessedMsgId
                watchers.notifyMsg(msg.latestProcessedMsgId)
            }

            // previously I errored here, but we need to just ignore them -
            // the transition proc submits a BlockUploaded as part of finishing the BlockBoundary messages.
            is ReplicaMessage.BlockUploaded -> {
                latestSourceMsgId = msg.latestProcessedMsgId
                watchers.notifyMsg(msg.latestProcessedMsgId)
            }

            is ReplicaMessage.NoOp -> Unit

            is ReplicaMessage.TriesDeleted -> {
                trieCatalog.deleteTries(TableRef.parse(dbState.name, msg.tableName), msg.trieKeys)
            }
        }
    }

    override suspend fun processRecords(records: List<Log.Record<ReplicaMessage>>) {
        LOG.debug("[$dbName] transition: processing ${records.size} records")

        for (record in records) {
            val msgId = record.msgId
            LOG.trace { "[$dbName] transition: message $msgId (${record.message::class.simpleName})" }

            try {
                if (!record.message.stale) processRecord(record)

                latestReplicaMsgId = msgId
            } catch (e: InterruptedException) {
                throw e
            } catch (e: Interrupted) {
                throw e
            } catch (e: Throwable) {
                LOG.error(
                    e,
                    "[$dbName] transition: failed to process log record with msgId $msgId (${record.message::class.simpleName})"
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
