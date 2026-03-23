package xtdb.indexer

import org.apache.arrow.memory.BufferAllocator
import xtdb.api.TransactionAborted
import xtdb.api.TransactionCommitted
import xtdb.api.log.DbOp
import xtdb.api.log.Log
import xtdb.api.log.MessageId
import xtdb.api.log.ReplicaMessage
import xtdb.api.log.Watchers
import xtdb.api.storage.Storage
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

    private val trieCatalog = dbState.trieCatalog

    private val allocator = allocator.newChildAllocator("transition-log-processor", 0, Long.MAX_VALUE)

    override suspend fun processRecords(records: List<Log.Record<ReplicaMessage>>) {
        LOG.debug("transition: processing ${records.size} records")

        for (record in records) {
            val msgId = record.msgId
            LOG.trace { "transition: message $msgId (${record.message::class.simpleName})" }

            try {
                when (val msg = record.message) {
                    is ReplicaMessage.ResolvedTx -> {
                        val latestTxId = liveIndex.latestCompletedTx?.txId
                        if (latestTxId == null || msg.txId > latestTxId) {
                            liveIndex.importTx(msg)
                        }
                        val result = if (msg.committed) {
                            when (val dbOp = msg.dbOp) {
                                is DbOp.Attach -> dbCatalog!!.attach(dbOp.dbName, dbOp.config)
                                is DbOp.Detach -> dbCatalog!!.detach(dbOp.dbName)
                                null -> {}
                            }

                            TransactionCommitted(msg.txId, msg.systemTime)
                        } else TransactionAborted(msg.txId, msg.systemTime, msg.error)

                        latestSourceMsgId = msg.txId
                        watchers.notifyTx(result, msg.txId, msgId)
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
                        watchers.notifyMsg(msg.sourceMsgId, msgId)
                    }

                    is ReplicaMessage.BlockBoundary -> {
                        LOG.debug("block boundary b${msg.blockIndex.asLexHex}: source=${msg.latestProcessedMsgId}, replica=$msgId")
                        blockUploader.uploadBlock(replicaProducer, msgId, msg)
                        latestSourceMsgId = msg.latestProcessedMsgId
                        watchers.notifyMsg(msg.latestProcessedMsgId, msgId)
                    }

                    // previously I errored here, but we need to just ignore them -
                    // the transition proc submits a BlockUploaded as part of finishing the BlockBoundary messages.
                    is ReplicaMessage.BlockUploaded -> {
                        latestSourceMsgId = msg.latestProcessedMsgId
                        watchers.notifyMsg(msg.latestProcessedMsgId, msgId)
                    }

                    is ReplicaMessage.NoOp -> {
                        watchers.notifyMsg(null, msgId)
                    }
                }

                latestReplicaMsgId = msgId
            } catch (e: InterruptedException) {
                throw e
            } catch (e: Interrupted) {
                throw e
            } catch (e: Throwable) {
                LOG.error(
                    e,
                    "transition: failed to process log record with msgId $msgId (${record.message::class.simpleName})"
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
