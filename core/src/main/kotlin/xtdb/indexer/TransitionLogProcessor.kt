package xtdb.indexer

import org.apache.arrow.memory.BufferAllocator
import xtdb.api.TransactionAborted
import xtdb.api.TransactionCommitted
import xtdb.api.log.DbOp
import xtdb.api.log.Log
import xtdb.api.log.ReplicaMessage
import xtdb.api.log.Watchers
import xtdb.api.storage.Storage
import xtdb.error.Interrupted
import xtdb.database.Database
import xtdb.database.DatabaseState
import xtdb.storage.BufferPool
import xtdb.table.TableRef
import xtdb.util.error
import xtdb.util.logger
import xtdb.util.warn

private val LOG = TransitionLogProcessor::class.logger

class TransitionLogProcessor(
    allocator: BufferAllocator,
    private val bufferPool: BufferPool,
    private val dbState: DatabaseState,
    private val liveIndex: LiveIndex,
    private val blockFinisher: BlockFinisher,
    private val replicaProducer: Log.AtomicProducer<ReplicaMessage>,
    private val sourceWatchers: Watchers,
    private val replicaWatchers: Watchers,
    private val dbCatalog: Database.Catalog?,
) : LogProcessor.TransitionProcessor {

    private val trieCatalog = dbState.trieCatalog

    private val allocator = allocator.newChildAllocator("transition-log-processor", 0, Long.MAX_VALUE)

    override fun processRecords(records: List<Log.Record<ReplicaMessage>>) {
        for (record in records) {
            val msgId = record.msgId

            try {
                val res = when (val msg = record.message) {
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

                        sourceWatchers.notify(msg.txId, result)
                        result
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
                        null
                    }

                    is ReplicaMessage.BlockBoundary -> {
                        blockFinisher.finishBlock(replicaProducer, msgId, msg)
                        null
                    }

                    // previously I errored here, but we need to just ignore them -
                    // the transition proc submits a BlockUploaded as part of finishing the BlockBoundary messages.
                    is ReplicaMessage.BlockUploaded -> null

                    is ReplicaMessage.NoOp -> null
                }

                replicaWatchers.notify(msgId, res)
            } catch (e: InterruptedException) {
                throw e
            } catch (e: Interrupted) {
                throw e
            } catch (e: Throwable) {
                LOG.error(e, "transition: failed to process log record with msgId $msgId (${record.message::class.simpleName})")
                sourceWatchers.notify(msgId, e) // strictly speaking, a replica msgId.
                replicaWatchers.notify(msgId, e)
                throw e
            }
        }
    }

    override fun close() {
        allocator.close()
    }
}
