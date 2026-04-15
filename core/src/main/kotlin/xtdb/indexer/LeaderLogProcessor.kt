package xtdb.indexer

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import org.apache.arrow.memory.BufferAllocator
import xtdb.database.ExternalSourceToken
import xtdb.api.TransactionKey
import xtdb.api.TransactionResult
import xtdb.api.log.*
import xtdb.api.log.Log.AtomicProducer.Companion.withTx
import xtdb.api.log.ReplicaMessage.BlockBoundary
import xtdb.api.storage.Storage
import xtdb.arrow.Relation
import xtdb.arrow.asChannel
import xtdb.database.Database
import xtdb.database.DatabaseState
import xtdb.database.DatabaseStorage
import xtdb.error.Anomaly
import xtdb.error.Interrupted
import xtdb.table.TableRef
import xtdb.tx.deserializeUserMetadata
import xtdb.util.*
import xtdb.util.StringUtil.asLexDec
import xtdb.util.StringUtil.asLexHex
import java.nio.ByteBuffer
import java.time.Duration
import java.time.ZoneId
import java.time.ZonedDateTime

private val LOG = LeaderLogProcessor::class.logger

class LeaderLogProcessor(
    allocator: BufferAllocator,
    dbStorage: DatabaseStorage,
    private val replicaProducer: Log.AtomicProducer<ReplicaMessage>,
    private val dbState: DatabaseState,
    private val indexer: Indexer.ForDatabase,
    private val watchers: Watchers,
    private val skipTxs: Set<MessageId>,
    private val dbCatalog: Database.Catalog?,
    private val blockUploader: BlockUploader,
    afterSourceMsgId: MessageId,
    afterReplicaMsgId: MessageId,
    flushTimeout: Duration = Duration.ofMinutes(5),
    meterRegistry: MeterRegistry? = null,
) : LogProcessor.LeaderProcessor {

    init {
        require((dbCatalog != null) == (dbState.name == "xtdb")) {
            "dbCatalog must be provided iff database is 'xtdb'"
        }
    }

    private val dbName = dbState.name
    private val sourceLog = dbStorage.sourceLog
    private val bufferPool = dbStorage.bufferPool
    private val liveIndex = dbState.liveIndex

    private val blockCatalog = dbState.blockCatalog
    private val trieCatalog = dbState.trieCatalog

    private val allocator = allocator.newChildAllocator("leader-log-processor", 0, Long.MAX_VALUE)

    override var pendingBlock: PendingBlock? = null
        private set

    override var latestSourceMsgId: MessageId = afterSourceMsgId
        private set

    override var latestReplicaMsgId: MessageId = afterReplicaMsgId
        private set

    private val blockFlusher = BlockFlusher(flushTimeout, blockCatalog)

    private suspend fun maybeFlushBlock() {
        if (blockFlusher.checkBlockTimeout(blockCatalog, liveIndex)) {
            val flushMessage = SourceMessage.FlushBlock(blockCatalog.currentBlockIndex ?: -1)
            blockFlusher.flushedTxId = sourceLog.appendMessage(flushMessage).msgId
        }
    }

    private suspend fun appendToReplica(message: ReplicaMessage): Log.MessageMetadata =
        replicaProducer.withTx { tx -> tx.appendMessage(message) }.await()
            .also { latestReplicaMsgId = it.msgId }

    private fun resolveTx(
        msgId: MessageId, record: Log.Record<SourceMessage>, msg: SourceMessage
    ): ReplicaMessage.ResolvedTx {
        if (skipTxs.isNotEmpty() && skipTxs.contains(msgId)) {
            LOG.warn("[$dbName] Skipping transaction id $msgId - within XTDB_SKIP_TXS")

            val payload = when (msg) {
                is SourceMessage.Tx -> msg.encode()
                is SourceMessage.LegacyTx -> msg.payload
                else -> error("unexpected message type: ${msg::class}")
            }
            bufferPool.putObject("skipped-txs/${msgId.asLexDec}".asPath, ByteBuffer.wrap(payload))

            return indexer.indexTx(msgId, record.logTimestamp, null, null, null, null, null)
        }

        return when (msg) {
            is SourceMessage.Tx -> {
                msg.txOps.asChannel.use { ch ->
                    Relation.StreamLoader(allocator, ch).use { loader ->
                        Relation(allocator, loader.schema).use { rel ->
                            loader.loadNextPage(rel)

                            val userMetadata = msg.userMetadata?.let { deserializeUserMetadata(allocator, it) }

                            indexer.indexTx(
                                msgId, record.logTimestamp,
                                rel["tx-ops"],
                                msg.systemTime, msg.defaultTz, msg.user, userMetadata
                            )
                        }
                    }
                }
            }

            is SourceMessage.LegacyTx -> {
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

            else -> error("unexpected message type: ${msg::class}")
        }
    }

    private suspend fun notifyTx(resolvedTx: ReplicaMessage.ResolvedTx) {
        val txKey = TransactionKey(resolvedTx.txId, resolvedTx.systemTime)

        val result = when (resolvedTx.committed) {
            true -> TransactionResult.Committed(txKey)
            false -> TransactionResult.Aborted(txKey, resolvedTx.error)
            null -> null
        }

        if (result != null) watchers.notifyTx(result, resolvedTx.txId, resolvedTx.externalSourceToken)
    }

    private suspend fun finishBlock(latestProcessedMsgId: MessageId, externalSourceToken: ExternalSourceToken?) {
        val boundaryMsg = BlockBoundary((blockCatalog.currentBlockIndex ?: -1) + 1, latestProcessedMsgId, externalSourceToken)
        val boundaryMsgId = appendToReplica(boundaryMsg).msgId
        LOG.debug("[$dbName] block boundary b${boundaryMsg.blockIndex.asLexHex}: source=$latestProcessedMsgId, replica=$boundaryMsgId")
        pendingBlock = PendingBlock(boundaryMsgId, boundaryMsg)

        latestReplicaMsgId = blockUploader.uploadBlock(replicaProducer, boundaryMsgId, boundaryMsg)
        pendingBlock = null
    }

    private suspend fun handleResolvedTx(resolvedTx: ReplicaMessage.ResolvedTx) {
        val txId = resolvedTx.txId

        appendToReplica(resolvedTx)
        latestSourceMsgId = txId
        notifyTx(resolvedTx)

        if (liveIndex.isFull())
            finishBlock(txId, resolvedTx.externalSourceToken)
    }

    override suspend fun processRecords(records: List<Log.Record<SourceMessage>>) {
        maybeFlushBlock()

        for (record in records) {
            val msgId = record.msgId
            LOG.trace { "[$dbName] leader: message $msgId (${record.message::class.simpleName})" }

            try {
                when (val msg = record.message) {
                    is SourceMessage.Tx -> {
                        val resolved = resolveTx(msgId, record, msg)
                        handleResolvedTx(msg.externalSourceToken?.let { resolved.copy(externalSourceToken = it) } ?: resolved)
                    }

                    is SourceMessage.LegacyTx -> handleResolvedTx(resolveTx(msgId, record, msg))

                    is SourceMessage.FlushBlock -> {
                        val expectedBlockIdx = msg.expectedBlockIdx
                        if (expectedBlockIdx != null && expectedBlockIdx == (blockCatalog.currentBlockIndex ?: -1L)) {
                            finishBlock(msgId, watchers.externalSourceToken)
                        }
                        latestSourceMsgId = msgId
                        watchers.notifyMsg(msgId)
                    }

                    is SourceMessage.AttachDatabase -> {
                        val txKey = TransactionKey(msgId, record.logTimestamp)
                        val error = try {
                            dbCatalog!!.attach(msg.dbName, msg.config)
                            null
                        } catch (e: Anomaly.Caller) {
                            LOG.debug(e) { "[$dbName] leader: attach database '${msg.dbName}' failed at $msgId" }
                            e
                        }

                        val resolvedTx = indexer.addTxRow(txKey, error)
                            .let { if (error == null) it.copy(dbOp = DbOp.Attach(msg.dbName, msg.config)) else it }

                        appendToReplica(resolvedTx)

                        val result =
                            if (error == null) TransactionResult.Committed(txKey)
                            else TransactionResult.Aborted(txKey, error)
                        latestSourceMsgId = msgId
                        watchers.notifyTx(result, msgId, null)
                    }

                    is SourceMessage.DetachDatabase -> {
                        val txKey = TransactionKey(msgId, record.logTimestamp)
                        val error = try {
                            dbCatalog!!.detach(msg.dbName)
                            null
                        } catch (e: Anomaly.Caller) {
                            LOG.debug(e) { "[$dbName] leader: detach database '${msg.dbName}' failed at $msgId" }
                            e
                        }

                        val resolvedTx = indexer.addTxRow(txKey, error)
                            .let { if (error == null) it.copy(dbOp = DbOp.Detach(msg.dbName)) else it }

                        appendToReplica(resolvedTx)

                        val result = if (error == null) TransactionResult.Committed(txKey)
                        else TransactionResult.Aborted(txKey, error)
                        latestSourceMsgId = msgId
                        watchers.notifyTx(result, msgId, null)
                    }

                    is SourceMessage.TriesAdded -> {
                        if (msg.storageVersion == Storage.VERSION && msg.storageEpoch == bufferPool.epoch) {
                            msg.tries.groupBy { it.tableName }.forEach { (tableName, tries) ->
                                trieCatalog.addTries(
                                    TableRef.parse(dbState.name, tableName),
                                    tries,
                                    record.logTimestamp
                                )
                            }
                        }

                        appendToReplica(
                            ReplicaMessage.TriesAdded(
                                msg.storageVersion, msg.storageEpoch, msg.tries, sourceMsgId = msgId
                            )
                        )

                        latestSourceMsgId = msgId
                        watchers.notifyMsg(msgId)
                    }

                    // TODO this one's going before release
                    is SourceMessage.BlockUploaded -> {
                        latestSourceMsgId = msgId
                        watchers.notifyMsg(msgId)
                    }
                }
            } catch (e: InterruptedException) {
                throw e
            } catch (e: Interrupted) {
                throw e
            } catch (e: Throwable) {
                LOG.error(
                    e,
                    "[$dbName] leader: failed to process log record with msgId $msgId (${record.message::class.simpleName})"
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
