package xtdb.indexer

import org.apache.arrow.memory.BufferAllocator
import xtdb.api.TransactionKey
import xtdb.api.log.Log
import xtdb.api.log.Log.Message
import xtdb.api.log.MessageId
import xtdb.api.storage.Storage
import xtdb.arrow.Relation
import xtdb.arrow.asChannel
import xtdb.catalog.BlockCatalog
import xtdb.database.DatabaseState
import xtdb.database.DatabaseStorage
import xtdb.database.proto.DatabaseConfig
import xtdb.error.Anomaly
import xtdb.error.Conflict
import xtdb.error.Incorrect
import xtdb.error.NotFound
import xtdb.log.proto.TrieDetails
import xtdb.table.TableRef
import xtdb.util.MsgIdUtil.offsetToMsgId
import xtdb.util.StringUtil.asLexDec
import xtdb.util.StringUtil.asLexHex
import xtdb.util.asPath
import xtdb.util.debug
import xtdb.util.logger
import xtdb.util.warn
import java.nio.ByteBuffer
import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime

private val LOG = SourceLogProcessor::class.logger

/**
 * Subscribes to the source log and resolves Message.Tx records into Message.ResolvedTx,
 * then forwards the amended records to the replica processor via processRecords.
 * All other message types pass through unchanged.
 *
 * Also owns block finishing: when the source LiveIndex is full, writes tries, table blocks,
 * and the block file to storage, then appends TriesAdded + BlockUploaded to the source log.
 */
class SourceLogProcessor(
    private val allocator: BufferAllocator,
    private val dbStorage: DatabaseStorage,
    private val dbState: DatabaseState,
    private val indexer: Indexer.ForDatabase,
    private val liveIndex: LiveIndex,
    private val replicaProcessor: ReplicaLogProcessor,
    private val skipTxs: Set<MessageId>,
    private val readOnly: Boolean = false,
) : Log.Subscriber {

    private val log = dbStorage.sourceLog
    private val epoch = log.epoch
    private val bufferPool = dbStorage.bufferPool

    private val blockCatalog = dbState.blockCatalog
    private val trieCatalog = dbState.trieCatalog
    private val tableCatalog = dbState.tableCatalog

    private val secondaryDatabases: MutableMap<String, DatabaseConfig> =
        blockCatalog.secondaryDatabases.toMutableMap()

    private var latestProcessedMsgId: MessageId = blockCatalog.latestProcessedMsgId ?: -1

    private fun resolveTx(msgId: MessageId, record: Log.Record, msg: Message.Tx): Message.ResolvedTx {
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
            Message.TriesAdded(Storage.VERSION, bufferPool.epoch, addedTries)
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

        val secondaryDatabasesForBlock = secondaryDatabases.takeIf { dbState.name == "xtdb" }

        val block = blockCatalog.buildBlock(
            blockIdx, liveIndex.latestCompletedTx, latestProcessedMsgId,
            tableBlocks.keys, secondaryDatabasesForBlock
        )

        bufferPool.putObject(BlockCatalog.blockFilePath(blockIdx), ByteBuffer.wrap(block.toByteArray()))
        blockCatalog.refresh(block)

        log.appendMessage(Message.BlockUploaded(blockIdx, latestProcessedMsgId, bufferPool.epoch))

        liveIndex.nextBlock()
        LOG.debug("finished block: 'b${blockIdx.asLexHex}'.")
    }

    override fun processRecords(records: List<Log.Record>) {
        var lastMsgId: MessageId = latestProcessedMsgId

        val amended = try {
            records.flatMap { record ->
                val msgId = offsetToMsgId(epoch, record.logOffset)
                lastMsgId = msgId
                latestProcessedMsgId = msgId

                when (val msg = record.message) {
                    is Message.Tx -> {
                        val resolvedTx = resolveTx(msgId, record, msg)

                        if (liveIndex.isFull()) {
                            if (readOnly)
                                liveIndex.nextBlock()
                            else
                                finishBlock(record.logTimestamp)
                        }

                        listOf(Log.Record(record.logOffset, record.logTimestamp, resolvedTx))
                    }

                    is Message.FlushBlock -> {
                        if (!readOnly) {
                            val expectedBlockIdx = msg.expectedBlockIdx
                            if (expectedBlockIdx != null && expectedBlockIdx == (blockCatalog.currentBlockIndex ?: -1L))
                                finishBlock(record.logTimestamp)
                        }
                        listOf(record)
                    }

                    is Message.AttachDatabase -> {
                        val error = try {
                            if (msg.dbName == "xtdb" || msg.dbName in secondaryDatabases)
                                throw Conflict("Database already exists", "xtdb/db-exists", mapOf("db-name" to msg.dbName))
                            secondaryDatabases[msg.dbName] = msg.config.serializedConfig
                            null
                        } catch (e: Anomaly.Caller) { e }

                        val resolvedTx = indexer.addTxRow(TransactionKey(msgId, record.logTimestamp), error)
                        listOf(record, Log.Record(record.logOffset, record.logTimestamp, resolvedTx))
                    }

                    is Message.DetachDatabase -> {
                        val error = try {
                            when {
                                msg.dbName == "xtdb" ->
                                    throw Incorrect("Cannot detach the primary 'xtdb' database", "xtdb/cannot-detach-primary", mapOf("db-name" to msg.dbName))
                                msg.dbName !in secondaryDatabases ->
                                    throw NotFound("Database does not exist", "xtdb/no-such-db", mapOf("db-name" to msg.dbName))
                                else -> {
                                    secondaryDatabases.remove(msg.dbName)
                                    null
                                }
                            }
                        } catch (e: Anomaly.Caller) { e }

                        val resolvedTx = indexer.addTxRow(TransactionKey(msgId, record.logTimestamp), error)
                        listOf(record, Log.Record(record.logOffset, record.logTimestamp, resolvedTx))
                    }

                    is Message.TriesAdded -> {
                        // Compactor appends TriesAdded to the source log.
                        // We need to update the source's trie catalog so that
                        // finishBlock writes correct partition info to block files.
                        if (msg.storageVersion == Storage.VERSION && msg.storageEpoch == bufferPool.epoch)
                            msg.tries.groupBy { it.tableName }.forEach { (tableName, tries) ->
                                trieCatalog.addTries(TableRef.parse(dbState.name, tableName), tries, record.logTimestamp)
                            }
                        listOf(record)
                    }

                    is Message.BlockUploaded, is Message.ResolvedTx -> listOf(record)
                }
            }
        } catch (e: Throwable) {
            // Notify replica watchers so that awaiting clients don't hang.
            replicaProcessor.ingestionStopped(lastMsgId, e)
            throw e
        }

        replicaProcessor.processRecords(amended)
    }
}
